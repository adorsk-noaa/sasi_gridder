"""
Task for gridding SASI Efforts.
"""

# TODO: use custom dao for adding nemareas.
from sasi_gridder.dao import SASIGridderDAO
from sasi_gridder import models as models
import task_manager
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import func 
import sasi_data.ingestors as ingestors
import sasi_data.util.gis as gis_util

import tempfile
import os
import shutil
import zipfile
import logging
import csv


def robust_cast(fn):
    def robust_fn(val):
        if val is None: return None
        else: return fn(val)
    return robust_fn

class LoggerLogHandler(logging.Handler):
    """ Custom log handler that logs messages to another
    logger. This can be used to chain together loggers. """
    def __init__(self, logger=None, **kwargs):
        logging.Handler.__init__(self, **kwargs)
        self.logger = logger

    def emit(self, record):
        self.logger.log(record.levelno, self.format(record))

class SASIGridderTask(task_manager.Task):

    def __init__(self, config={}, data={}, 
                 get_connection=None, max_mem=1e9, **kwargs):
        super(SASIGridderTask, self).__init__(**kwargs)
        self.logger.debug("RunSasiTask.__init__")

        self.data = data
        self.config = config
        self.value_attrs = models.Effort.value_attrs
        self.key_attrs = ['gear_id']

        # Define trip type to gear code mappings.
        # @TODO: put this in config.
        self.trip_type_gear_mappings = {
            'hy_drg': 'GC30',
            'otter': 'GC10',
            'sca-gc': 'GC21',
            'sca-la': 'GC20',
            'shrimp': 'GC11',
            'squid': 'GC12',
            'raised': 'GC13',
            'trap': 'GC60',
            'gillne': 'GC50',
            'longli': 'GC40',
        }

        for kwarg in ['raw_efforts_path', 'grid_path', 'stat_areas_path',
                      'output_path']:
            setattr(self, kwarg, kwargs.get(kwarg))

        if not self.output_path:
            os_hndl, self.output_path = tempfile.mkstemp(
                prefix="gridded_efforts.", suffix='.csv')

        # Assign get_session function.
        if not get_connection:
            def get_connection():
                engine = create_engine('sqlite://')
                return engine.connect()
        self.get_connection = get_connection

        self.message_logger = logging.getLogger("Task%s_msglogger" % id(self))
        main_log_handler = LoggerLogHandler(self.logger)
        main_log_handler.setFormatter(
            logging.Formatter('%(message)s'))
        self.message_logger.addHandler(main_log_handler)
        self.message_logger.setLevel(self.logger.level)

    def call(self):
        self.progress = 1
        self.message_logger.info("Starting...")

        # Create build dir.
        build_dir = tempfile.mkdtemp(prefix="gridderWork.")

        con = self.get_connection()
        trans = con.begin()
        session = sessionmaker()(bind=con)

        # @TODO: add validation here?

        # Read in data.
        try:
            base_msg = "Ingesting..."
            ingest_logger = self.get_logger_logger('ingest', base_msg,
                                                   self.logger)
            self.message_logger.info(base_msg)
            self.dao = SASIGridderDAO(session=session)

            # Read in cells.
            self.ingest_cells(parent_logger=ingest_logger)

            # Read in stat_areas.
            self.ingest_stat_areas(parent_logger=ingest_logger)

            # Read in raw_efforts.
            self.ingest_raw_efforts(parent_logger=ingest_logger)

        except Exception as e:
            self.logger.exception("Error ingesting")
            raise e

        # Grid the efforts
        try:
            base_msg = "Starting gridding."
            gridding_logger = self.get_logger_logger('gridding', base_msg,
                                                      self.logger)
            self.message_logger.info(base_msg)

            #
            #  Main part of the gridding task.
            #   

            #
            # 0. Terms used here:
            # 'clean' efforts can be assigned to a cell.
            # 'kinda_dirty' efforts can be assigned to a stat_area.
            # 'super_dirty' efforts can not be assigned to a cell or a stat_area.
            #
            # Running example:
            # We start with two cells, 'C1' and 'C2', and one stat_area , 'StatArea1'.
            # 'StatArea1' contains 50% of 'C1', and 100% of 'C2'.
            #

            #
            # 1. Assign 'clean' efforts to cells, assign kinda-dirty efforts to
            # stat areas, and save super-dirty efforts to the super-dirty efforts list.
            #
            # Running example:
            # We have 100 points of clean effort which can be assigned to 'C1',
            # 100 points of clean effort which can be assigned to 'C2',
            # 100 points of kinda-dirty effort which can be assigned to 'StatArea1',
            # and 100 points of super-dirty effort which can't be assigned to anything.
            # After this first step, both 'C1' and 'C2' will have 100 points of effort assigned
            # from clean efforts.
            #

            unassigned = {}

            effort_counter = 0
            commit_interval = 1e4
            batched_efforts = self.dao.get_batched_results(
                self.dao.session.query(self.dao.schema['sources']['Effort']),
                1e4)
            for effort in batched_efforts:
                effort_counter += 1

                # If effort has lat and lon...
                if effort.lat is not None and effort.lon is not None:
                    # Can effort can be assigned to cell?
                    cell = self.get_cell_for_pos(effort.lat, effort.lon)
                    if cell:
                        self.add_effort_to_cell(cell, effort, commit=False)
                        continue

                    # Otherwise can effort can be assigned to statarea?
                    stat_area = self.get_stat_area_for_pos(
                        effort.lat, effort.lon)
                    if stat_area:
                        self.add_effort_to_stat_area(stat_area, effort,
                                                     commit=False)
                        continue

                    # Otherwise add to unassigned.
                    else:
                        self.add_effort_to_unassigned(unassigned, effort)
                        continue

                # Otherwise if effort has a stat area...
                elif effort.stat_area_id is not None:
                    stat_area = self.dao.session.query(
                        self.dao.schema['sources']['StatArea']).get(
                            effort.stat_area_id)
                    self.add_effort_to_stat_area(stat_area, effort, commit=False)

                # Otherwise add to unassigned list.
                else:
                    self.add_effort_to_unassigned(unassigned, effort)

                if (effort_counter % commit_interval) == 0:
                    self.dao.commit()

            # Commit any remaining changes.
            self.dao.commit()

            # 
            # 2. For each effort assigned to a stat area,
            # distribute values across cracked cells in that stat area.
            # We distribute values in proportion to the amount of value
            # contained in the cracked cell relative to the total amount
            # of 'clean' value the stat area already contains.
            #
            # Running Example:
            # We now distribute the 100 points of kinda effort which can be assigned to 'StatArea1'.
            # We distribute the effort proportionally to the cracked cells,
            # so that 'C1' gets 33 additional effort points, and 'C2' gets 66 additional effort points.
            #

            stat_areas = self.dao.session.query(
                self.dao.schema['sources']['StatArea'])
            for stat_area in stat_areas:
                # Calculate totals for efforts assigned to the stat area.
                stat_area_totals = {}
                for effort_key, effort_values in stat_area.keyed_values.items():
                    stat_area_values = stat_area_totals.setdefault(
                        effort_key,
                        self.new_values_dict()
                    )
                    for attr, effort_value in effort_values.items():
                        stat_area_values[attr] += effort_value

                # Get list of cracked cells.
                cracked_cells = self.get_cracked_cells_for_stat_area(stat_area)

                # Calculate totals for values across cracked cells.
                ccell_totals = {}
                for ccell in cracked_cells:
                    for effort_key, ccell_values in ccell.keyed_values.items():
                        ccell_totals_values = ccell_totals.setdefault(
                            effort_key,
                            self.new_values_dict()
                        )
                        for attr, ccell_value in ccell_values.items():
                            ccell_totals_values[attr] += ccell_value

                # Distribute the stat area's values across the cracked
                # cells, in proportion to the cracked cell's values as a
                # percentage of the stat area's cracked cell totals.
                for ccell in cracked_cells:
                    pcell = ccell.parent_cell
                    for effort_key, sat_values in stat_area_totals.items():
                        ccell_totals_values = ccell_totals.get(effort_key)
                        ccell_values = ccell.keyed_values.get(effort_key)
                        if not ccell_totals_values or not ccell_values:
                            continue
                        for attr, sa_value in sat_values.items():
                            # Don't add anything for empty values.
                            # This also avoids division by zero errors.
                            if not sa_value:
                                continue
                            ccell_value = ccell_values.get(attr, 0.0)
                            ccell_totals_value = ccell_totals_values.get(attr,
                                                                         0.0)
                            pct_value = ccell_value/ccell_totals_value
                            # Add proportional value to cracked cell's parent 
                            # cell.
                            pcell_values = pcell.keyed_values[effort_key]
                            pcell_values[attr] += sa_value * pct_value
                    self.dao.save(pcell, commit=False)

            # Commit changes.
            self.dao.commit()


            #
            # 3. For efforts which could not be assigned to a cell or a stat area
            # ('super-dirty' efforts), distribute the efforts across all cells,
            # such that the amount of effort each cell is receives is proportional to the cell's
            # total contribution to the overall total.
            #
            # Running Example:
            # We start cells 'C1' and 'C2'.
            # 'C1' starts with 133 effort points from clean efforts + kinda-dirty efforts.
            # Likewise 'C1' starts with 166 effort points from clean efforts + kinda-dirty efforts.
            # Our overall total is 133 + 166 = 300.
            # 'C1' is responsible for 133/300 = 45% of the total effort.
            # 'C2' is responsible for 166/300 = 55% of the total effort.
            # We then have 100 additional points of super-dirty effort which could not be assigned to any cell
            # or stat area.
            # We distributed the effort proportionally to the cells so that
            # 'C1' gets 45 additional effort points, and 'C2' gets 55 additional effort points.
            # Our final result is that 'C1' has 133 + 45 = 178 effort points, and
            # 'C2' has 166 + 55 = 221 effort points.

            # Calculate totals across all cells.
            totals = {}
            cells = self.dao.session.query(
                self.dao.schema['sources']['Cell'])
            for cell in cells:
                for effort_key, cell_values in cell.keyed_values.items():
                    totals_values = totals.setdefault(
                        effort_key, 
                        self.new_values_dict()
                    )
                    for attr, cell_value in cell_values.items():
                        totals_values[attr] += cell_value

            # Distribute unassigned efforts across all cells,
            # in proportion to the cell's values as a percentage of the total.
            for effort_key, unassigned_values in unassigned.items():
                for cell in cells:
                    cell_values = cell.keyed_values.get(effort_key)
                    if not cell_values:
                        continue
                    for attr, unassigned_value in unassigned_values.items():
                        if not unassigned_value:
                            continue
                        cell_value = cell_values.get(attr, 0.0)
                        pct_value = cell_value/unassigned_value
                        cell_values[attr] += unassigned_value * pct_value
                    self.dao.save(cell, commit=False)

            # Commit changes.
            self.dao.commit()

            # Done! At this point the effort has been distributed. 

            # Note that there may be some efforts which are not included.
            # For example, if an unassigned effort has an effort_key which is 
            # not used by any effort assigned to a cell or a stat_area, then 
            # no cell will have a non-zero pct_value for that effort_key.

        except Exception as e:
            self.logger.exception("Error gridding : %s" % e)
            raise e


        #
        # Output gridded efforts.
        #
        csv_file = open(self.output_path, "w")
        w = csv.writer(csv_file)
        fields = ['cell_id'] + self.key_attrs + self.value_attrs
        w.writerow(fields)

        cells = self.dao.session.query(
            self.dao.schema['sources']['Cell'])
        for cell in cells:
            for keys, values in cell.keyed_values.items():
                row_dict = {
                    'cell_id': cell.id
                }
                for i in range(len(self.key_attrs)):
                    row_dict[self.key_attrs[i]] = keys[i]
                row_dict.update(values)
                w.writerow([row_dict[f] for f in fields])
        csv_file.close()

        shutil.rmtree(build_dir)

        self.progress = 100
        self.message_logger.info("Gridding completed, output file is:'%s'" % (
            self.output_path))
        self.status = 'resolved'

    def get_logger_logger(self, name=None, base_msg=None, parent_logger=None):
        logger = logging.getLogger("%s_%s" % (id(self), name))
        formatter = logging.Formatter(base_msg + ' %(message)s.')
        log_handler = LoggerLogHandler(parent_logger)
        log_handler.setFormatter(formatter)
        logger.addHandler(log_handler)
        logger.setLevel(self.message_logger.level)
        return logger

    def get_cell_for_pos(self, lat, lon):
        """ Get cell which contains a given lat lon.
        Returns None if no cell could be found."""
        Cell = self.dao.schema['sources']['Cell']
        return self.get_obj_for_pos(Cell, lat, lon)

    def get_stat_area_for_pos(self, lat, lon):
        """ Get statarea which contains a given lat lon.
        Returns None if no StatArea could be found."""
        StatArea = self.dao.schema['sources']['StatArea']
        return self.get_obj_for_pos(StatArea, lat, lon)

    def get_obj_for_pos(self, clazz, lat, lon):
        pos_wkt = 'POINT(%s %s)' % (lon, lat)
        objs = self.dao.session.query(clazz).filter(func.ST_Contains(
            clazz.geom, func.ST_GeomFromText(pos_wkt, 4326))).all()
        if objs:
            return objs[0]
        return None


    def new_values_dict(self):
        return dict(zip(self.value_attrs, [0.0] * len(self.value_attrs)))

    def update_values_dict(self, values_dict, effort):
        for k in values_dict.keys():
            effort_value = getattr(effort, k, 0.0)
            if effort_value is None:
                effort_value = 0.0
            values_dict[k] += effort_value

    def add_effort_to_obj(self, obj, effort, keyed_values_attr='keyed_values',
                          commit=True):
        keyed_values_dict = getattr(obj, keyed_values_attr)
        self.add_effort_to_keyed_values_dict(keyed_values_dict, effort)

    def add_effort_to_keyed_values_dict(self, kvd, effort):
        values = kvd.setdefault(
            self.get_effort_key(effort), 
            self.new_values_dict()
        )
        self.update_values_dict(values, effort)

    def add_effort_to_cell(self, cell, effort, commit=True):
        self.add_effort_to_obj(cell, effort)
        self.dao.save(cell, commit=commit)

    def add_effort_to_stat_area(self, stat_area, effort, commit=True):
        self.add_effort_to_obj(stat_area, effort)
        self.dao.save(stat_area, commit=commit)

    def add_effort_to_unassigned(self, unassigned, effort):
        self.add_effort_to_keyed_values_dict(unassigned, effort)

    def get_effort_key(self, effort):
        """  Key for grouping values by effort types. """
        return tuple([getattr(effort, attr, None) for attr in self.key_attrs])

    def ingest_cells(self, parent_logger=None):
        logger = self.get_logger_logger(
            name='cell_ingest', 
            base_msg='Ingesting cells...',
            parent_logger=parent_logger
        )
        ingestor = ingestors.Shapefile_Ingestor(
            dao=self.dao,
            shp_file=self.grid_path,
            clazz=self.dao.schema['sources']['Cell'],
            reproject_to='EPSG:4326',
            mappings=[
                {'source': 'TYPE', 'target': 'type'},
                {'source': 'TYPE_ID', 'target': 'type_id'},
            ],
            logger=logger,
            commit_interval=1e3,
            limit=1e3,
        ) 
        ingestor.ingest()
        self.dao.commit()

        # Calculate cell areas.
        for cell in self.dao.session.query(
            self.dao.schema['sources']['Cell']):
            cell.area = gis_util.get_shape_area(
                gis_util.wkb_to_shape(str(cell.geom.geom_wkb)),
            )
            self.dao.save(cell, commit=False)
        self.dao.commit()

    def ingest_stat_areas(self, parent_logger=None):
        logger = self.get_logger_logger(
            name='stat_area_ingest', 
            base_msg='Ingesting stat_areas...',
            parent_logger=parent_logger
        )
        ingestor = ingestors.Shapefile_Ingestor(
            dao=self.dao,
            shp_file=self.stat_areas_path,
            clazz=self.dao.schema['sources']['StatArea'],
            reproject_to='EPSG:4326',
            mappings=[
                {'source': 'SAREA', 'target': 'id'},
            ],
            logger=logger,
            commit_interval=1e3,
        ) 
        ingestor.ingest()
        self.dao.commit()

    def ingest_raw_efforts(self, parent_logger=None):
        logger = self.get_logger_logger(
            name='raw_effort_ingest', 
            base_msg='Ingesting raw efforts...',
            parent_logger=parent_logger
        )

        # Define mappings form trip types to gear codes.
        def trip_type_to_gear_id(trip_type):
            return self.trip_type_gear_mappings.get(trip_type)

        ingestor = ingestors.DAO_CSV_Ingestor(
            dao=self.dao, 
            csv_file=self.raw_efforts_path,
            clazz=self.dao.schema['sources']['Effort'],
            mappings=[
                {'source': 'trip_type', 'target': 'gear_id', 
                 'processor': trip_type_to_gear_id},
                {'source': 'year', 'target': 'time'},
                {'source': 'nemarea', 'target': 'stat_area_id'},
                {'source': 'A', 'target': 'a'},
                {'source': 'value', 'target': 'value'},
                {'source': 'hours_fished', 'target': 'hours_fished'},
                {'source': 'lat', 'target': 'lat'},
                {'source': 'lon', 'target': 'lon'}
            ],
            logger=logger,
            get_count=True,
            commit_interval=1e4,
            limit=1e2
        ) 
        ingestor.ingest()

    def get_cracked_cells_for_stat_area(self, stat_area):
        cracked_cells = []
        Cell = self.dao.schema['sources']['Cell']
        intersecting_cells = self.dao.session.query(Cell).filter(
            stat_area.geom.intersects(Cell.geom))
        for icell in intersecting_cells:
            intersection = gis_util.get_intersection(
                gis_util.wkb_to_shape(str(stat_area.geom.geom_wkb)),
                gis_util.wkb_to_shape(str(icell.geom.geom_wkb)),
            )
            intersection_area = gis_util.get_shape_area(
                intersection)

            pct_area = intersection_area/icell.area

            # Set cracked cell values in proportion to percentage
            # of parent cell's area.
            ccell_keyed_values = {}
            for effort_key, icell_values in icell.keyed_values.items():
                ccell_values = ccell_keyed_values.setdefault(effort_key, {})
                for attr, value in icell_values.items():
                    ccell_values[attr] = pct_area * value

            cracked_cells.append(models.CrackedCell(
                parent_cell=icell,
                area=intersection_area,
                keyed_values=ccell_keyed_values,
            ))
        return cracked_cells

    def print_cells(self):
        """ Utility method for debugging. """
        cells = self.dao.session.query(
            self.dao.schema['sources']['Cell'])
        for cell in cells:
            print cell.__dict__
