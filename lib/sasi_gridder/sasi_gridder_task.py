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
from sqlalchemy.sql import literal_column, select, and_
import sasi_data.ingestors as ingestors
import sasi_data.util.gis as gis_util

import tempfile
import os
import shutil
import zipfile
import logging
import csv
from math import floor


class SpatialHash(object):
    def __init__(self, cell_size=.05):
        self.cell_size = float(cell_size)
        self.d = {}

    def _add(self, cell_coord, o):
        """Add the object o to the cell at cell_coord."""
        try:
            self.d.setdefault(cell_coord, set()).add(o)
        except KeyError:
            self.d[cell_coord] = set((o,))

    def _cell_for_point(self, p):
        cx = floor(p[0]/self.cell_size)
        cy = floor(p[1]/self.cell_size)
        return (int(cx), int(cy))

    def _cells_for_rect(self, r):
        cells = set()
        cy = floor(r[1] / self.cell_size)
        while (cy * self.cell_size) <= r[3]:
            cx = floor(r[0] / self.cell_size)
            while (cx * self.cell_size) <= r[2]:
                cells.add((int(cx), int(cy)))
                cx += 1.0
            cy += 1.0
        return cells

    def add_rect(self, r, obj):
        cells = self._cells_for_rect(r)
        for c in cells:
            self._add(c, obj)

    def items_for_point(self, p):
        cell = self._cell_for_point(p)
        return self.d.get(cell, set())

    def items_for_rect(self, r):
        cells = self._cells_for_rect(r)
        items = set()
        for c in cells:
            items.update(self.d.get(c, set()))
        return items

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

        self.con = self.get_connection()
        self.trans = self.con.begin()
        session = sessionmaker()(bind=self.con)

        # Create spatial hash for cells.
        # Cell size of about .1 (degrees) seems to work well.
        self.cell_spatial_hash = SpatialHash(cell_size=.1)

        # Read in data.
        base_msg = "Ingesting..."
        ingest_logger = self.get_logger_logger('ingest', base_msg,
                                               self.logger)
        self.message_logger.info(base_msg)
        self.dao = SASIGridderDAO(session=session)

        # Read in cells and add to spatial hash.
        self.ingest_cells(parent_logger=ingest_logger, limit=None)

        # Create dict to hold cell values.
        self.c_values = {}
        for cell in self.dao.query('__Cell'):
            self.c_values[cell.id] = {}

        # Read in stat_areas.
        self.ingest_stat_areas(parent_logger=ingest_logger)

        # Create dict to hold stat area values.
        self.sa_values = {}
        for stat_area in self.dao.query('__StatArea'):
            self.sa_values[stat_area.id] = {}

        #
        #  Main part of the gridding task.
        #   

        base_msg = "Starting gridding."
        gridding_logger = self.get_logger_logger('gridding', base_msg,
                                                  self.logger)
        self.message_logger.info(base_msg)

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


        # Do the first pass on efforts
        # as we read them in.
        

        base_msg = "Assigning raw efforts to cells/stat_areas ... "
        fp_logger = self.get_logger_logger('first_pass', base_msg,
                                              gridding_logger)
        fp_logger.info(base_msg)

        unassigned = {}

        logging_interval = 1e4

        # Define functions to handle raw effort columns
        def trip_type_to_gear_id(trip_type):
            return self.trip_type_gear_mappings.get(trip_type)

        def float_w_empty_dot(value):
            if value == '.' or value == '':
                return None
            elif value is not None:
                return float(value)

        # Define function to execute after each raw effort is mapped to an
        # effort column. This is the first pass described above.
        def first_pass(effort, effort_counter): 
            # If effort has lat and lon...
            if effort.lat is not None and effort.lon is not None:
                # Can effort can be assigned to cell?
                cell = self.get_cell_for_pos(effort.lat, effort.lon)
                if cell:
                    self.add_effort_to_cell(cell, effort)
                    return

                # Otherwise can effort can be assigned to statarea?
                stat_area = self.get_stat_area_for_pos(
                    effort.lat, effort.lon)
                if stat_area:
                    self.add_effort_to_stat_area(stat_area, effort)
                    return

                # Otherwise add to unassigned.
                else:
                    self.add_effort_to_unassigned(unassigned, effort)
                    return

            # Otherwise if effort has a stat area...
            elif effort.stat_area_id is not None:
                stat_area = self.dao.session.query(
                    self.dao.schema['sources']['StatArea']).get(
                        effort.stat_area_id)
                if not stat_area:
                    self.add_effort_to_unassigned(unassigned, effort)
                    return
                else:
                    self.add_effort_to_stat_area(stat_area, effort)

            # Otherwise add to unassigned list.
            else:
                self.add_effort_to_unassigned(unassigned, effort)


        # Define ingestor class to read raw efforts.
        # The ingestor will execute the 'first_pass' function
        # for each mapped record object.
        class EffortIngestor(ingestors.CSV_Ingestor):
            def __init__(self, *args, **kwargs):
                super(EffortIngestor, self).__init__(*args, **kwargs)

            def set_target_attr(self, target, attr, value):
                setattr(target, attr, value)

            def after_record_mapped(self, source_record, target_record, 
                                    counter): 
                first_pass(target_record, counter)
            def initialize_target_record(self, counter):
                return models.Effort()

        # Create ingestor instance.
        ingestor = EffortIngestor(
            csv_file=self.raw_efforts_path,
            mappings=[
                {'source': 'trip_type', 'target': 'gear_id', 
                 'processor': trip_type_to_gear_id},
                {'source': 'year', 'target': 'time',
                 'processor': float_w_empty_dot},
                {'source': 'nemarea', 'target': 'stat_area_id',
                 'processor': float_w_empty_dot},
                {'source': 'A', 'target': 'a',
                 'processor': float_w_empty_dot},
                {'source': 'value', 'target': 'value',
                 'processor': float_w_empty_dot},
                {'source': 'hours_fished', 'target': 'hours_fished',
                 'processor': float_w_empty_dot},
                {'source': 'lat', 'target': 'lat', 
                 'processor': float_w_empty_dot},
                {'source': 'lon', 'target': 'lon',
                 'processor': float_w_empty_dot}
            ],
            logger=fp_logger,
            get_count=True,
            #limit=1e3
        ) 

        # Run the ingestor, and thus do the first pass.
        ingestor.ingest()

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

        base_msg = "Distributing stat_area values to cells ... "
        sa_logger = self.get_logger_logger('stat_areas', base_msg,
                                              gridding_logger)
        sa_logger.info(base_msg)

        stat_areas = self.dao.session.query(
            self.dao.schema['sources']['StatArea'])
        num_stat_areas = stat_areas.count()
        logging_interval = 1
        sa_counter = 0
        for stat_area in stat_areas:
            sa_counter += 1
            if (sa_counter % logging_interval) == 0:
                sa_logger.info("stat_area %s of %s (%.1f%%)" % (
                    sa_counter, num_stat_areas, 
                    100.0 * sa_counter/num_stat_areas))

            # Get stat area values.
            sa_keyed_values = self.sa_values[stat_area.id]

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
                pcell_keyed_values = self.c_values[ccell.parent_cell.id]
                for effort_key, sa_values in sa_keyed_values.items():
                    ccell_totals_values = ccell_totals.get(effort_key)
                    ccell_values = ccell.keyed_values.get(effort_key)
                    pcell_values = pcell_keyed_values.setdefault(
                        effort_key, self.new_values_dict())
                    if not ccell_totals_values or not ccell_values:
                        continue
                    for attr, sa_value in sa_values.items():
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
                        pcell_values[attr] += sa_value * pct_value

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
        base_msg = "Distributing unassigned values to cells ... "
        unassigned_logger = self.get_logger_logger('unassigned', base_msg,
                                              gridding_logger)
        unassigned_logger.info(base_msg)

        # Calculate totals across all cells.
        totals = {}
        cells = self.dao.session.query(
            self.dao.schema['sources']['Cell'])
        num_cells = cells.count()
        for cell in cells:
            cell_keyed_values = self.c_values[cell.id]
            for effort_key, cell_values in cell_keyed_values.items():
                totals_values = totals.setdefault(
                    effort_key, 
                    self.new_values_dict()
                )
                for attr, cell_value in cell_values.items():
                    totals_values[attr] += cell_value

        # Distribute unassigned efforts across all cells,
        # in proportion to the cell's values as a percentage of the total.
        logging_interval = 1e3
        cell_counter = 0
        for cell in cells:
            cell_counter += 1
            if (cell_counter % logging_interval) == 0:
                unassigned_logger.info("cell %s of %s (%.1f%%)" % (
                    cell_counter, num_cells, 100.0 * cell_counter/num_cells))

            for effort_key, unassigned_values in unassigned.items():
                cell_values = self.c_values[cell.id].get(effort_key)
                if not cell_values:
                    continue
                for attr, unassigned_value in unassigned_values.items():
                    if not unassigned_value:
                        continue
                    cell_value = cell_values.get(attr, 0.0)
                    pct_value = cell_value/unassigned_value
                    cell_values[attr] += unassigned_value * pct_value

        # Done with gridding. At this point the effort has been distributed. 

        # Note that there may be some efforts which are not included.
        # For example, if an unassigned effort has an effort_key which is 
        # not used by any effort assigned to a cell or a stat_area, then 
        # no cell will have a non-zero pct_value for that effort_key.

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
            cell_keyed_values = self.c_values[cell.id]
            for keys, values in cell_keyed_values.items():
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
        self.data['output_file'] = self.output_path
        self.status = 'resolved'

    def get_logger_logger(self, name=None, base_msg=None, parent_logger=None):
        logger = logging.getLogger("%s_%s" % (id(self), name))
        formatter = logging.Formatter(base_msg + ' %(message)s.')
        log_handler = LoggerLogHandler(parent_logger)
        log_handler.setFormatter(formatter)
        logger.addHandler(log_handler)
        logger.setLevel(self.message_logger.level)
        return logger

    def get_cell_for_pos_db(self, lat, lon):
        """ Get cell which contains a given lat lon.
        Returns None if no cell could be found.
        Via the db."""
        Cell = self.dao.schema['sources']['Cell']
        return self.get_obj_for_pos(Cell, lat, lon)

    def get_cell_for_pos_spatial_hash(self, lat, lon):
        """
        Get cell which contains given point, via
        spatial hash.
        """
        pos_wkt = 'POINT(%s %s)' % (lon, lat)
        candidates = self.cell_spatial_hash.items_for_point((lon,lat))
        for c in candidates:
            c_shp = gis_util.wkb_to_shape(str(c.geom.geom_wkb))
            pnt_shp = gis_util.wkt_to_shape(pos_wkt)
            if gis_util.get_intersection(c_shp, pnt_shp):
                return c
        return None
    get_cell_for_pos = get_cell_for_pos_spatial_hash

    def get_stat_area_for_pos(self, lat, lon):
        """ Get statarea which contains a given lat lon.
        Returns None if no StatArea could be found."""
        StatArea = self.dao.schema['sources']['StatArea']
        return self.get_obj_for_pos(StatArea, lat, lon)

    def get_obj_for_pos(self, clazz, lat, lon):
        pos_wkt = 'POINT(%s %s)' % (lon, lat)

        # Use indices to speed things up...
        engine_url = self.dao.session.connection().engine.url
        if 'sqlite' in engine_url.drivername:
            table = clazz.__name__.lower()
            search_frame = literal_column('BuildCircleMbr(%s, %s, 1)' % (lon, lat))
            subq = self.dao.session.query(clazz.id).filter(
                literal_column('ROWID').in_(
                    select(
                        ["ROWID FROM SpatialIndex"],
                        and_(
                            (literal_column('f_table_name') == table),
                            (literal_column('search_frame') == search_frame),
                        )
                    )
                )
            ).subquery('idx_subq')

            q = self.dao.session.query(clazz).join(subq, clazz.id == subq.c.id)
            q = q.filter(func.ST_Contains(
                clazz.geom, func.ST_GeomFromText(pos_wkt, 4326)))
            objs = q.all()
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

    def add_effort_to_keyed_values_dict(self, kvd, effort):
        values = kvd.setdefault(
            self.get_effort_key(effort), 
            self.new_values_dict()
        )
        self.update_values_dict(values, effort)

    def add_effort_to_cell(self, cell, effort):
        cell_keyed_values = self.c_values[cell.id]
        self.add_effort_to_keyed_values_dict(cell_keyed_values, effort)

    def add_effort_to_stat_area(self, stat_area, effort):
        sa_keyed_values = self.sa_values[stat_area.id]
        self.add_effort_to_keyed_values_dict(sa_keyed_values, effort)

    def add_effort_to_unassigned(self, unassigned, effort):
        self.add_effort_to_keyed_values_dict(unassigned, effort)

    def get_effort_key(self, effort):
        """  Key for grouping values by effort types. """
        return tuple([getattr(effort, attr, None) for attr in self.key_attrs])

    def ingest_cells(self, parent_logger=None, limit=None):
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
            commit_interval=None,
            limit=limit
        ) 
        ingestor.ingest()
        self.commit()

        # Calculate cell areas and add cells to spatial hash.
        for cell in self.dao.session.query(
            self.dao.schema['sources']['Cell']):
            cell_shape = gis_util.wkb_to_shape(str(cell.geom.geom_wkb))
            cell.area = gis_util.get_shape_area(cell_shape)
            cell_mbr = gis_util.get_shape_mbr(cell_shape)
            self.cell_spatial_hash.add_rect(cell_mbr, cell)
            self.dao.save(cell, commit=False)
        self.commit()

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
            commit_interval=None,
        ) 
        ingestor.ingest()
        self.commit()

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
            icell_keyed_values = self.c_values[icell.id]
            for effort_key, icell_values in icell_keyed_values.items():
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
            print id(cell), cell.__dict__

    def commit(self):
        self.dao.commit()
        self.trans.commit()
        self.trans = self.con.begin()
