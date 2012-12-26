"""
Task for gridding SASI Efforts.
"""

from sasi_gridder import models as models
from sasi_data.ingestors.ingestor import Ingestor
from sasi_data.ingestors.processor import Processor
from sasi_data.ingestors.csv_reader import CSVReader
from sasi_data.ingestors.shapefile_reader import ShapefileReader
from sasi_data.ingestors.dict_writer import DictWriter 
from sasi_data.ingestors.mapper import ClassMapper
import sasi_data.util.gis as gis_util
import task_manager

import tempfile
import os
import shutil
import zipfile
import logging
import csv
from math import floor
from time import time
import inspect

def ln_(msg=""):
    return "%s (%s)" % (msg, inspect.currentframe().f_back.f_lineno)

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

    def __init__(self, config={}, data={}, **kwargs):
        super(SASIGridderTask, self).__init__(**kwargs)
        self.logger.debug("RunSasiTask.__init__")

        self.data = data
        self.config = config
        self.value_attrs = models.Effort.value_attrs
        self.key_attrs = ['gear_id', 'time']

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

        # Create spatial hashes for cells and stat areas.
        # Cell size of about .1 (degrees) seems to work well.
        self.cell_spatial_hash = SpatialHash(cell_size=.1)
        self.sa_spatial_hash = SpatialHash(cell_size=.1)

        # Read in data.
        base_msg = "Ingesting..."
        ingest_logger = self.get_logger_logger('ingest', base_msg,
                                               self.logger)
        self.message_logger.info(base_msg)

        # Read in cells.
        self.ingest_cells(parent_logger=ingest_logger, limit=None)

        # Read in stat_areas.
        self.ingest_stat_areas(parent_logger=ingest_logger)

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
        c_ = {
            'll': 0,
            'll_c': 0,
            'll_sa': 0,
            'll_ua': 0,
            'sa': 0,
            'sa_ua': 0,
            'ua': 0
        }
        
        def first_pass(data=None, **kwargs):
            effort = data
            #if (effort_counter % 1e3) == 0:
            #    print ["%s: %.3e" % (k, v) for k,v in c_.items()]

            # If effort has lat and lon...
            if effort.lat is not None and effort.lon is not None:
                c_['ll'] += 1
                # Can effort can be assigned to cell?
                cell = self.get_cell_for_pos(effort.lat, effort.lon)
                if cell:
                    c_['ll_c'] += 1
                    self.add_effort_to_cell(cell, effort)
                    return

                # Otherwise can effort can be assigned to statarea?
                stat_area = self.get_stat_area_for_pos(
                    effort.lat, effort.lon)
                if stat_area:
                    c_['ll_sa'] += 1
                    self.add_effort_to_stat_area(stat_area, effort)
                    return

                # Otherwise add to unassigned.
                else:
                    c_['ll_ua'] += 1
                    self.add_effort_to_unassigned(unassigned, effort)
                    return

            # Otherwise if effort has a stat area...
            elif effort.stat_area_id is not None:
                c_['sa'] += 1
                stat_area = self.stat_areas.get(effort.stat_area_id)
                if not stat_area:
                    c_['sa_ua'] += 1
                    self.add_effort_to_unassigned(unassigned, effort)
                    return
                else:
                    self.add_effort_to_stat_area(stat_area, effort)
                    return

            # Otherwise add to unassigned list.
            else:
                c_['ua'] += 1
                self.add_effort_to_unassigned(unassigned, effort)
                return

        # Create and run effort ingestor.
        ingestor = Ingestor(
            reader=CSVReader(csv_file=self.raw_efforts_path),
            processors=[
                ClassMapper(
                    clazz=models.Effort,
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
                ),
                first_pass,
            ],
            logger=fp_logger,
            get_count=True,
            #limit=1e3
        ).ingest() 

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

        num_stat_areas = len(self.stat_areas)
        logging_interval = 1
        sa_counter = 0
        for stat_area in self.stat_areas.values():
            sa_counter += 1
            if (sa_counter % logging_interval) == 0:
                sa_logger.info("stat_area %s of %s (%.1f%%)" % (
                    sa_counter, num_stat_areas, 
                    100.0 * sa_counter/num_stat_areas))

            # Get stat area values.
            sa_keyed_values = self.sa_values.setdefault(stat_area.id, {})

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
                pcell_keyed_values = self.c_values.setdefault(
                    ccell.parent_cell.id, {})
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
                        ccell_totals_value = ccell_totals_values.get(attr, 0.0)
                        if not ccell_value or not ccell_totals_value:
                            continue
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
        num_cells = len(self.cells)
        for cell in self.cells.values():
            cell_keyed_values = self.c_values.setdefault(cell.id, {})
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
        for cell in self.cells.values():
            cell_counter += 1
            if (cell_counter % logging_interval) == 0:
                unassigned_logger.info("cell %s of %s (%.1f%%)" % (
                    cell_counter, num_cells, 100.0 * cell_counter/num_cells))

            cell_keyed_values = self.c_values.setdefault(cell.id, {})
            for effort_key, unassigned_values in unassigned.items():
                cell_values = cell_keyed_values.get(effort_key)
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

        for cell in self.cells.values():
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

    def get_cell_for_pos(self, lat, lon):
        """
        Get cell which contains given point, via
        spatial hash.
        """
        pos_wkt = 'POINT(%s %s)' % (lon, lat)
        candidates = self.cell_spatial_hash.items_for_point((lon,lat))
        for c in candidates:
            pnt_shp = gis_util.wkt_to_shape(pos_wkt)
            if gis_util.get_intersection(c.shape, pnt_shp):
                return c
        return None

    def get_stat_area_for_pos(self, lat, lon):
        pos_wkt = 'POINT(%s %s)' % (lon, lat)
        candidates = self.sa_spatial_hash.items_for_point((lon,lat))
        for c in candidates:
            pnt_shp = gis_util.wkt_to_shape(pos_wkt)
            if gis_util.get_intersection(c.shape, pnt_shp):
                return c
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
        cell_keyed_values = self.c_values.setdefault(cell.id, {})
        self.add_effort_to_keyed_values_dict(cell_keyed_values, effort)

    def add_effort_to_stat_area(self, stat_area, effort):
        sa_keyed_values = self.sa_values.setdefault(stat_area.id, {})
        self.add_effort_to_keyed_values_dict(sa_keyed_values, effort)

    def add_effort_to_unassigned(self, unassigned, effort):
        self.add_effort_to_keyed_values_dict(unassigned, effort)

    def get_effort_key(self, effort):
        """  Key for grouping values by effort types. """
        return tuple([getattr(effort, attr, None) for attr in self.key_attrs])

    def ingest_cells(self, parent_logger=None, limit=None):
        self.cells = {}
        self.cell_spatial_hash = SpatialHash(cell_size=.1)
        self.c_values = {}
        logger = self.get_logger_logger(
            name='cell_ingest', 
            base_msg='Ingesting cells...',
            parent_logger=parent_logger
        )

        Ingestor(
            reader=ShapefileReader(shp_file=self.grid_path,
                                   reproject_to='EPSG:4326'),
            processors=[
                ClassMapper(
                    clazz=models.Cell,
                    mappings=[{'source': 'ID', 'target': 'id'},
                              {'source': '__shape', 'target': 'shape'},],
                ),
                DictWriter(dict_=self.cells, key_func=lambda c: c.id),
            ],
            logger=logger,
            limit=limit
        ).ingest()

        # Calculate cell areas and add cells to spatial hash.
        for cell in self.cells.values():
            cell.area = gis_util.get_shape_area(cell.shape)
            cell.mbr = gis_util.get_shape_mbr(cell.shape)
            self.cell_spatial_hash.add_rect(cell.mbr, cell)

    def ingest_stat_areas(self, parent_logger=None, limit=None):
        self.stat_areas = {}
        self.sa_spatial_hash = SpatialHash(cell_size=.1)
        self.sa_values = {}
        logger = self.get_logger_logger(
            name='stat_area_ingest', 
            base_msg='Ingesting stat_areas...',
            parent_logger=parent_logger
        )

        Ingestor(
            reader=ShapefileReader(shp_file=self.stat_areas_path,
                                   reproject_to='EPSG:4326'),
            processors=[
                ClassMapper(
                    clazz=models.StatArea,
                    mappings=[{'source': 'SAREA', 'target': 'id'},
                              {'source': '__shape', 'target': 'shape'},],
                ),
                DictWriter(dict_=self.stat_areas, key_func=lambda sa: sa.id),
            ],
            logger=logger,
            limit=limit
        ).ingest()

        # Add to spatial hash.
        for stat_area in self.stat_areas.values():
            stat_area.mbr = gis_util.get_shape_mbr(stat_area.shape)
            self.sa_spatial_hash.add_rect(stat_area.mbr, stat_area)

    def get_cracked_cells_for_stat_area(self, stat_area):
        cracked_cells = []
        candidates = self.cell_spatial_hash.items_for_rect(stat_area.mbr)
        for icell in candidates:
            intersection = gis_util.get_intersection(stat_area.shape, icell.shape)
            if not intersection:
                continue

            intersection_area = gis_util.get_shape_area(intersection)
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
