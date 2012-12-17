"""
Task for gridding SASI Efforts.
"""

# TODO: use custom dao for adding nemareas.
from sasi_data.dao.sasi_sa_dao import SASI_SqlAlchemyDAO
import task_manager
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import tempfile
import os
import shutil
import zipfile
import logging


class LoggerLogHandler(logging.Handler):
    """ Custom log handler that logs messages to another
    logger. This can be used to chain together loggers. """
    def __init__(self, logger=None, **kwargs):
        logging.Handler.__init__(self, **kwargs)
        self.logger = logger

    def emit(self, record):
        self.logger.log(record.levelno, self.format(record))

class SasiGridderTask(task_manager.Task):

    def __init__(self, config={}, data={}, 
                 get_connection=None, max_mem=1e9, config={},
                 **kwargs):
        super(RunSasiTask, self).__init__(**kwargs)
        self.logger.debug("RunSasiTask.__init__")

        self.data = data
        self.config = config
        self.value_attrs = ['a', 'value', 'hours_fished']

        for kwarg in ['raw_efforts_path', 'grid_path', 'nemareas_path']:
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
            ingest_logger = self.get_logger_for_stage('ingest', base_msg)
            self.message_logger.info(base_msg)
            # TODO: use custom DAO and ingestor.
            #dao = SASI_SqlAlchemyDAO(session=session)
            dao = none
            #sasi_ingestor.ingest(data_dir=data_dir)
            ingestor = None
        except Exception as e:
            self.logger.exception("Error ingesting")
            raise e

        # Grid the efforts
        try:
            base_msg = "Starting gridding."
            run_model_logger = self.get_logger_for_stage('gridding', base_msg)
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

            for effort in raw_efforts:

                # If effort has lat and lon...
                if effort.lat is not None and effort.lon is not None:
                    # Can effort can be assigned to cell?
                    cell = self.get_cell_for_effort(effort.lat, effort.lon):
                    if cell:
                        self.add_effort_to_cell(cell, effort)
                        continue

                    # Otherwise can effort can be assigned to statarea?
                    stat_area = self.get_stat_area_for_pos(
                        effort.lat, effort.lon)
                    if stat_area:
                        self.add_effort_to_stat_area(stat_area, effort)
                        continue

                    # Otherwise add to unassigned.
                    else:
                        self.add_effort_to_unassigned(unassigned, effort)
                        continue

                # Otherwise if effort has a stat area...
                else if effort.stat_area is not None:
                    self.add_effort_to_stat_area(effort.stat_area, effort)

                # Otherwise add to unassigned list.
                else:
                    self.add_effort_to_unassigned(unassigned, effort)

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

            for stat_area in stat_areas:

                # Calculate totals for efforts assigned to the stat area.
                stat_area_totals = {}
                for effort_key, effort_values in stat_area.aggregates.items():
                    stat_area_values = stat_area_totals.setdefault(
                        effort_key,
                        self.new_values_dict()
                    )
                    for attr, effort_value in effort_values.items():
                        stat_area_values[attr] += effort_value

                # Get list of cracked cells.
                cracked_cells = get_cracked_cells_for_stat_area(statarea)

                # Calculate totals for values across cracked cells.
                ccell_totals = {}
                for ccell in cracked_cells:
                    for effort_key, ccell_values in ccell.aggregates.items():
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
                    for effort_key, sa_values in stat_area.aggregates.items():
                        ccell_values = ccell.aggregates.get(effort_key)
                        if not ccell_values:
                            continue
                        for attr, sa_value in sa_values.items():
                            # Don't add anything for empty values.
                            # This also avoids division by zero errors.
                            if not sa_value:
                                continue
                            ccell_value = ccell_values.get(attr, 0.0)
                            pct_value = ccell_value/sa_value
                            # Add proportional value to cracked cell's parent
                            # cell.
                            pcell = ccell.parent_cell
                            pcell[effort_key][attr] = sa_value * pct_value


            #
            # 4. For efforts which could not be assigned to a cell or a stat area
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
            for cell in cells:
                for effort_key, cell_values in cell.aggregates.items():
                    totals_values = totals.setdefault(
                        effort_key, 
                        self.get_new_values_dict()
                    )
                    for attr, cell_value in cell_values.items():
                        totals_values[attr] += cell_value

            # Distribute unassigned efforts across all cells,
            # in proportion to the cell's values as a percentage of the total.
            for effort_key, unassigned_values in unassigned.items():
                for cell in cells:
                    cell_values = cell.aggregates.get(effort_key)
                    if not cell_values:
                        continue
                    for attr, unassigned_value in unassigned_values.items():
                        if not unassigned_value:
                            continue
                        cell_value = cell_values.get(attr, 0.0)
                        pct_value = cell_value/unassigned_value
                        cell_values[attr] += unassigned_value * pct_value

            # Done! At this point the effort has been distributed. 

            # Note that there may be some efforts which do not included.
            # For example, if an unassigned effort has an effort_key which is 
            # not used by any effort assigned to a cell or a stat_area, then 
            # no cell will have a non-zero pct_value for that effort_key.

        except Exception as e:
            self.logger.exception("Error gridding : %s" % e)
            raise e

        #
        # Output gridded efforts.
        # @TODO
        #

        shutil.rmtree(build_dir)

        self.progress = 100
        self.message_logger.info("Gridding completed, output file is:'%s'" % (
            self.output_path))
        self.status = 'resolved'

    def get_logger_for_stage(self, stage_id=None, base_msg=None):
        logger = logging.getLogger("%s_%s" % (id(self), stage_id))
        formatter = logging.Formatter(base_msg + ' %(message)s.')
        log_handler = LoggerLogHandler(self.message_logger)
        log_handler.setFormatter(formatter)
        logger.addHandler(log_handler)
        logger.setLevel(self.message_logger.level)
        return logger

    def get_cell_for_pos(self, lat, lon):
        """ Get cell which contains a given lat lon.
        Returns None if no cell could be found."""
        pass

    def get_statarea_for_pos(self, lat, lon):
        """ Get statarea which contains a given lat lon.
        Returns None if no cell could be found."""

    def new_values_dict(self):
        return dict(zip(self.value_attrs, [0.0] * len(self.value_attrs)))

    def add_effort_values(self, values, effort):
        for k in values.keys():
            effort_value = getattr(effort, k, 0.0)
            values[k] += effort_value

    def add_effort_to_obj(self, obj, effort, aggregates_attr='aggregates'):
        aggregates_dict = getattr(obj, aggregates_attr)
        self.add_effort_dict(aggregates_dict, effort)

    def add_effort_to_dict(self, dict_, effort):
        values = dict_.setdefault(
            self.get_effort_key(effort), 
            self.new_values_dict()
        )
        self.add_effort_values(values, effort)

    def add_effort_to_cell(self, cell, effort):
        self.add_effort_to_obj(cell, effort)

    def add_effort_to_stat_area(self, stat_area, effort):
        self.add_effort_to_obj(stat_area, effort)

    def add_effort_to_unassigned(self, unassigned, effort):
        self.add_effort_to_dict(unassigned, effort)

    def get_effort_key(self, effort):
        """  Aggregate key for grouping efforts. """
        return (effort.gear_id,)
