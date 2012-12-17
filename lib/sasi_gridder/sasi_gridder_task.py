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

            # DO GRIDDING HERE.

        except Exception as e:
            self.logger.exception("Error gridding : %s" % e)
            raise e

        # Output gridded efforts.

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
