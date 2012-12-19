from sasi_gridder.sasi_gridder_task import SASIGridderTask
from sqlalchemy import create_engine
import logging
import argparse
import platform
import sys


argparser = argparse.ArgumentParser()
argparser.add_argument('-g', '--grid', help='grid shapefile', required=True)
argparser.add_argument('-e', '--raw-efforts', help='raw efforts csv',
                       required=True)
argparser.add_argument('-s', '--stat-areas', help='stat areas shapefile',
                       required=True)
argparser.add_argument('-o', '--output-path', help='output path')
argparser.add_argument('--db-uri', default='sqlite://')

args = argparser.parse_args()

logger = logging.getLogger('run_gridder_task')
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())

def get_connection():
    if platform.system() == 'Java':
        engine = create_engine(args.db_uri)
        con = engine.connect()
        javaCon = con.connection.__connection__
        from geodb.GeoDB import InitGeoDB
        InitGeoDB(javaCon)
        return con
    else:
        import pyspatialite
        sys.modules['pysqlite2'] = pyspatialite
        engine = create_engine(args.db_uri)
        con = engine.connect()
        con.execute('SELECT InitSpatialMetadata()')
        return con

task = SASIGridderTask(
    grid_path=args.grid,
    raw_efforts_path=args.raw_efforts,
    stat_areas_path=args.stat_areas,
    output_path=args.output_path,
    logger=logger,
    get_connection=get_connection,
)
task.call()
