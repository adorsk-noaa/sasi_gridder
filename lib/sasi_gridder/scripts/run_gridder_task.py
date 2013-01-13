from sasi_gridder.sasi_gridder_task import SASIGridderTask
import logging
import argparse
import platform
import sys
import csv


argparser = argparse.ArgumentParser()
argparser.add_argument('-g', '--grid', help='grid shapefile', required=True)
argparser.add_argument('-e', '--raw-efforts', help='raw efforts csv',
                       required=True)
argparser.add_argument('-s', '--stat-areas', help='stat areas shapefile',
                       required=True)
argparser.add_argument('-o', '--output-path', help='output path')
argparser.add_argument('-l', '--effort-limit', help='output path', type=int)
argparser.add_argument('-m', '--mappings-file', help='mappings file')

args = argparser.parse_args()

logger = logging.getLogger('run_gridder_task')
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())

if args.mappings_file:
    gear_mappings = {}
    with open(args.mappings_file, 'rb') as f:
        r = csv.DictReader(f)
        for mapping in r:
            gear_mappings[mapping['trip_type']] = mapping['gear_code']
else:
    gear_mappings = None

task = SASIGridderTask(
    grid_path=args.grid,
    raw_efforts_path=args.raw_efforts,
    stat_areas_path=args.stat_areas,
    output_path=args.output_path,
    logger=logger,
    effort_limit=args.effort_limit,
    gear_mappings=gear_mappings,
)
task.call()
