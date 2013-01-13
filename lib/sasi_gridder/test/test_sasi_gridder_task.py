from sasi_gridder.sasi_gridder_task import SASIGridderTask
from sasi_data.util import data_generators as dg
import sasi_data.util.shapefile as shapefile_util
import sys
import unittest
import logging
import tempfile
import shutil
import os
import csv
import platform


def frange(*args):
    """A float range generator."""
    start = 0.0
    step = 1.0
    l = len(args)
    if l == 1:
        end = args[0]
    elif l == 2:
        start, end = args
    elif l == 3:
        start, end, step = args
        if step == 0.0:
            raise ValueError, "step must not be zero"
    else:
        raise TypeError, "frange expects 1-3 arguments, got %d" % l
    v = start
    while True:
        if (step > 0 and v >= end) or (step < 0 and v <= end):
            raise StopIteration
        yield v
        v += step

class SASIGridderTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(clz):
        clz.tmp_dir = tempfile.mkdtemp(prefix="sgTest.")
        clz.grid_path = clz.generateMockGrid(clz.tmp_dir)
        clz.stat_areas_path = clz.generateMockStatAreas(clz.tmp_dir)
        clz.raw_efforts_path = clz.generateMockRawEfforts(clz.tmp_dir)

    @classmethod
    def tearDownClass(clz):
        if hasattr(clz, 'tmp_dir') and clz.tmp_dir.startswith('/tmp'):
            pass
            #shutil.rmtree(clz.tmp_dir)

    @classmethod
    def generateMockGrid(clz, dir_):
        shpfile = os.path.join(dir_, "grid.shp")
        schema = {
            'geometry': 'MultiPolygon',
            'properties': {
                'ID': 'int'
            }
        }
        coord_sets = [
            [[dg.generate_polygon_coords(x0=0, x1=2, y0=-1, y1=1)]],
            [[dg.generate_polygon_coords(x0=0, x1=2, y0=0, y1=1)]]
        ]
        records = []
        i = 0
        for coord_set in coord_sets:
            i += 1
            records.append({
                'id': i,
                'geometry': {
                    'type': 'MultiPolygon',
                    'coordinates': coord_set
                },
                'properties': {
                    'ID': i
                }
            })
        return clz.generate_shapefile(shpfile=shpfile, schema=schema,
                                      records=records)

    @classmethod
    def generateMockStatAreas(clz, dir_):
        shpfile = os.path.join(dir_, "stat_areas.shp")
        schema = {
            'geometry': 'MultiPolygon',
            'properties': {
                'SAREA': 'int'
            }
        }
        coords = [[dg.generate_polygon_coords(x0=1, x1=2, y0=-1, y1=2)]]
        records = [{
            'id': 1,
            'geometry': {
                'type': 'MultiPolygon',
                'coordinates': coords
            },
            'properties': {
                'SAREA': 1
            }
        }]
        return clz.generate_shapefile(shpfile=shpfile, schema=schema,
                                      records=records)
    
    @classmethod
    def generate_shapefile(clz, shpfile=None, crs='EPSG:4326', schema=None, 
                           records=None):
        if not shpfile:
            hndl, shpfile = tempfile.msktemp(suffix=".shp")
        w = shapefile_util.get_shapefile_writer(
            shapefile=shpfile, 
            crs=crs,
            schema=schema
        )
        for record in records:
            w.write(record)
        w.close()
        return shpfile

    @classmethod
    def generateMockRawEfforts(clz, dir_):
        csv_path = os.path.join(dir_, 'raw_efforts.csv')
        csv_file = open(csv_path, "w")
        w = csv.writer(csv_file)
        fields = ['nemarea', 'trip_type', 'A', 'hours_fished', 'value', 
                  'year', 'lat', 'lon']
        records = [
            # Cell A
            {'lat': .5, 'lon': .5, 'A': 1},
            # Cell B
            {'lat': -.5, 'lon': .5, 'A': 2},
            # Stat Area 1
            {'nemarea': 1, 'A': 3},
            # Out-of-domain
            {'A': 6}
        ]
        w.writerow(fields)
        for r in records:
            r['trip_type'] = 'otter'
            r['year'] = '1'
            w.writerow([r.get(f) for f in fields])
        csv_file.close()
        return csv_path

    def test_gridder_task(self):

        logger = logging.getLogger('test_gridder_task')
        logger.addHandler(logging.StreamHandler())
        logger.setLevel(logging.INFO)

        output_path = os.path.join(self.tmp_dir, "output.csv")
        task = SASIGridderTask(
            logger=logger,
            raw_efforts_path=self.raw_efforts_path,
            grid_path=self.grid_path,
            stat_areas_path=self.stat_areas_path,
            output_path=output_path
        )
        task.call()
        output_file = open(output_path, "rb")
        reader = csv.DictReader(output_file)
        results = [r for r in reader]
        results.sort(key=lambda r: r['cell_id'])
        output_file.close()
        self.assertEquals(
            results, 
            [
                {'a': '8.0', 'cell_id': '1', 'gear_id': 'GC10', 'hours_fished': '0.0', 'value': '0.0', 'time': '1.0'},
                {'a': '4.0', 'cell_id': '2', 'gear_id': 'GC10', 'hours_fished': '0.0', 'value': '0.0', 'time': '1.0'}
            ]
        )


if __name__ == '__main__':
    unittest.main()
