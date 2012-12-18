from sasi_data.util import data_generators as dg
import sasi_data.util.shapefile as shapefile_util
import unittest
import logging
import tempfile
import shutil
import os


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
        #clz.efforts_path = clz.generateMockRawEfforts(clz.tmp_dir)

    @classmethod
    def tearDownClass(clz):
        if hasattr(clz, 'tmp_dir') and clz.tmp_dir.startswith('/tmp'):
            pass
            #shutil.rmtree(clz.tmp_dir)

    @classmethod
    def generateMockGrid(clz, dir_, x0=0, xf=4, dx=2, y0=0, yf=4, dy=2):
        shpfile = os.path.join(dir_, "grid.shp")
        schema = {
            'geometry': 'MultiPolygon',
            'properties': {
                'ID': 'int'
            }
        }
        records = []
        i = 0
        for j in frange(x0, xf, dx):
            for k in frange(y0, yf, dy):
                coords = [[dg.generate_polygon_coords(x=j, dx=dx, y=k, dy=dy)]]
                records.append({
                    'id': i,
                    'geometry': {
                        'type': 'MultiPolygon',
                        'coordinates': coords
                    },
                    'properties': {
                        'ID': i
                    }
                })
                i += 1
        return clz.generate_shapefile(shpfile=shpfile, schema=schema,
                                      records=records)

    @classmethod
    def generateMockStatAreas(clz, dir_, x0=0, xf=4, dx=2, y0=1, yf=3, dy=2):
        shpfile = os.path.join(dir_, "stat_areas.shp")
        schema = {
            'geometry': 'MultiPolygon',
            'properties': {
                'ID': 'int'
            }
        }
        records = []
        i = 0
        for j in frange(x0, xf, dx):
            for k in frange(y0, yf, dy):
                coords = [[dg.generate_polygon_coords(x=j, y=k, dx=dx, dy=dy)]]
                records.append({
                    'id': i,
                    'geometry': {
                        'type': 'MultiPolygon',
                        'coordinates': coords
                    },
                    'properties': {
                        'ID': i
                    }
                })
                i += 1
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
        pass

    def test_foo(self):
        print "foo"

if __name__ == '__main__':
    unittest.main()