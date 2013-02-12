# Add setuptools egg to path.
import sys 
import os
this_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(this_dir, 'setuptools.egg'))
from sasi_gridder import jython_gui

def split_path(path):
    parts = []
    while True:
        newpath, tail = os.path.split(path)
        if newpath == path:
            assert not tail
            if path: parts.append(path)
            break
        parts.append(tail)
        path = newpath
    parts.reverse()
    return parts

instructions_dir = os.path.join(os.path.join(*(split_path(this_dir)[:-2])),
                                'doc')
instructionsURI = "file://%s/index.html" % instructions_dir

def main():
    jython_gui.main(instructionsURI=instructionsURI)

if __name__ == '__main__':
    main()
