# Add setuptools egg to path.
import sys 
import os
this_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(this_dir, 'setuptools.egg'))
from sasi_gridder import jython_gui

def main():
    jython_gui.main()

if __name__ == '__main__':
    main()
