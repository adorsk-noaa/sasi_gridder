config = {
    'CACHE_DIR': 'cache',
    'TARGET_DIR': 'assets'
}

assets = {
    'setuptools.egg' : {
        'type': 'url',
        'source': 'http://pypi.python.org/packages/2.7/s/setuptools/setuptools-0.6c11-py2.7.egg'
    },
    'blinker' : {
        'type': 'url',
        'source': 'http://pypi.python.org/packages/source/b/blinker/blinker-1.2.tar.gz',
        'untar': True,
        'path': 'blinker-1.2/blinker',
    },
    'sasi_data' : {
        'type': 'git',
        'source': 'https://github.com/adorsk-noaa/sasi_data',
        'path': 'lib/sasi_data',
    },
    'sasi_gridder' : {
        'type': 'git',
        'source': 'https://github.com/adorsk-noaa/sasi_gridder',
        'path': 'lib/sasi_gridder',
    },
    'task_manager' : {
        'type': 'git',
        'source': 'https://github.com/adorsk/TaskManager.git',
        'path': 'lib/task_manager',
    },
    'jython.jar' : {
        'type': 'rsync',
        'source': '/home/adorsk/tools/jython/full_build/work/dist/jython-standalone.jar'
    },
   'spring_utilities.py' : {
        'type': 'url',
        'source': 'https://gist.github.com/raw/4524185/gistfile1.txt'
    },
    'jython_runner' : {
        'type': 'git',
        'source': 'https://github.com/adorsk/jython_runner.git'
    },
}
