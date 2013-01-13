import subprocess
import os
import shutil


this_dir = os.path.dirname(os.path.abspath(__file__))
templates_dir = os.path.join(this_dir, 'templates')
assets_dir = os.path.join(this_dir, 'assets')
cache_dir = os.path.join(this_dir, 'cache')
build_dir = os.path.join(this_dir, "build")
dist_dir = os.path.join(this_dir, "dist")
maven_cache_dir = os.path.join(this_dir, 'mvn_cache')
pom_file = os.path.join(this_dir, "pom.xml")


def main():
    # Fetch assets.
    subprocess.call(
        "python -m wrangler.cmds install",
        shell=True
    )

    # Remove build, dist dirs if present
    for dir_ in [build_dir, dist_dir]:
        if os.path.exists(dir_):
            shutil.rmtree(dir_)

    os.makedirs(build_dir)

    # Move assets into dirs.

    # Main dir, from jython_runner template
    base_dir = os.path.join(build_dir, 'sasi_gridder')
    shutil.copytree(
        os.path.join(assets_dir, 'jython_runner'),
        base_dir,
    )

    # jython.jar
    shutil.copyfile(
        os.path.join(assets_dir, 'jython.jar'),
        os.path.join(base_dir, '.jython', 'jython.jar')
    )

    # Pylibs.
    pylib_dir = os.path.join(base_dir, '.lib', 'python')
    for py_asset in ['sasi_data', 'sasi_gridder', 'blinker', 'task_manager',
                     'spring_utilities.py']:
        asset_path = os.path.join(assets_dir, py_asset)
        target_path = os.path.join(pylib_dir, py_asset)
        if os.path.isdir(asset_path):
            shutil.copytree( asset_path, target_path)
        else:
            shutil.copy(asset_path, target_path)

    shutil.copy(
        os.path.join(assets_dir, 'setuptools.egg'),
        os.path.join(pylib_dir, 'setuptools.egg')
    )
    shutil.copy(
        os.path.join(templates_dir, 'setuptools.pth'),
        os.path.join(pylib_dir, 'setuptools.pth')
    )

    entrypoint_target = os.path.join(pylib_dir, 'entrypoint.py')
    if os.path.exists(entrypoint_target):
        os.remove(entrypoint_target)
    shutil.copy(
        os.path.join(templates_dir, 'entrypoint.py'),
        entrypoint_target,
    )

    # Jars.
    javalib_dir = os.path.join(base_dir, '.lib', 'java')
    if not os.path.exists(maven_cache_dir):
        os.makedirs(maven_cache_dir)
    subprocess.call(
        'mvn -f %s -DoutputDirectory="%s" dependency:copy-dependencies' % (
            pom_file, maven_cache_dir), 
        shell=True)
    subprocess.call('cp -r "%s"/*  %s' % (maven_cache_dir, javalib_dir),
                    shell=True)

if __name__ == '__main__':
    main()
