import subprocess
import os
import shutil


this_dir = os.path.dirname(os.path.abspath(__file__))
templates_dir = os.path.join(this_dir, 'templates')
assets_dir = os.path.join(this_dir, 'assets')
cache_dir = os.path.join(this_dir, 'cache')
build_dir = os.path.join(this_dir, "build")
dist_dir = os.path.join(this_dir, "dist")


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

    for dir_ in ['etc', 'python-src', 'python-lib']:
        os.makedirs(os.path.join(build_dir, dir_))

    # Move assets into dirs.

    # jython.jar
    shutil.copyfile(
        os.path.join(assets_dir, 'jython-full.jar'),
        os.path.join(build_dir, 'jython-full.jar')
    )

    # Pylibs.
    for py_asset in ['sasi_data', 'sasi_gridder', 'blinker', 'task_manager']:
        shutil.copytree(
            os.path.join(assets_dir, py_asset),
            os.path.join(build_dir, 'python-lib', py_asset)
        )
    shutil.copy(
        os.path.join(assets_dir, 'setuptools.egg'),
        os.path.join(build_dir, 'python-lib', 'setuptools.egg')
    )
    shutil.copy(
        os.path.join(templates_dir, 'setuptools.pth'),
        os.path.join(build_dir, 'python-lib', 'setuptools.pth')
    )

    # Jars.
    shutil.copytree(
        os.path.join(assets_dir, 'java-lib'),
        os.path.join(build_dir, 'java-lib')
    )
    subprocess.call(
        "cp -r %s/* %s" % (
            os.path.join(assets_dir, 'jenv-java-lib'),
            os.path.join(build_dir, 'java-lib'),
        ), shell=True)

    # Copy java src.
    shutil.copytree(
        os.path.join(templates_dir, 'java-src'),
        os.path.join(build_dir, 'java-src')
    )

    # Copy entry point.
    shutil.copyfile(
        os.path.join(templates_dir, 'entrypoint.py'),
        os.path.join(build_dir, 'python-src', 'entrypoint.py')
    )

    # Copy build script.
    shutil.copyfile(
        os.path.join(templates_dir, 'build.xml'),
        os.path.join(build_dir, 'build.xml')
    )

    # Run ant build.
    subprocess.call(
        "cd %s; ant" % build_dir,
        shell=True
    )

    # Move dist dir to this directory.
    shutil.move(
        os.path.join(build_dir, "dist"),
        dist_dir
    )


if __name__ == '__main__':
    main()
