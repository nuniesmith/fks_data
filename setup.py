from setuptools import setup, find_packages

import os, glob
top_level_modules = [os.path.splitext(os.path.basename(p))[0] for p in glob.glob('src/*.py')]

setup(
    name='fks_data',
    version='0.0.0-dev',
    packages=find_packages(where='src') + [''],  # include package modules
    package_dir={'': 'src'},
    py_modules=top_level_modules,
    include_package_data=True,
    description='FKS data layer',
)
