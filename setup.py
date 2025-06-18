# -*- coding: utf-8 -*-

from distutils.core import setup
try:
    from mujincommon.setuptools import Distribution
except (ImportError, SyntaxError):
    from distutils.dist import Distribution

version = {}
exec(open('python/mujinasync/version.py').read(), version)

setup(
    distclass=Distribution,
    name='mujinasync',
    version=version['__version__'],
    packages=['mujinasync'],
    package_dir={'mujinasync': 'python/mujinasync'},
    package_data={'mujinasync': ['py.typed']},
    locale_dir='locale',
    license='Apache License, Version 2.0',
    long_description=open('README.md').read(),
    # flake8 compliance configuration
    enable_flake8=True,  # Enable checks
    fail_on_flake=True,  # Fail builds when checks fail
    install_requires=[],
)

