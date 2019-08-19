import sys
# Remove current dir from sys.path, otherwise setuptools will peek up our
# module instead of system's.
sys.path.pop(0)
from setuptools import setup
sys.path.append("..")
import sdist_upip

setup(name='micropydatabase',
      version='0.1',
      description='Low-memory json-based databse for MicroPython.',
      long_description='Data is stored in a folder structure in json for easy inspection. RAM usage is optimized for embedded systems.',
      url='https://github.com/sungkhum/micropydatabase',
      author='Nathan Wells and James Hill',
      license='MIT',
      cmdclass={'sdist': sdist_upip.sdist},
      py_modules=['micropython'],
      install_requires=['micropython-os', 'micropython-os.path'])
