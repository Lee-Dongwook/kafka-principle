import re
import sys
from setuptools import find_packages, setup, Command

version = ''
with open('kafkatest/__init__.py', 'r') as fd:
    version = re.search(r'^__version__\s*=\s*[\'"]([^\'"]*)[\'"]', fd.read(), re.MULTILINE).group(1)

class PyTest(Command):
    user_options = [('pytest-args=', 'a', "Arguments to pass to py.test")]

    def initialize_options(self):
        self.pytest_args = []
    
    def finalize_options(self):
        self.test_args = []
        self.test_suite = True

    def run(self):
        import pytest
        print(self.pytest_args)
        errno = pytest.main(self.pytest_args)
        sys.exit(errno)

setup(name="kafkatest",
    version=version,
    description="Kafka System Tests",
    author="Lee-Dongwook, oriented by Apache Kafka",
    platforms=["any"],
    packages=find_packages(),
    include_package_data=True,
    install_requires=["ducktape==0.14.0", "requests>=2.32.4", "psutil==5.7.2", "pytest==9.0.3", "mock==5.1.0"],
    cmdclass={'test': PyTest},
    zip_safe=False
)
