import os
from setuptools import setup, find_packages

README = open(os.path.join(os.path.dirname(__file__), 'README.md')).read()

# allow setup.py to be run from any path
os.chdir(os.path.normpath(os.path.join(os.path.abspath(__file__), os.pardir)))

setup(
    name='altdg-api-mapper',
    version='1.0',
    packages=find_packages(),
    include_package_data=True,
    install_requires=['requests>=2.20.0'],
    license='MIT',
    description='Wrapper for ADG Mapping API ',
    long_description=README,
    long_description_content_type="text/markdown",
    url='https://github.com/altdg/bulk_mapper',
    keywords='adg alternative data group',
    classifiers=[
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.4',
        'Topic :: Internet :: WWW/HTTP',
    ],
)
