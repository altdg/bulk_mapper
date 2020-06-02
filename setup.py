import os
from setuptools import setup, find_packages

README = open(os.path.join(os.path.dirname(__file__), 'README.md')).read()

# allow setup.py to be run from any path
os.chdir(os.path.normpath(os.path.join(os.path.abspath(__file__), os.pardir)))

setup(
    name='altdg',
    description='Utility libraries from Alternative Data Group',
    url='https://github.com/altdg/bulk_mapper',
    version='1.0.1',
    packages=find_packages(),
    include_package_data=True,
    install_requires=['requests', 'chardet'],
    python_requires='>=3.6',
    license='MIT',
    long_description=README,
    long_description_content_type="text/markdown",

    keywords='adg alternative data group',
    classifiers=[
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.6',
        'Topic :: Internet :: WWW/HTTP',
    ],
)
