# ADG API Python Tools
Command-line tool with methods to consume the [ADG API](https://developer.altdg.com) in bulk.

© [Alternative Data Group](https://www.altdg.com/). All rights reserved.

**Version 1.0.0-beta**


## Contents

* [Requirements](#requirements)
* [Installation](#installation)
* [Authorization](#authorization)
* [Usage](#usage)
    * [Domain mapper](#domain-mapper)
    * [Merchant mapper](#merchant-mapper)
* [Development](#development)
* [Support](#support)


## Requirements

Python 3.6+
> See also requirements.txt


## Installation

Run the following commands in your shell:

```sh
# clone the repo
git clone https://github.com/altdg/bulk_mapper adg

# create and activate a virtual environment
cd adg
python -m venv env
source env/bin/activate

# install requirements
pip install -r requirements.txt
```

Now everything is ready to run the tool.

## Authorization

To use this tool you must have a valid app key to the [ADG API](https://developer.altdg.com).
Methods are avalible depending on you account type with ADG.

## Free tier key

Use this key to try the API for free: **f816b9125492069f7f2e3b1cc60659f0**

## Usage

A preferred way to run the tool is to load it as module with the `python` command.

Run the tool with `--help` flag to dispay command's usage:

```sh
python -m adg_api_bulkMapper --help
```

### Domain mapper

Maps domain names from given text to structured company information.
> More details in https://developer.altdg.com/docs#domain-mapper

This will run all the domains in the provided text file (one per line expected):

```sh
python -m adg_api_bulkMapper -e domains sample-domains.txt -k "f816b9125492069f7f2e3b1cc60659f0"
```

`12345` is your ADG API application key. Sign up in https://developer.altdg.com/ to get one!

A CSV output file will be created automatically with the same path as the input file but prepending the current date.

[sample-domains.txt](sample-domains.txt) is a sample list of domains we included in our repo. This file is downloaded as part of this package, no need to re-create it. 

### Merchant mapper

Maps strings from transactional purchase text (e.g. credit card transactions) to structured company information.
> More details in https://developer.altdg.com/docs#merchant-mapper

```sh
python -m adg_api_bulkMapper -e merchants sample-merchants.txt -k "f816b9125492069f7f2e3b1cc60659f0"
```
`12345` is your ADG API application key. Sign up in https://developer.altdg.com/ to get one!

A CSV output file will be created automatically with the same path as the input file but prepending the current date.

[sample-merchants.txt](sample-merchants.txt) is a sample list of domains we included in our repo. This file is downloaded as part of this package, no need to re-create it. 

### Product mapper

Maps strings from product related text (e.g. inventory) to structured company information.
> More details in https://developer.altdg.com/docs#product-mapper

```sh
python -m adg_api_bulkMapper -e products sample-products.txt -k "f816b9125492069f7f2e3b1cc60659f0"
```
`12345` is your ADG API application key. Sign up in https://developer.altdg.com/ to get one!

A CSV output file will be created automatically with the same path as the input file but prepending the current date.


### Command arguments (options)

Optional arguments:

* `-e` `--endpoint` Type of mapper. Choices are merchants, domains and products.
* `-k` `--key` ADG API application key.
* `-o` `--out` Output file path. If not provided, the input file name is used with the ".csv" extension, prepended with the date and time.
* `-F` `--force` When providing a specific out_file, some results may already exist in that file for an input.
                 Use this option to force re-process results that are already in that output file, otherwise existing
                 results won't be processed again. Previous results are NOT overwritten, a new CSV row is added.
* `-n` `--input_no` Number of requests to process in parallel. (See `--help` for max and default)
* `-r` `--retires` Number of retries per request. (See `--help` for max and default)
* `-t` `--timeout` API request timeout (in seconds). (See `--help` for max and default)
* `-c` `--companies_only` Improves the accuracy if the input file contains only company names, no other miscellaneous text. Applicable only when the endpoint mapper is set to merchants.


## Development

Having [pip](https://pip.pypa.io/en/stable/installing/) for your python3 environment, clone this repo, cd into its directory, and run:

```sh
pip install -r requirements.txt
```

### Usage as library

You may use `Mapper` class from your python program:

```python
# first, import adg_api_bulkMapper module
import adg_api_bulkMapper

# initialize Mapper class with your key
domain_mapper = adg_api_bulkMapper.Mapper(endpoint='domains', api_key='f816b9125492069f7f2e3b1cc60659f0')

# query API
results = domain_mapper.query_api(['abc.com', 'yahoo.com', 'amazon.com'])
```


## Support

Please email info@altdg.com if you need to contact us directly.
