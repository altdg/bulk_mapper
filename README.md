# AltDG API Python Tools
Command-line tool with methods to consume the [AltDG API](https://developer.altdg.com) in bulk.

© [Alternative Data Group](https://www.altdg.com/). All rights reserved.

## Contents

- [AltDG API Python Tools](#AltDG-api-python-tools)
  - [Contents](#contents)
  - [Requirements](#requirements)
  - [Installation](#installation)
  - [Authorization](#authorization)
  - [Free tier key](#free-tier-key)
  - [Usage](#usage)
    - [Domain mapper](#domain-mapper)
    - [Merchant mapper](#merchant-mapper)
    - [Product mapper](#product-mapper)
    - [Command arguments (options)](#command-arguments-options)
  - [Development](#development)
    - [Usage as library](#usage-as-library)
  - [Support](#support)


## Requirements

Python 3.6+
> See also requirements.txt


## Installation

Run the following commands in your shell:

```sh
# install as usual python package
pip install altdg

# ... or install "altdg" package directly from repo
pip install git+https://github.com/altdg/bulk_mapper.git

# ... or if you want to get samples for testing, clone the repo
git clone https://github.com/altdg/bulk_mapper.git altdg
cd altdg
pip install -r requirements.txt
```

Now everything is ready to run the tool.

## Authorization

To use this tool you must have a valid app key to the [AltDG API](https://developer.altdg.com).
Methods are available depending on you account type with AltDG.

## Free tier key

Use this key to try the API for free: **f816b9125492069f7f2e3b1cc60659f0**

Sign up at https://developer.altdg.com/ to get a non-trial key.

## Usage

A preferred way to run the tool is to load it as module with the `python` command.

Run the tool with `--help` flag to display command's usage:

```sh
python -m altdg.api --help
```

### Domain mapper

Maps domain names from given text to structured company information.
> More details in https://developer.altdg.com/docs#domain-mapper

This will run all the domains in the provided text file (one per line expected):

```sh
python -m altdg.api -e domain-mapper sample-domains.txt -k "f816b9125492069f7f2e3b1cc60659f0"
```

Sign up at https://developer.altdg.com/ to get a non-trial key.

A CSV output file will be created automatically with the same path as the input file but prepending the current date.

[sample-domains.txt](sample-domains.txt) is a sample list of domains we included in our repo. This file is downloaded as part of this package, no need to re-create it.

### Merchant mapper

Maps strings from transactional purchase text (e.g. credit card transactions) to structured company information.
> More details in https://developer.altdg.com/docs#merchant-mapper

```sh
python -m altdg.api -e merchant-mapper sample-merchants.txt -k "f816b9125492069f7f2e3b1cc60659f0"
```
Sign up at https://developer.altdg.com/ to get a non-trial key.

A CSV output file will be created automatically with the same path as the input file but prepending the current date.

[sample-merchants.txt](sample-merchants.txt) is a sample list of domains we included in our repo. This file is downloaded as part of this package, no need to re-create it.

### Product mapper

Maps strings from product related text (e.g. inventory) to structured company information.
> More details in https://developer.altdg.com/docs#product-mapper

```sh
python -m altdg.api -e product-mapper sample-products.txt -k "f816b9125492069f7f2e3b1cc60659f0"
```
Sign up at https://developer.altdg.com/ to get a non-trial key.

A CSV output file will be created automatically with the same path as the input file but prepending the current date.


### Command arguments (options)

Arguments:

* `-e <endpoint>` `--endpoint` Type of mapper. Choices are "merchant-mapper", "domain-mapper" and "product-mapper".
* `-k <key>` `--key` AltDG API application key.
* `-o <filename>` `--out` Output file path. If not provided, the input file name is used with the ".csv" extension, prepended with the date and time.
* `-F` `--force` When providing a specific out_file, some results may already exist in that file for an input.
                 Use this option to force re-process results that are already in that output file, otherwise existing
                 results won't be processed again. Previous results are NOT overwritten, a new CSV row is added.
* `-n` `--num-threads` Number of requests to process in parallel. (See `--help` for max and default)
* `-r` `--num-retires` Number of retries per request. (See `--help` for max and default)
* `-t` `--timeout` API request timeout (in seconds). (See `--help` for max and default)
* `-th <hint>` `--type-hint` Improves the accuracy by providing the industry name or any keyword hint relevant to the inputs. E.g. `-th "medical"`


## Usage as library

You may use `AltdgAPI` class from your python program:

```python
from altdg.api import AltdgAPI

# initialize Mapper class with your key
mapper = AltdgAPI('domain-mapper', api_key='f816b9125492069f7f2e3b1cc60659f0')

# single query
print(mapper.query('abc.com'))

# single query with hint
print(mapper.query('abc.com', hint='news'))

# bulk query
print(mapper.bulk_query(['yahoo.com', 'amazon.com']))

# bulk query with same hint for all inputs
print(mapper.bulk_query(['yahoo.com', 'amazon.com'], hint='company'))

# bulk query with overwriting hint
print(mapper.bulk_query([
    ('purple mint', 'restaurant'),  # (input, hint) tuple
    'amazon',  # just input with base hint
], hint='company'))  # base hint
```

## Support

Please email info@altdg.com if you need to contact us directly.
