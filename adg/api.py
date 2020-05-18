#!/usr/bin/env python
"""
2018 Alternative Data Group. All Rights Reserved.
Module for running input file through ADG's mapper API and writing results to CSV file.
Requires python >= 3.6 (with pip)
Install dependencies with 'pip install requirements.txt'.
Usage (domain mapper):
    python -m adg domains -k "12345" path/to/domains.txt
Usage (merchant mapper):
    python -m adg merchants -k "12345" path/to/merchants.txt
"""
#
# you can see the char issue for example in above_farms_modelsnum-2020-05-15.csv -- look at  ï»¿MANUFACTURER
#
#
#
#
#
# 01:34
# the input file doesnt have any special chars
# 01:34
# it might have to do with how excel saves csvs

import os
import sys
import logging
import json
import csv
from concurrent.futures.thread import ThreadPoolExecutor
from math import ceil
from warnings import warn

import requests
import datetime
import time
import argparse
from argparse import ArgumentParser, RawDescriptionHelpFormatter

from typing import List, Optional, Iterator, Iterable

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Yield successive n-sized chunks from input file.
def chunks(l, n):
    for i in range(0, len(l), n):
        yield l[i:i + n]


class ApiAuthError(Exception):
    pass


class ApiInternalError(Exception):
    pass


class AdgApi:
    # ---- access settings ----
    API_URL = 'https://api-2445582026130.production.gw.apicast.io/'
    DEMO_KEY = 'f816b9125492069f7f2e3b1cc60659f0'
    SUPPORT_EMAIL = 'info@altdg.com'
    HEADERS = {
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        'User-Agent': 'https://github.com/altdg/bulk_mapper',
    }

    MAX_INPUT_LENGTH = 127

    # ---- parallel settings ----
    DEFAULT_NUM_THREADS = 4
    MAX_NUM_THREADS = 8

    # ---- failure handling ----
    RESPONSE_TIMEOUT = 30  # max time to wait for each query to complete
    DEFAULT_NUM_RETRIES = 2
    MAX_NUM_RETRIES = 10


    TIMEOUT_PER_ITEM = 30
    N_TIMEOUT_RETRIES = 10
    MAX_TIMEOUT = 35
    N_sec_sleep_key_limit = 15
    error_count = 0
    ENCODING = 'utf-8'

    def __init__(
        self,
        endpoint: str,
        api_key: str = DEMO_KEY,
        num_threads: int = DEFAULT_NUM_THREADS,
        num_retries: int = DEFAULT_NUM_RETRIES,
    ):
        """
        Initialize ADG API mapper with some inter-requests settings.

        Args:
            endpoint: mapper's endpoint ("merchant-mapper", "domain-mapper", etc) - see docs
            api_key: your API key
            num_threads: how many threads to use
            num_retries: how many retries to perform if case of failure
        """
        if endpoint in ['merchants', 'domains', 'products']:
            raise ValueError(f'Outdated endpoint value "{endpoint}", use one from ADG API docs '
                             f'("merchant-mapper", "domain-mapper" etc')

        self.endpoint = endpoint
        self.api_key = api_key

        self.num_threads = min(num_threads, self.MAX_NUM_THREADS)
        if num_threads > self.MAX_NUM_THREADS:
            logger.warning('Number of requests to process in parallel was set to its max of {self.num_threads}.')

        self.num_retries = min(num_retries, self.MAX_NUM_RETRIES)
        if num_retries > self.MAX_NUM_RETRIES:
            logger.warning(f'Number of retries was set to its max of {self.num_retries}.')

    def prepare_input(self, inp: str) -> str:
        """
        Truncates string to be no more than MAX_INPUT_LENGTH. Warns if truncation happened.
        """
        if not isinstance(inp, str):
            raise ValueError('Input is not string: {}'.format(inp))

        if len(inp) > self.MAX_INPUT_LENGTH:
            inp = inp[:self.MAX_INPUT_LENGTH]
            logger.warning(f'Truncated input to {self.MAX_INPUT_LENGTH} chars: "{inp}"')

        return inp

    def query(self, value: str, hint: Optional[str] = None) -> dict:
        """
        Make a single request to ADG API.

        Args:
            value: text string to map ("amzn", "PURCHASE DEBIT CARD XXXX-2211 ETSY.COM", ...)
            hint: any string which may help identifying input type ("company", "agriculture", "brand" etc)

        Returns:
            dict {
                'Original Input': ...,
                'Company Name': company name retrieved from ADG API,
                ... <additional fields, refer to ADG API docs> ...
            }
        """
        if not value:
            raise ValueError(f'Empty input: {value}')

        payload = json.dumps([self.prepare_input(value)])

        headers = self.HEADERS
        if hint:
            headers['X-Type-Hint'] = hint

        for n_attempt in range(1, self.num_retries+1):
            if n_attempt > 1:
                logger.warning(f'Retrying (attempt #{n_attempt}): {value}')

            try:
                response = requests.post(
                    f'{self.API_URL}/{self.endpoint}?X_User_Key={self.api_key}',
                    data=payload,
                    headers=headers,
                    timeout=self.RESPONSE_TIMEOUT,
                )
                response.raise_for_status()
                return response.json()[0]
            except Exception as exc:
                logger.error(f'API request error: {exc}. Please contact {self.SUPPORT_EMAIL} for help '
                             'if this problem persists.')

                if response.status_code == 401:
                    logger.warning(f'Authentication error. Check your API key "{self.api_key}"'
                                     f' or use demo key "{self.DEMO_KEY}"')
                    raise

                continue

        logger.error(f'Could not process "{value}"')
        raise

    def bulk_query(self, values: Iterable[str], hint: Optional[str] = None) -> Iterator[dict]:
        """
        Processes `values` in bulk using multithreading.

        Args:
            values: collection of strings to map
            hint: any string which may help identifying input type ("company", "agriculture", "brand" etc)

        Yields:
            dict of mapped info (see `query` method)

        """
        num_threads = self.num_threads

        # process all values by chunks, decrease num of threads if any errors
        for chunk in map(list, chunks(values, self.num_threads)):
            while True:
                try:
                    for result in ThreadPoolExecutor(max_workers=num_threads).map(
                            lambda value: self.query(value, hint=hint), chunk):
                        yield result

                    if num_threads < self.num_threads:
                        num_threads += 1
                        logger.debug(f'Increasing number of threads to {num_threads}')

                    break

                except Exception:
                    num_threads = ceil(num_threads / 2)
                    logger.debug(f'Decreasing number of threads to {num_threads}')
    #
    # # Load raw inputs from input file.
    # def _load_inputs(self):
    #     with open(self.in_file_location, 'r', encoding=self.ENCODING, errors='ignore') as in_file:
    #         raw_inputs = in_file.read().splitlines()
    #
    #     logger.info("Reading from {}. Writing to {}.".format(self.in_file_location, self.out_file_location))
    #     logger.info("Now running. {} rows to process.".format(len(raw_inputs)))
    #
    #     return raw_inputs
    #
    # # Load already processed inputs from CSV output file.
    # def _load_processed_inputs(self):
    #     result = []
    #
    #     if not os.path.isfile(self.out_file_location):
    #         return result
    #
    #     with open(self.out_file_location, 'r', encoding=self.ENCODING) as csv_out_file:
    #         csv_reader = csv.reader(csv_out_file, delimiter=',')
    #
    #         for i, row in enumerate(csv_reader):
    #             if i == 0:
    #                 # Skip the first line which is the header.
    #                 continue
    #
    #             if row:
    #                 result.append(row[0])
    #
    #     return result
    #
    #
    # def process_csv(self, ):
    #
    #     raw_inputs = self._load_inputs()
    #
    #     # Check previous CSV output file.
    #     # Only request for lines are not processed before.
    #     processed_inputs = self._load_processed_inputs()
    #     num_processed_inputs = len(processed_inputs)
    #
    #     if not self.force_reprocess:
    #         raw_inputs = raw_inputs[num_processed_inputs:]
    #         if (len(processed_inputs)) and raw_inputs != []:
    #             logger.info('{} already exists in your destination with {} rows. Continue processing from row {} of '
    #                         'input file.'.format(
    #                 self.out_file_location, len(processed_inputs), len(processed_inputs) + 1))
    #         if (len(processed_inputs)) and raw_inputs == []:
    #             logger.info('{} already exists in your destination with {} rows. All rows from input file have been '
    #                         'processed.'.format(self.out_file_location, len(processed_inputs)))
    #     if self.force_reprocess and len(processed_inputs):
    #         logger.info(
    #             'Appending results to {} from row {}.'.format(self.out_file_location, len(processed_inputs) + 1))
    #
    #     for one_json_response in list_json_response:
    #         self.write_csv(one_json_response)
    #
    #     chunk_counter += 1
    #     proccessed_row_counter = chunk_counter * self.inputs_per_request
    #     if len(raw_inputs) - proccessed_row_counter <= 0:
    #         logger.info('Wrote {} rows to {}. Finished.'.format(
    #             len(raw_inputs), self.out_file_location))
    #         logger.debug('{} rows succeed. {} rows failed.'.format(
    #             len(raw_inputs) - self.error_count, self.error_count))
    #     else:
    #         logger.info('Processed {} rows in total. {} rows are left.'.format(
    #             proccessed_row_counter, len(raw_inputs) - proccessed_row_counter))
    #
    #
    # # Write one API response to output CSV file.
    # def write_csv(self, one_json_response):
    #     self.in_file_location = os.path.expanduser(in_file)
    #     self.out_file_location = os.path.expanduser(out_file)
    #
    #     # Check if output file already exist.
    #     # Always write API response to the same CSV.
    #     if os.path.dirname(self.out_file_location):
    #         os.makedirs(os.path.dirname(self.out_file_location), exist_ok=True)
    #     file_exists = os.path.isfile(self.out_file_location)
    #     field_names = [
    #         'Original Input',
    #         'Date & Time',
    #         'Company Name',
    #         'Alias 1',
    #         'Alias 2',
    #         'Alias 3',
    #         'All Aliases',
    #         'Confidence Level',
    #         'Confidence',
    #         'Ticker',
    #         'Exchange',
    #         'Majority Owner',
    #         'FIGI',
    #         'Related Entity 1 Name',
    #         'Related Entity 2 Name',
    #         'Related Entity 3 Name',
    #         'All Related Entities',
    #         'Alternative Company Matches',
    #         'Websites'
    #     ]
    #
    #     date_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%I:%S')
    #     csv_output = open(self.out_file_location, 'a', newline='', encoding=self.ENCODING)
    #     writer = csv.DictWriter(
    #         csv_output,
    #         fieldnames=field_names,
    #         extrasaction='ignore',
    #         delimiter=',',
    #         quotechar='"',
    #         quoting=csv.QUOTE_MINIMAL
    #     )
    #
    #     if (not file_exists) or len(self._load_processed_inputs()) == 0:
    #         writer.writeheader()
    #         csv_output.flush()
    #         logger.debug('Wrote headers to {}', format(self.out_file_location))
    #
    #     # Transform API response from Json to a dictionary for writing to CSV purpose.
    #     for result in one_json_response:
    #         aliases = result.get('Aliases', [])
    #         related = result.get('Related Entities', [])
    #         # alternatives = result.get('Alternative Company Matches', [])
    #
    #         non_changing_keys = ['Original Input', 'Ticker', 'Exchange',
    #                              'Company Name', 'Confidence Level', 'Confidence', 'FIGI']
    #
    #         csv_row = {
    #             **{key: value for key, value in result.items() if key in non_changing_keys},
    #             'Date & Time': date_time,
    #             'Alias 1': aliases[0] if aliases else None,
    #             'Alias 2': aliases[1] if len(aliases) > 1 else None,
    #             'Alias 3': aliases[2] if len(aliases) > 2 else None,
    #             'All Aliases': '; '.join(aliases),
    #             'Related Entity 1 Name': related[0] if related else None,
    #             'Related Entity 2 Name': related[1] if len(related) > 1 else None,
    #             'Related Entity 3 Name': related[2] if len(related) > 2 else None,
    #             'All Related Entities': '; '.join(related),
    #             'Majority Owner': result.get('Majority Owner'),
    #             'Alternative Company Matches': result.get('Alternative Company Matches'),
    #             'Websites': result.get('Websites'),
    #         }
    #
    #         writer.writerow(csv_row)
    #         csv_output.flush()
    #
    #         logger.debug('{}: {}'.format(result["Original Input"], result["Company Name"]))
    #         if "error" in result["Company Name"]:
    #             self.error_count += 1
    #     logger.debug('Wrote results to {}'.format(self.out_file_location))


def positive_integer(value: str) -> int:
    try:
        value = int(value)
    except ValueError:
        raise argparse.ArgumentTypeError(f'Not a number: {value}')

    if value <= 0:
        raise argparse.ArgumentTypeError(f'Not a positive number: {value}')

    return value


if __name__ == '__main__':
    start_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%I:%S')
    start = time.time()
    now_str = datetime.datetime.now().strftime('%Y-%m-%d')

    # Parse command-line input arguments.
    parser = ArgumentParser(
        description=f"""
            Examples:
            python -m adg.api -e domain-mapper sample-domains.txt -k "{AdgApi.DEMO_KEY}"
            python -m adg.api -e merchant-mapper sample-merchants.txt -k "{AdgApi.DEMO_KEY}"
        """,
        formatter_class=RawDescriptionHelpFormatter
    )
    parser.add_argument(
        '-e', '--endpoint',
        required=True,
        help='Type of mapper',
        default='merchant-mapper',
        dest='endpoint',
    )
    parser.add_argument(
        '-k', '--key',
        required=True,
        help='ADG API application key',
        default=AdgApi.DEMO_KEY,
        dest='api_key',
    )
    parser.add_argument(
        '-o', '--out',
        help='Path to output file',
        dest='out_file')
    parser.add_argument(
        '-F', '--force',
        help='Re-process results that already exist in the output file. (Adds new CSV rows.)',
        default=False,
        action='store_const', const=True,
        dest='force_reprocess',
    )
    parser.add_argument(
        '-n', '--num-threads',
        help=f'Number of threads. Max: {AdgApi.MAX_NUM_THREADS}',
        default=AdgApi.DEFAULT_NUM_THREADS,
        type=positive_integer,
        dest='num_threads',
    )
    parser.add_argument(
        '-r', '--num-retries',
        help=f'Number of retries per request. Max: {AdgApi.MAX_NUM_RETRIES}',
        default=AdgApi.DEFAULT_NUM_RETRIES,
        type=positive_integer,
    )
    parser.add_argument(
        '-t', '--timeout',
        help='API request timeout (in seconds)',
        default=AdgApi.RESPONSE_TIMEOUT,
        type=positive_integer,
    )
    parser.add_argument(
        '-th', '--type-hint',
        help='Any hint about input data ("company", "brand", ...)',
        default=None,
        dest='hint',)
    parser.add_argument(
        help='Path to input file',
        dest='in_file',
    )
    args = parser.parse_args()

    if not os.path.isfile(args.in_file):
        logger.error(f'Input file does not exist: {args.in_file}')
        sys.exit()

    # Take input file name plus date as output file name.
    if not args.out_file:
        in_file_dir, in_file_name = os.path.split(args.in_file)
        args.out_file = os.path.join(
            in_file_dir,
            f'{os.path.splitext(in_file_name)[0]}-{now_str}.csv'
        )

    AdgApi(**vars(args)).bulk_query()
    end_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%I:%S')
    end = time.time()

    print(f'Start time: {start_time}')
    print(f'End time: {end_time}')
    print(f'Elapsed (in sec): {end-start:.2f}')
