#!/usr/bin/env python
"""
© 2018 Alternative Data Group. All Rights Reserved.

Module for running inputs file (one input per line) through ADG's mapper API and writing results to CSV file.

Requires python >= 3.6 (with pip)
Install dependencies with `pip install requirements.txt`.

Usage (domain mapper):
    python -m adg domains -k "12345" path/to/domains.txt

Usage (merchant mapper):
    python -m adg merchants -k "12345" path/to/domains.txt

TODO: Handle all errors gracefully and provide informative messages (as specific as possible)
      - [x] including rows it can't process from input: write error message out to corresponding CSV row
      - [ ] and output info@altdg.com email for support
TODO: out_file CSV should preserve order of inputs in in_file TXT
TODO: out_file CSV should contain the cleaned domain names. Check those to decide re-processing too.
"""
import json
import csv
import datetime
import logging
import os
import requests
from argparse import ArgumentParser, RawDescriptionHelpFormatter
from math import ceil
from typing import List
from math import sqrt
from joblib import Parallel, delayed

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(f'adg.{__name__}')

MAX_INPUTS_PER_QUERY = 1
N_REQUEST_RETRIES = 2
TIMEOUT_PER_ITEM = 60


def _chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]


class Mapper:
    API_URL = 'https://api-2445582026130.production.gw.apicast.io/'
    ENCODING = 'utf-8'

    def __init__(self, endpoint: str, api_key: str, in_file: str = '', out_file: str = '', force_reprocess: bool = False,
                 inputs_per_request: int = MAX_INPUTS_PER_QUERY, retries: int = N_REQUEST_RETRIES,
                 timeout: int = TIMEOUT_PER_ITEM):
        """
        TODO: Document.
        :param endpoint: Which API method to use, either "merchant" or "domain".
        :param in_file: Location of the inputs
        :param out_file: Location of the output
        :param api_key: App Key for the ADG API
        :param force_reprocess: Whether to resume or reprocess results
        :param inputs_per_request: inputs per request
        :param retries: inputs per Retries per input group
        :param timeout: Timeout (in sec) per input
        """
        assert endpoint in ['merchants', 'domains']

        self.endpoint = endpoint[:-1]
        self.in_file_location = os.path.expanduser(in_file)
        self.out_file_location = os.path.expanduser(out_file)
        self.api_key = api_key
        self.force_reprocess = force_reprocess
        self.inputs_per_request = min(inputs_per_request, MAX_INPUTS_PER_QUERY)
        if MAX_INPUTS_PER_QUERY < inputs_per_request:
            logger.info(f'The of inputs to process per API request was set to its max of {MAX_INPUTS_PER_QUERY}.')
        self.retries = retries
        self.timeout = timeout

        self._previously_tested_input = []  # used to store the previously tested input data, and allow users to resume
        self._in_file_contents = []  # list of the search terms to test against

    def _load_inputs(self) -> List[str]:
        """
        Loads raw inputs from input file.

        Returns: list of raw inputs
        """
        with open(self.in_file_location, 'r', encoding=self.ENCODING) as in_file:
            result = in_file.readlines()

        # remove dupes and empty strings; strip inputs
        result = set([line.strip() for line in result]) - {''}
        result = list(result)

        logger.info(f"Number of input strings in ingested file: {len(result)}")

        return result

    def _load_processed_inputs(self) -> List[str]:
        """
        Loads already processed inputs from csv output file.

        Returns: list of already processed results.
        """
        result = []

        if not os.path.isfile(self.out_file_location):
            return result

        with open(self.out_file_location, 'r', encoding=self.ENCODING) as csv_out_file:
            csv_reader = csv.reader(csv_out_file, delimiter=',')

            for i, row in enumerate(csv_reader):
                if i == 0:
                    # Skip this line cause it's the header
                    continue

                if row:
                    result.append(row[0])

        return result

    def query_api(self, inputs: List[str]) -> List[dict]:
        """
        Queries ADG mapper API and returns json output.

        Args:
            inputs: list of raw input strings to process

        Returns:
            List of dicts with data for each input,
            or empty list if request failed
        """
        payload = json.dumps(inputs)
        print(payload)
        headers = {
            'Accept': "application/json",
            'Content-Type': "application/json",
            'User-Agent': 'https://github.com/geneman/altdg',
        }

        timeout = len(inputs) * self.timeout
        logger.debug(f'Timeout for these inputs: {timeout}')

        for n_attempt in range(self.retries):
            if n_attempt > 0:
                logger.debug(f'Retrying previous request. Attempt # {n_attempt+1}')

            try:
                response = requests.post(f'{self.API_URL}/{self.endpoint}-mapper?X_User_Key={self.api_key}', data=payload, headers=headers, timeout=timeout)

            except Exception as ex:
                error = f'API request error: {ex}. ' \
                    f'Please contact info@altdg.com for help if this problem persists.'
                logger.debug(error)
                continue

            if not response.ok:
                error = f'API response error: {response.status_code} {response.reason} for inputs {inputs}. ' \
                    f'Please contact info@altdg.com for help if this problem persists.'
                logger.debug(error)
                continue

            return response.json()

        logger.error(error)
        return [{'Original Input': rawin, 'Company Name': error} for rawin in inputs]

    def bulk(self):
        """
        TODO: Document.
        :return:
        """
        raw_inputs = self._load_inputs()

        if not self.force_reprocess:
            # Check and load previous CSV Output
            processed_inputs = self._load_processed_inputs()
            if len(processed_inputs):
                logger.info(f'Loaded {len(processed_inputs)} results a previous run.')

            if processed_inputs:
                raw_inputs = [rawin for rawin in raw_inputs if rawin not in processed_inputs]

        # File handling
        date_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%I:%S')
        if os.path.dirname(self.out_file_location):
            os.makedirs(os.path.dirname(self.out_file_location), exist_ok=True)
        file_exists = os.path.isfile(self.out_file_location)
        field_names = [
            'Original Input',
            'Date & Time',
            'Company Name',
            'Alias 1',
            'Alias 2',
            'Alias 3',
            'Confidence Level',
            'Confidence',
            'Ticker',
            'Exchange',
            'Majority Owner',
            'FIGI',
            'Related Entity 1 Name',
            'Related Entity 1 Score',
            'Related Entity 2 Name',
            'Related Entity 2 Score',
            'Related Entity 3 Name',
            'Related Entity 3 Score',
            'Alt Company 1',
            'Alt Company 2',
        ]

        csv_output = open(self.out_file_location, 'a', newline='', encoding=self.ENCODING)
        writer = csv.DictWriter(
            csv_output,
            fieldnames=field_names,
            extrasaction='ignore',
            delimiter=',',
            quotechar='"',
            quoting=csv.QUOTE_MINIMAL
        )

        if not file_exists:
            writer.writeheader()
            csv_output.flush()
            logger.debug(f'Wrote headers to {self.out_file_location}')

        n_chunks = ceil(len(raw_inputs) / self.inputs_per_request)
        list_json_response = Parallel(n_jobs=-1)(delayed(self.query_api)([i]) for i in raw_inputs)
        for one_json_response in list_json_response:

            for result in one_json_response:
                aliases = result.get('Aliases', [])
                related = result.get('Related Entities', [])
                alternatives = result.get('Alternative Company Matches', [])

                non_changing_keys = ['Original Input', 'Ticker', 'Exchange',
                                     'Company Name', 'Confidence Level', 'Confidence', 'FIGI']
                csv_row = {
                    **{key: value for key, value in result.items() if key in non_changing_keys},
                    'Date & Time': date_time,
                    'Alias 1': aliases[0] if aliases else None,
                    'Alias 2': aliases[1] if len(aliases) > 1 else None,
                    'Alias 3': aliases[2] if len(aliases) > 2 else None,
                    'Related Entity 1 Name': related[0]['Name'] if related else None,
                    'Related Entity 1 Score': related[0]['Closeness Score'] if related else None,
                    'Related Entity 2 Name': related[1]['Name'] if len(related) > 1 else None,
                    'Related Entity 2 Score': related[1]['Closeness Score'] if len(related) > 1 else None,
                    'Related Entity 3 Name': related[2]['Name'] if len(related) > 2 else None,
                    'Related Entity 3 Score': related[2]['Closeness Score'] if len(related) > 2 else None,
                    'Majority Owner': result.get('Majority Owner'),
                    'Alt Company': alternatives[0] if alternatives else None,
                }

                writer.writerow(csv_row)
                csv_output.flush()

                logger.debug(f'Wrote results for {result["Original Input"]}')

        logger.info(f'Wrote results to {self.out_file_location}')
        logger.info('Process complete')


if __name__ == '__main__':
    now_str = datetime.datetime.now().strftime('%Y-%m-%d-%H%M%S')

    parser = ArgumentParser(
        description="""
            Examples:
            python -m adg_api_bulkMapper -e domains sample-domains.txt -k "12345"
            python -m adg_api_bulkMapper -e merchants sample-merchants.txt -k "12345"
        """,
        formatter_class=RawDescriptionHelpFormatter
    )
    parser.add_argument('-e', '--endpoint', required=True, help='Type of input (and API request).', choices=['merchants', 'domains'], dest='endpoint')
    parser.add_argument('-k', '--key', required=True, dest='api_key', help='ADG API application key')
    parser.add_argument('-o', '--out', help='Path to output file', dest='out_file')
    parser.add_argument('-F', '--force', action='store_const', const=True, default=False, dest='force_reprocess',
                        help='Re-process results that already exist in the output file. (Adds new CSV rows.)')
    parser.add_argument('-n', '--input_no', type=int, default=1, dest='inputs_per_request',
                        help=f'Number of inputs to process per API request. Default: {1} ')
    parser.add_argument('-r', '--retries', type=int, default=N_REQUEST_RETRIES, dest='retries',
                        help=f'Number of retries per domain group. Default: {N_REQUEST_RETRIES}')
    parser.add_argument('-t', '--timeout', type=int, default=TIMEOUT_PER_ITEM, dest='timeout',
                        help=f'API request timeout (in seconds) allowed per domain. Default: {TIMEOUT_PER_ITEM}')
    parser.add_argument(help='Path to input file', dest='in_file')
    args = parser.parse_args()

    if not args.out_file:
        in_file_dir, in_file_name = os.path.split(args.in_file)
        args.out_file = os.path.join(
            in_file_dir,
            f'{os.path.splitext(in_file_name)[0]}-{now_str}.csv'
        )

    Mapper(**vars(args)).bulk()
