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
import logging
import os
import requests
from argparse import ArgumentParser, RawDescriptionHelpFormatter
from typing import List
from joblib import Parallel, delayed
import datetime
import time
import argparse


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(f'adg.{__name__}')

MAX_INPUTS_PER_QUERY = 4
N_REQUEST_RETRIES = 2
TIMEOUT_PER_ITEM = 60


def _chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]


class Mapper:
    # API_URL = 'https://api-2445582026130.production.gw.apicast.io/'
    API_URL = 'http://adg-api-dev.us-east-1.elasticbeanstalk.com/'
    ENCODING = 'utf-8'

    count_request = 0
    N_TIMEOUT_RETRIES = 10
    N_const_thread = 1

    def __init__(self, endpoint: str, api_key: str, in_file: str = '', out_file: str = '', force_reprocess:
                 bool = False, num_requests_parallel: int = MAX_INPUTS_PER_QUERY, retries: int = N_REQUEST_RETRIES,
                 timeout: int = TIMEOUT_PER_ITEM):
        """
        TODO: Document.
        :param endpoint: Which API method to use, either "merchant" or "domain".
        :param in_file: Location of the inputs
        :param out_file: Location of the output
        :param api_key: App Key for the ADG API
        :param force_reprocess: Whether to resume or reprocess results
        :param num_requests_parallel: inputs per request
        :param retries: inputs per Retries per input group
        :param timeout: Timeout (in sec) per input
        """
        assert endpoint in ['merchants', 'domains']

        self.endpoint = endpoint[:-1]
        self.in_file_location = os.path.expanduser(in_file)
        self.out_file_location = os.path.expanduser(out_file)
        self.api_key = api_key
        self.force_reprocess = force_reprocess
        self.inputs_per_request = min(num_requests_parallel, MAX_INPUTS_PER_QUERY)
        if MAX_INPUTS_PER_QUERY < num_requests_parallel:
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
            raw_inputs = in_file.readlines()

        logger.info(f"Number of input strings in ingested file: {len(raw_inputs)}")

        return raw_inputs

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
        print(datetime.datetime.now())
        headers = {
            'Accept': "application/json",
            'Content-Type': "application/json",
            'User-Agent': 'https://github.com/geneman/altdg',
            'X-Configurations': '{"CACHE_ERS": False}',
            'X-3scale-proxy-secret-token':'rkMcYebOqEIRmpFFm0nGIBex7m8pRVtR'
        }

        timeout = self.timeout
        logger.debug(f'Timeout for these inputs: {timeout}')

        for n_attempt in range(self.retries):
            if n_attempt > 0:
                logger.debug(f'Retrying previous request. Attempt # {n_attempt+1}')

            try:
                # esponse = requests.post(f'{self.API_URL}/{self.endpoint}-mapper-debug?X_User_Key={self.api_key}',
                #                         data=payload, headers=headers, timeout=timeout)
                response = requests.post(f'{self.API_URL}/{self.endpoint}-mapper', data=payload, headers=headers, timeout=timeout)
                self.count_request += 1
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

    def has_timeout(self,company_name):
        timeout_messages = ["API response error: 504 Gateway Time-out",
                            "Read timed out",
                            "API response error: 503 Service Unavailable: Back-end server is at capacity"]
        return any(msg in company_name for msg in timeout_messages)


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
            num_processed_inputs = len(processed_inputs)
            raw_inputs = raw_inputs[num_processed_inputs:]

        num_threads = self.inputs_per_request
        chunks_without_timeout_streak = 0
        for i, chunk in enumerate(_chunks(raw_inputs, self.inputs_per_request)):
            num_reduction = (num_threads - 4) // 2 + 3
            num_retries_one_thread = 10
            max_timeout_retry = max(num_reduction + num_retries_one_thread, self.N_TIMEOUT_RETRIES)

            logger.info(f"Number of threads in the chunk: {num_threads}")


            list_json_response = []
            had_timeout = False

            for _ in range(max_timeout_retry + 1):

                start = time.time()
                list_json_response = Parallel(n_jobs=num_threads, prefer="threads")(delayed(self.query_api)([one_row])
                                                                                    for one_row in chunk)
                end = time.time()
                print(end - start)

                if any(self.has_timeout(response[0]['Company Name']) for response in list_json_response):
                    had_timeout = True
                    if num_threads > 4:
                        num_threads -= 2
                    elif num_threads > 1:
                        num_threads -= 1
                    logger.info(f"Number of threads in the chunk after error: {num_threads}")

                if not any(self.has_timeout(response[0]['Company Name']) for response in list_json_response):
                    break

            if had_timeout:
                chunks_without_timeout_streak = 0
            else:
                chunks_without_timeout_streak += 1

            if chunks_without_timeout_streak >= 3:
                num_threads += 1
                num_threads = min(num_threads, self.inputs_per_request)

            for one_json_response in list_json_response:
                self.write_csv(one_json_response)

    def write_csv(self, one_json_response):

        # File handling

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

        date_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%I:%S')
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

def is_pos_int(string):
    try:
        value = int(string)
        if value <= 0:
            msg = "%r is not a postive integer" % string
            raise argparse.ArgumentTypeError(msg)
    except ValueError:
        msg = "%r is not a postive integer" % string
        raise argparse.ArgumentTypeError(msg)
    return value

if __name__ == '__main__':
    now_str = datetime.datetime.now().strftime('%Y-%m-%d')

    parser = ArgumentParser(
        description="""
            Examples:
            python -m adg_api_bulkMapper -e domains sample-domains.txt -k "12345"
            python -m adg_api_bulkMapper -e merchants sample-merchants.txt -k "12345"
        """,
        formatter_class=RawDescriptionHelpFormatter
    )
    parser.add_argument('-e', '--endpoint', required=True, help='Type of input (and API request).',
                        choices=['merchants', 'domains'], dest='endpoint')
    parser.add_argument('-k', '--key', required=True, dest='api_key', help='ADG API application key')
    parser.add_argument('-o', '--out', help='Path to output file', dest='out_file')
    parser.add_argument('-F', '--force', action='store_const', const=True, default=False, dest='force_reprocess',
                        help='Re-process results that already exist in the output file. (Adds new CSV rows.)')
    parser.add_argument('-n', '--input_no', type=is_pos_int, default=4, dest='num_requests_parallel',
                        help=f'Number of requests to process in parallel. Default: {4} ')
    parser.add_argument('-r', '--retries', type=is_pos_int, default=N_REQUEST_RETRIES, dest='retries',
                        help=f'Number of retries per domain group. Default: {N_REQUEST_RETRIES}')
    parser.add_argument('-t', '--timeout', type=is_pos_int, default=35, dest='timeout',
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