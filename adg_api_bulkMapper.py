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

import os
import sys
import logging
import json
import csv
import requests
import datetime
import time
import argparse
from argparse import ArgumentParser, RawDescriptionHelpFormatter
from joblib import Parallel, delayed

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('adg.{}'.format(__name__))


# Yield successive n-sized chunks from input file.
def _chunks(l, n):
    for i in range(0, len(l), n):
        yield l[i:i + n]


class Mapper:
    API_URL = 'https://api-2445582026130.production.gw.apicast.io/'
    ENCODING = 'utf-8'

    MAX_REQUESTS_PARALLEL = 8
    N_REQUEST_RETRIES = 2
    TIMEOUT_PER_ITEM = 30
    N_TIMEOUT_RETRIES = 10
    MAX_RETIES = 10
    MAX_TIMEOUT = 35
    N_const_thread = 1
    N_sec_sleep_key_limit = 15
    count_request = 0
    error_count = 0

    def __init__(
            self,
            endpoint: str,
            api_key: str,
            companies_only: bool = False,
            in_file: str = '',
            out_file: str = '',
            force_reprocess: bool = False,
            num_requests_parallel: int = MAX_REQUESTS_PARALLEL,
            retries: int = N_REQUEST_RETRIES,
            timeout: int = TIMEOUT_PER_ITEM,
    ):
        assert endpoint in ['merchants', 'domains', 'products']

        self.endpoint = endpoint[:-1]
        self.in_file_location = os.path.expanduser(in_file)
        self.out_file_location = os.path.expanduser(out_file)
        self.api_key = api_key
        self.force_reprocess = force_reprocess
        self.companies_only = companies_only

        self.inputs_per_request = min(num_requests_parallel, self.MAX_REQUESTS_PARALLEL)
        if self.MAX_REQUESTS_PARALLEL < num_requests_parallel:
            logger.info('Number of requests to process in parallel was set to its max of {}.'.format(
                self.MAX_REQUESTS_PARALLEL))

        self.retries = min(retries, self.MAX_RETIES)
        if self.MAX_RETIES < retries:
            logger.info('Number of retries per request was set to its max of {}.'.format(self.MAX_RETIES))

        self.timeout = min(timeout, self.MAX_TIMEOUT)
        if self.MAX_TIMEOUT < timeout:
            logger.info('API request timeout (in seconds) was set to its max of {}.'.format(self.MAX_TIMEOUT))

    # Load raw inputs from input file.
    def _load_inputs(self):
        with open(self.in_file_location, 'r', encoding=self.ENCODING, errors='ignore') as in_file:
            raw_inputs = in_file.read().splitlines()

        logger.info("Reading from {}. Writing to {}.".format(self.in_file_location, self.out_file_location))
        logger.info("Number of rows of input file: {}.".format(len(raw_inputs)))

        return raw_inputs

    # Load already processed inputs from CSV output file.
    def _load_processed_inputs(self):
        result = []

        if not os.path.isfile(self.out_file_location):
            return result

        with open(self.out_file_location, 'r', encoding=self.ENCODING) as csv_out_file:
            csv_reader = csv.reader(csv_out_file, delimiter=',')

            for i, row in enumerate(csv_reader):
                if i == 0:
                    # Skip the first line which is the header.
                    continue

                if row:
                    result.append(row[0])

        return result

    # Query ADG API and return json output.
    def query_api(self, inputs):
        payload = json.dumps(inputs)
        timeout = self.timeout

        headers = {
            'Accept': "application/json",
            'Content-Type': "application/json",
            'User-Agent': 'https://github.com/geneman/altdg'
        }

        if self.companies_only and self.endpoint == 'merchant':
            headers['X-Input-Type'] = 'company name'

        for n_attempt in range(self.retries):

            if payload == '[""]':
                error = 'Empty row from input file as company name'
                continue

            if n_attempt > 0:
                logger.debug('Retrying previous request. Attempt # {}'.format(n_attempt+1))

            try:
                response = requests.post('{}/{}-mapper?X_User_Key={}'.format(self.API_URL,
                                         self.endpoint, self.api_key), data=payload, headers=headers, timeout=timeout)
                self.count_request += 1

            except Exception as ex:
                error = 'API request error: {}. Please contact info@altdg.com for help ' \
                        'if this problem persists.'.format(ex)
                logger.debug(error)
                continue

            if not response.ok:
                error = 'API response error: {} {} for inputs {}. Please contact info@altdg.com for help ' \
                        'if this problem persists.'.format(response.status_code, response.reason, inputs)
                if response.status_code != 401:
                    logger.info(error)
                continue

            return response.json()

        if 'API response error: 401 Unauthorized for inputs' not in error:
            logger.error(error)
        return [{'Original Input': rawin, 'Company Name': error} for rawin in inputs]

    def has_timeout(self, company_name):
        timeout_messages = ["API response error: 504 Gateway Time-out",
                            "Read timed out",
                            "API response error: 503 Service Unavailable: Back-end server is at capacity"]
        return any(msg in company_name for msg in timeout_messages)

    def has_wrong_key(self, company_name):
        wrong_key_messages = ["API response error: 401 Unauthorized for inputs"]
        return any(msg in company_name for msg in wrong_key_messages)

    def has_key_limit(self, company_name):
        key_limit_messages = ["API response error: 429 Too Many Requests for inputs"]
        return any(msg in company_name for msg in key_limit_messages)

    def bulk(self):

        raw_inputs = self._load_inputs()

        # Check previous CSV output file.
        # Only request for lines are not processed before.
        processed_inputs = self._load_processed_inputs()
        num_processed_inputs = len(processed_inputs)

        if not self.force_reprocess:
            raw_inputs = raw_inputs[num_processed_inputs:]
            if (len(processed_inputs)) and raw_inputs != []:
                logger.info('{} already exists in your destination with {} rows. Continue processing from row {} of '
                            'input file.'.format(self.out_file_location, len(processed_inputs), len(processed_inputs)+1))
            if (len(processed_inputs)) and raw_inputs == []:
                logger.info('{} already exists in your destination with {} rows. All rows from input file have been '
                            'processed.'.format(self.out_file_location, len(processed_inputs)))
        if self.force_reprocess and len(processed_inputs):
            logger.info('Appending results to {} from row {}.'.format(self.out_file_location, len(processed_inputs)+1))

        num_threads = self.inputs_per_request
        chunks_without_timeout_streak = 0
        chunk_counter = 0

        # Use parallel to request API for lines in each chunk.
        for i, chunk in enumerate(_chunks(raw_inputs, self.inputs_per_request)):
            list_json_response = []
            had_timeout = False
            num_reduction = (num_threads - 4) // 2 + 3
            num_retries_one_thread = 10
            max_timeout_retry = max(num_reduction + num_retries_one_thread, self.N_TIMEOUT_RETRIES)
            logger.info("Number of requests to process in parallel: {}.".format(num_threads))

            # Check time out error in a chunk.
            # If there is any time out error, then reduce number of thread and request API again.
            for timeout_retries in range(max_timeout_retry + 1):

                list_json_response = Parallel(n_jobs=num_threads)(delayed(self.query_api)([one_row])
                                                                                    for one_row in chunk)

                if any(self.has_wrong_key(response[0]['Company Name']) for response in list_json_response):
                    logger.info("Invalid ADG API application key.")
                    sys.exit()

                if any(self.has_key_limit(response[0]['Company Name']) for response in list_json_response):
                    had_timeout = True
                    if num_threads > 4:
                        num_threads -= 2
                    elif num_threads > 1:
                        num_threads -= 1
                    if timeout_retries != max_timeout_retry:
                        logger.info("Retries #{}. Number of requests to process in parallel after too many requests: "
                                    "{}. Continue in {} seconds.".format(timeout_retries+1, num_threads,
                                                                         self.N_sec_sleep_key_limit))
                    time.sleep(self.N_sec_sleep_key_limit)

                if any(self.has_timeout(response[0]['Company Name']) for response in list_json_response):
                    had_timeout = True
                    if num_threads > 4:
                        num_threads -= 2
                    elif num_threads > 1:
                        num_threads -= 1
                    if timeout_retries != max_timeout_retry:
                        logger.info("Retries #{}. Number of requests to process in parallel after timeout error: "
                                    "{}.".format(timeout_retries+1, num_threads))

                if not any(self.has_timeout(response[0]['Company Name']) or self.has_key_limit(
                        response[0]['Company Name']) for response in list_json_response):
                    break

            # Increase number of thread when there is no timeout error in a number of consecutive chunks.
            if had_timeout:
                chunks_without_timeout_streak = 0
            else:
                chunks_without_timeout_streak += 1

            if chunks_without_timeout_streak >= self.N_const_thread:
                num_threads += 1
                num_threads = min(num_threads, self.inputs_per_request)

            for one_json_response in list_json_response:
                self.write_csv(one_json_response)

            chunk_counter += 1
            proccessed_row_counter = chunk_counter * self.inputs_per_request
            if len(raw_inputs) - proccessed_row_counter <= 0:
                logger.info('Wrote {} rows to {}. Processed {} rows in total. Finished.'.format(
                    self.inputs_per_request, self.out_file_location, len(raw_inputs)))
                logger.info('{} rows succeed. {} rows failed.'.format(
                    len(raw_inputs)-self.error_count, self.error_count))
            else:
                logger.info('Wrote {} rows to {}. Processed {} rows in total. {} rows are left.'.format(
                    self.inputs_per_request, self.out_file_location, proccessed_row_counter,
                    len(raw_inputs) - proccessed_row_counter))

    # Write one API response to output CSV file.
    def write_csv(self, one_json_response):

        # Check if output file already exist.
        # Always write API response to the same CSV.
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
            'All Aliases',
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
            'All Related Entities',
            'Alternative Company Matches',
            'Websites'
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

        if (not file_exists) or len(self._load_processed_inputs()) == 0:
            writer.writeheader()
            csv_output.flush()
            logger.debug('Wrote headers to {}', format(self.out_file_location))

        # Transform API response from Json to a dictionary for writing to CSV purpose.
        for result in one_json_response:
            aliases = result.get('Aliases', [])
            related = result.get('Related Entities', [])
            # alternatives = result.get('Alternative Company Matches', [])

            non_changing_keys = ['Original Input', 'Ticker', 'Exchange',
                                 'Company Name', 'Confidence Level', 'Confidence', 'FIGI']

            csv_row = {
                **{key: value for key, value in result.items() if key in non_changing_keys},
                'Date & Time': date_time,
                'Alias 1': aliases[0] if aliases else None,
                'Alias 2': aliases[1] if len(aliases) > 1 else None,
                'Alias 3': aliases[2] if len(aliases) > 2 else None,
                'All Aliases': result.get('aliases'),
                'Related Entity 1 Name': related[0].get('Name') if related else None,
                'Related Entity 1 Score': related[0].get('Closeness Score') if related else None,
                'Related Entity 2 Name': related[1].get('Name') if len(related) > 1 else None,
                'Related Entity 2 Score': related[1].get('Closeness Score') if len(related) > 1 else None,
                'Related Entity 3 Name': related[2].get('Name') if len(related) > 2 else None,
                'Related Entity 3 Score': related[2].get('Closeness Score') if len(related) > 2 else None,
                'All Related Entities': result.get('Related Entities'),
                'Majority Owner': result.get('Majority Owner'),
                'Alternative Company Matches': result.get('Alternative Company Matches'),
                'Websites': result.get('Websites'),
            }

            writer.writerow(csv_row)
            csv_output.flush()

            logger.info('{}: {}'.format(result["Original Input"], result["Company Name"]))
            if "error" in result["Company Name"]:
                self.error_count += 1
        logger.debug('Wrote results to {}'.format(self.out_file_location))


# Check if -n (num_requests_parallel), -r (retries) and -t (timeout) are positive integers.
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
    start_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%I:%S')
    start = time.time()
    now_str = datetime.datetime.now().strftime('%Y-%m-%d')

    # Parse command-line input arguments.
    parser = ArgumentParser(
        description="""
            Examples:
            python -m adg_api_bulkMapper -e domains sample-domains.txt -k "12345"
            python -m adg_api_bulkMapper -e merchants sample-merchants.txt -k "12345"
        """,
        formatter_class=RawDescriptionHelpFormatter
    )
    parser.add_argument('-e', '--endpoint', required=True, help='Type of mapper.',
                        choices=['merchants', 'domains', 'products'], dest='endpoint')
    parser.add_argument('-k', '--key', required=True, dest='api_key', help='ADG API application key')
    parser.add_argument('-o', '--out', help='Path to output file', dest='out_file')
    parser.add_argument('-F', '--force', action='store_const', const=True, default=False, dest='force_reprocess',
                        help='Re-process results that already exist in the output file. (Adds new CSV rows.)')
    parser.add_argument('-n', '--num_requests_parallel', type=is_pos_int, default=4, dest='num_requests_parallel',
                        help='Number of requests to process in parallel. Default: {}. Max: {} '.format(4, 8))
    parser.add_argument('-r', '--retries', type=is_pos_int, default=2, dest='retries',
                        help='Number of retries per request. Default: {}. Max: {}'.format(2, 10))
    parser.add_argument('-t', '--timeout', type=is_pos_int, default=30, dest='timeout',
                        help='API request timeout (in seconds). Default: {}. Max: {}'.format(30, 35))
    parser.add_argument('-c', '--companies_only',  action='store_const', const=True, default=False, dest='companies_only',
                        help='The input data contains only clean company names text (Applies only to the Merchants Mapper Type')
    parser.add_argument(help='Path to input file', dest='in_file')
    args = parser.parse_args()

    # check if input file exists.
    # If input file does not exist, then terminate the program.
    exists = os.path.isfile(args.in_file)
    if not exists:
        logger.error("Input file does not exist")
        sys.exit()

    # Take input file name plus date as output file name.
    if not args.out_file:
        in_file_dir, in_file_name = os.path.split(args.in_file)
        args.out_file = os.path.join(
            in_file_dir,
            '{}-{}.csv'.format(os.path.splitext(in_file_name)[0], now_str)
        )

    # Start making API request.
    Mapper(**vars(args)).bulk()
    end_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%I:%S')
    end = time.time()
    logger.info('Time started: {}. Time ended: {}. Time elapsed (in sec): {:.2f}.'.format(start_time, end_time, end-start))
