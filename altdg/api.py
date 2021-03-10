#!/usr/bin/env python
"""
2020 Alternative Data Group. All Rights Reserved.

Module for running input file through AltDG's mapper API and writing results to CSV file.

Requires python >= 3.6.
Install dependencies with 'pip install requirements.txt'.

Help: altdg -h
"""
import argparse
import csv
import datetime
import logging
import os
import sys
from argparse import ArgumentParser, RawDescriptionHelpFormatter
from concurrent.futures.thread import ThreadPoolExecutor
from copy import copy
from operator import itemgetter
from random import randrange
from time import sleep
from typing import Optional, Iterator, Iterable, Any, Union, Generator, List

import requests
from chardet import UniversalDetector

logger = logging.getLogger(__name__)


def chunks(collection: Iterable, n: int):
    """ Yield successive n-sized chunks from collection """
    collection = list(collection)
    for i in range(0, len(collection), n):
        yield collection[i:i + n]


def get_or_default(lst: list, idx: int, default: Any) -> Any:
    try:
        return lst[idx]
    except IndexError:
        return default


class ChunkedDictReader(csv.DictReader):
    def read(self, chunk_size: int, offset: int = 0) -> Generator[List[dict], None, None]:
        assert chunk_size >= 1, f'Wrong chunk size: {chunk_size}'

        chunk = []
        for i, row in enumerate(self):
            if i < offset:
                continue

            chunk.append(row)
            if len(chunk) >= chunk_size:
                yield chunk
                chunk = []

        else:
            if chunk:
                yield chunk


class AltdgAPI:
    # ---- access settings ----
    API_URL = 'https://api-2445582026130.production.gw.apicast.io/'
    DEMO_KEY = 'f816b9125492069f7f2e3b1cc60659f0'
    SUPPORT_EMAIL = 'info@altdg.com'
    HEADERS = {
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        'User-Agent': 'https://github.com/altdg/bulk_mapper',
    }

    # ---- input processing ----
    MAX_INPUT_LENGTH = 127

    # ---- parallel settings ----
    DEFAULT_NUM_THREADS = 2
    MAX_NUM_THREADS = 4

    # ---- failure handling ----
    RESPONSE_TIMEOUT = 30  # max time to wait for each query to complete
    DEFAULT_NUM_RETRIES = 2
    RETRY_INTERVAL = 60  # if attempt 1 failed, wait 0-60s, if attempt 2 failed - wait 60-120s etc

    # ---- csv file fields mappings  ----
    CSV_FIELDS = {
        # CSV file field: API output mapping function
        'Original Input': itemgetter('Original Input'),
        'Type Hint': itemgetter('Type Hint'),
        'Company Name': lambda result: result.get('Company Name', ''),
        'Alias 1': lambda result: get_or_default(result.get('Aliases', []), 0, ''),
        'Alias 2': lambda result: get_or_default(result.get('Aliases', []), 1, ''),
        'Alias 3': lambda result: get_or_default(result.get('Aliases', []), 2, ''),
        'All Aliases': lambda result: '; '.join(result.get('Aliases', [])),
        'Confidence Level': lambda result: result.get('Confidence Level', ''),
        'Confidence': lambda result: result.get('Confidence', ''),
        'Ticker': lambda result: result.get('Ticker', ''),
        'Exchange': lambda result: result.get('Exchange', ''),
        'Majority Owner': lambda result: result.get('Majority Owner', ''),
        'FIGI': lambda result: result.get('FIGI', ''),
        'Related Entity 1 Name': lambda result: get_or_default(result.get('Related Entities', []), 0, ''),
        'Related Entity 2 Name': lambda result: get_or_default(result.get('Related Entities', []), 1, ''),
        'Related Entity 3 Name': lambda result: get_or_default(result.get('Related Entities', []), 2, ''),
        'All Related Entities': lambda result: '; '.join(result.get('Related Entities', [])),
        'Alternative Company Matches': lambda result: ': '.join(result.get('Alternative Company Matches', [])),
        'Websites': lambda result: '; '.join(result.get('Websites', [])),
        'Date & Time': lambda result: datetime.datetime.now().strftime('%Y-%m-%d %H:%I:%S'),
    }

    def __init__(
        self,
        endpoint: str,
        api_key: str = DEMO_KEY,
        num_threads: int = DEFAULT_NUM_THREADS,
        num_retries: int = DEFAULT_NUM_RETRIES,
    ):
        """
        Initialize AltDG API mapper with some inter-requests settings.

        Args:
            endpoint: mapper's endpoint ("merchant-mapper", "domain-mapper", etc) - see docs
            api_key: your API key
            num_threads: how many threads to use
            num_retries: how many retries to perform if case of failure
        """
        if endpoint in ['merchants', 'domains', 'products']:
            raise ValueError(f'Outdated endpoint value "{endpoint}", use one from AltDG API docs '
                             f'("merchant-mapper", "domain-mapper" etc')

        self.endpoint = endpoint
        self.api_key = api_key
        self.num_retries = num_retries

        self.num_threads = min(num_threads, self.MAX_NUM_THREADS)
        if num_threads > self.MAX_NUM_THREADS:
            logger.warning('Number of requests to process in parallel was set to its max of {self.num_threads}.')

    def prepare_input(self, inp: str) -> str:
        """
        Truncates string to be no more than MAX_INPUT_LENGTH. Warns if truncation happened.
        """
        if not isinstance(inp, str):
            raise ValueError('Input is not string: {}'.format(inp))

        if len(inp) > self.MAX_INPUT_LENGTH:
            inp = inp[:self.MAX_INPUT_LENGTH]
            logger.warning(f'Input too long, truncated input to {self.MAX_INPUT_LENGTH} chars: "{inp}"')

        return inp

    def query(self, value: str, hint: Optional[str] = None, clean: bool = True) -> dict:
        """
        Make a single request to AltDG API.

        Args:
            value: text string to map ("amzn", "PURCHASE DEBIT CARD XXXX-2211 ETSY.COM", ...)
            hint: any string which may help identifying input type ("company", "agriculture", "brand" etc)
            clean: whether to "clean" input string, i.e. do a preprocessing to remove garbage substrings

        Returns:
            dict {
                'Original Input': ...,
                'Company Name': company name retrieved from AltDG API,
                ... <additional fields, refer to AltDG API docs> ...
            }
        """
        if isinstance(value, tuple):
            assert len(value) == 2, f'Only (input, hint) tuples are allowed as value, but received {value}'
            if value[1]:
                hint = value[1]

            value = value[0]

        if not value:
            return {}

        logger.debug(f'Running value "{value}"' + (f' with hint "{hint}"' if hint else ''))

        payload = [self.prepare_input(value)]

        headers = copy(self.HEADERS)
        if hint:
            headers['X-Type-Hint'] = hint

        headers['X-Clean-Input'] = str(clean)

        for n_attempt in range(1, self.num_retries+1):
            if n_attempt > 1:
                logger.debug(f'Retrying (attempt #{n_attempt}): {value}')

            try:
                response = requests.post(
                    f'{self.API_URL}/{self.endpoint}?X_User_Key={self.api_key}',
                    json=payload,
                    headers=headers,
                    timeout=self.RESPONSE_TIMEOUT,
                )
                response.raise_for_status()
                result = response.json()[0]
                result['Type Hint'] = hint  # TODO: modify API
                return result

            except Exception as exc:
                logger.debug(f'Error when mapping {payload}: {exc}')

                # raise exception on 4xx errors
                if 'response' in vars() and 400 <= response.status_code < 500:  # NOQA
                    raise

                wait = randrange(
                    self.RETRY_INTERVAL*(n_attempt-1),
                    self.RETRY_INTERVAL*n_attempt
                )
                logger.debug(f'Waiting {wait}s before another attempt')
                sleep(wait)

        logger.warning(f'Could not process "{value}". Please contact {self.SUPPORT_EMAIL} for help '
                       f'if this problem persists.')

        return {
            'Original Input': value,
            'Type Hint': hint,
        }

    def bulk_query(self, values: Iterable[Union[str, tuple]], hint: Optional[str] = None,
                   clean: bool = True) -> Iterator[dict]:
        """
        Processes `values` in bulk using multithreading.

        Args:
            values: collection of items to map; item may be a simple string input or (input, hint) tuple
            hint: any string which may help identifying input type ("company", "agriculture", "brand" etc)
            clean: whether to clean values

        Yields:
            dict of mapped info (see `query` method)

        """
        yield from ThreadPoolExecutor(max_workers=self.num_threads).map(
            lambda value: self.query(value, hint, clean), values)

    @staticmethod
    def detect_encoding(file_path: str) -> str:
        """ Detects encoding of given file """
        detector = UniversalDetector()

        with open(file_path, 'rb') as file:
            for line in file.readlines():
                detector.feed(line)
                if detector.done:
                    break

        detector.close()

        encoding = detector.result['encoding']
        logger.debug(f'Detected encoding for file "{file_path}": {encoding}')

        return encoding

    def process_file(
            self,
            input_file_path: str,
            input_file_encoding: Optional[str] = None,
            input_file_chunk_size: int = 16,
            output_file_path: Optional[str] = None,
            output_file_encoding: Optional[str] = None,
            force_reprocess: bool = False,
            hint: Optional[str] = None,
            clean: bool = True,
    ):
        """
        Runs input file (one input per row, TXT or CSV) through AltDG Mapping API and produces
        a CSV file with results.

        Args:
            input_file_path: path to input file
            input_file_encoding: encoding of input file
            input_file_chunk_size: how many rows read at once from input file
            output_file_path: path to output file; if empty, input file name + current date will be used
            output_file_encoding: encoding of output file (by default 'utf-8-sig' on Windows, 'utf-8'
                on other platforms)
            force_reprocess: whether to re-process already processed rows
            hint: optional value which may help mapping inputs (i.e. "medical", "bank", "agriculture" etc)
            clean: whether to clean inputs
        """
        logger.debug(f'Starting processing "{input_file_path}"')

        # ---- create output file ----
        if not output_file_path:
            # take input file name plus date as output file name.
            in_file_dir, in_file_name = os.path.split(input_file_path)
            output_file_path = os.path.join(
                in_file_dir,
                f'{os.path.splitext(in_file_name)[0]}-{datetime.datetime.now().strftime("%Y-%m-%d")}.csv'
            )

        out_dir = os.path.dirname(output_file_path)
        if out_dir:
            os.makedirs(out_dir, exist_ok=True)
        logger.debug(f'Output to "{output_file_path}"')

        # ---- detect encodings ----
        input_file_encoding = input_file_encoding or self.detect_encoding(input_file_path)
        if not output_file_encoding:
            if os.path.isfile(output_file_path):  # if output file already exists, detect encoding from it
                output_file_encoding = self.detect_encoding(output_file_path)
            else:
                output_file_encoding = 'utf-8-sig' if os.name == 'nt' else 'utf-8'
        logger.debug(f'Output file encoding: {output_file_encoding}')

        # ---- retrieve indexes of already processed inputs ----
        num_processed = 0
        if not force_reprocess and os.path.isfile(output_file_path):
            with open(output_file_path, 'r', encoding=output_file_encoding) as out_file:
                reader = csv.DictReader(out_file, delimiter=',')
                num_processed = sum(1 for _ in reader)

        if num_processed:
            logger.debug(f'Found {num_processed} already processed inputs, skipping')

        # ---- process inputs ----
        with open(input_file_path, 'r', encoding=input_file_encoding) as in_file, \
                open(output_file_path, 'w' if force_reprocess else 'a',
                     encoding=output_file_encoding, newline='') as out_file:

            reader = ChunkedDictReader(
                in_file,
                fieldnames=['input', 'hint'],
            )

            writer = csv.DictWriter(
                out_file,
                fieldnames=self.CSV_FIELDS.keys(),
                extrasaction='ignore',
                delimiter=',',
                quotechar='"',
                quoting=csv.QUOTE_MINIMAL,
            )

            if not num_processed:
                writer.writeheader()

            # just in case the file is big, we read it by chunks
            for chunk in reader.read(chunk_size=input_file_chunk_size, offset=num_processed):
                logger.debug(f'Processing inputs chunk of size {len(chunk)}')
                queue = [(value['input'], value['hint']) for value in chunk]

                for result in self.bulk_query(queue, hint=hint, clean=clean):
                    logger.info(f'Writing result '
                                f'{ {k: v for k, v in result.items() if k in list(self.CSV_FIELDS)[:3]} }')
                    writer.writerow({field: mapper(result) for field, mapper in self.CSV_FIELDS.items()}
                                    if result else {})
                    out_file.flush()

                num_processed += len(chunk)
                logger.debug(f'Processed {num_processed} inputs so far')

            logger.debug('Processing complete')


def positive_integer(value: str) -> int:
    try:
        value = int(value)
    except ValueError:
        raise argparse.ArgumentTypeError(f'Not a number: {value}')

    if value <= 0:
        raise argparse.ArgumentTypeError(f'Not a positive number: {value}')

    return value


def main():
    parser = ArgumentParser(
        description=f"""
            Examples:
            altdg -e domain-mapper sample-domains.txt -k "{AltdgAPI.DEMO_KEY}"
            altdg -e merchant-mapper sample-merchants.txt -k "{AltdgAPI.DEMO_KEY}"
        """,
        formatter_class=RawDescriptionHelpFormatter
    )
    # ---- AltdgAPI args ----
    parser.add_argument(
        '-e', '--endpoint',
        help='Type of mapper',
        default='merchant-mapper',
        dest='endpoint',
    )
    parser.add_argument(
        '-k', '--key',
        help='AltDG API application key',
        default=AltdgAPI.DEMO_KEY,
        dest='api_key',
    )
    parser.add_argument(
        '-n', '--num-threads',
        help=f'Number of threads. Max: {AltdgAPI.MAX_NUM_THREADS}',
        default=AltdgAPI.DEFAULT_NUM_THREADS,
        type=positive_integer,
        dest='num_threads',
    )
    parser.add_argument(
        '-r', '--num-retries',
        help=f'Number of retries if request returns 5xx error.'
             f'Delay between retries increments after each unsuccessful attempt.',
        default=AltdgAPI.DEFAULT_NUM_RETRIES,
        type=positive_integer,
        dest='num_retries',
    )
    parser.add_argument(
        '-l', '--log-level',
        help=f'Log level',
        default='info',
        type=lambda level: getattr(logging, level.upper()),
        dest='log_level',
    )

    # ---- process_file args ----
    parser.add_argument(
        '--encoding',
        help=f'Input file encoding (will auto-detect if this option is missing)',
        type=str,
        dest='input_file_encoding',
    )
    parser.add_argument(
        '-o', '--out',
        help='Path to output file',
        dest='output_file_path')
    parser.add_argument(
        '-F', '--force',
        help='Process all inputs even if some results already exist in the output file',
        default=False,
        action='store_const', const=True,
        dest='force_reprocess',
    )
    parser.add_argument(
        '-th', '--type-hint',
        help='Any hint about input data ("company", "brand", ...)',
        default=None,
        dest='hint',
    )
    parser.add_argument(
        '-c', '--clean',
        choices=['high', 'low'],
        help='How much input cleaning should be done. If your inputs contain a lot of noize '
             '(meaningless information), set this to "high"; if your inputs are rather good '
             '(for example, contain exact titles), set this option to "low".',
        default='high',
        dest='clean',
    )
    parser.add_argument(
        help='Path to input file',
        dest='input_file_path',
    )
    args = parser.parse_args()

    print(fr"""
 _____ _ _                   _   _            ____      _          _____
|  _  | | |_ ___ ___ ___ ___| |_|_|_ _ ___   |    \ ___| |_ ___   |   __|___ ___ _ _ ___
|     | |  _| -_|  _|   | .'|  _| | | | -_|  |  |  | .'|  _| .'|  |  |  |  _| . | | | . |
|__|__|_|_| |___|_| |_|_|__,|_| |_|\_/|___|  |____/|__,|_| |__,|  |_____|_| |___|___|  _|
                                                                                    |_|
                                    {AltdgAPI.SUPPORT_EMAIL}
    """)

    logging.basicConfig(level=args.log_level, format='%(asctime)s %(levelname)-8s %(message)s')
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)

    start_time = datetime.datetime.now()
    logger.info(f'Starting mapping')

    if not os.path.isfile(args.input_file_path):
        logger.error(f'Input file does not exist: {args.input_file_path}')
        sys.exit()

    if args.hint:
        logger.info(f'Using type hint: {args.hint}')

    AltdgAPI(**{
        arg: value for arg, value in vars(args).items()
        if arg in ['endpoint', 'api_key', 'num_threads', 'num_retries']
    }).process_file(**{
        arg: value for arg, value in vars(args).items() if arg in [
            'input_file_path', 'input_file_encoding', 'output_file_path', 'force_reprocess',
            'hint', 'clean',
        ]
    })

    end_time = datetime.datetime.now()
    logger.info(f'All done')
    logger.info(f'Elapsed: {(end_time-start_time).seconds:.0f}s')


if __name__ == '__main__':
    main()
