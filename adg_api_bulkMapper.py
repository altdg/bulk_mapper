#!/usr/bin/env python
"""
2018 Alternative Data Group. All Rights Reserved.
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


import sys
if float(sys.version[:3]) < 3.6:
    sys.exit("This script requires Python version 3.6")

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
    API_URL = 'https://api-2445582026130.production.gw.apicast.io/'
    # API_URL = 'http://adg-api-dev.us-east-1.elasticbeanstalk.com/'
    ENCODING = 'utf-8'

    count_request = 0
    N_TIMEOUT_RETRIES = 10
    N_const_thread = 1
    N_sec_sleep_key_limit = 15

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
            raw_inputs = in_file.read().splitlines()

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
            # 'X-3scale-proxy-secret-token':'rkMcYebOqEIRmpFFm0nGIBex7m8pRVtR'
        }

        timeout = self.timeout
        logger.debug(f'Timeout for these inputs: {timeout}')

        for n_attempt in range(self.retries):
            if n_attempt > 0:
                logger.debug(f'Retrying previous request. Attempt # {n_attempt+1}')

            try:
                response = requests.post(f'{self.API_URL}/{self.endpoint}-mapper-debug?X_User_Key={self.api_key}',
                                     data=payload, headers=headers, timeout=timeout)
                # response = requests.post(f'{self.API_URL}/{self.endpoint}-mapper', data=payload, headers=headers, timeout=timeout)
                print(f'>>>>> CALLING REQUESTS.POST FOR PAYLOAD {payload}')
                self.count_request += 1


            except Exception as ex:
                error = f'API request error: {ex}. ' \
                    f'Please contact info@altdg.com for help if this problem persists.'
                logger.debug(error)

                continue

            # if response.status_code == 401:
            #     key_error = f'API response error: {response.status_code}. You have the wrong key. ' \
            #         f'Please contact info@altdg.com for help if this problem persists.'
            #     logger.error(key_error)
            #     sys.exit()

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

    def has_wrong_key(self,company_name):
        wrong_key_messages = ["401"]
        return any(msg in company_name for msg in wrong_key_messages)

    def has_key_limit(self,company_name):
        key_limit_messages = ["limit"]
        return any(msg in company_name for msg in key_limit_messages)

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

                # list_json_response = [[{'Original Input': 'Interest Charged on Purchases\n', 'Company Name': 'limit', 'Confidence': None, 'Confidence Level': None, 'Aliases': [], 'Alternative Company Matches': [], 'Related Entities': [], 'Ticker': None, 'Exchange': None, 'Majority Owner': None, 'FIGI': None, '_DEBUG': {'original_input': 'Interest Charged on Purchases\n', 'mode': 'merchant', 'start_time': '2019-03-21 19:05:25.431822', 'attempts': [{'cleaner': 'CcCleaner', 'cleaned': '', 'cleaned_score': None}, {'cleaner': 'MerchantCleaner', 'cleaned': 'Interest Charged on Purchases', 'cleaned_score': None, 'er_times': {'bane': 1.092, 'fwfm': 1.088, 'wikipedia': 1.083, 'google_knowledge_graph': 1.074, 'company_website': 1.088, 'bloomberg_lite': 1.079, 'small_parsers': 1.462}, 'losers': [{'graph': [{'id': '5d8df9a0', 'value': 'Tax Management, Inc.', 'score': 1, 'url': 'https://www.bloomberg.com/research/stocks/private/snapshot.asp?privcapId=4259664', 'source': 'bloomberg_lite', 'comparison': {'names': ['tax management'], 'websites': []}}], 'score': 0.015193906612694263}, {'graph': [{'id': 'd8da1584', 'value': 'TD Bank, N.A', 'score': 1, 'websites': ['td.com'], 'source': 'company_website[copyright]', 'company_website': 'http://www.tdbank.com/tdhelps/default.aspx/what-is-interest-charge-purchases-charge-on-my-td-bank-visa/v/40487597/', 'comparison': {'names': ['td bank'], 'websites': ['td.com']}}, {'id': '78af8092', 'value': 'Visa U.S.A. Inc', 'score': 1, 'websites': ['td.com'], 'source': 'company_website[copyright]', 'company_website': 'http://www.tdbank.com/tdhelps/default.aspx/what-is-interest-charge-purchases-charge-on-my-td-bank-visa/v/40487597/', 'comparison': {'names': ['visa u'], 'websites': ['td.com']}}, {'id': '45aa7e16', 'value': 'The Toronto-Dominion Bank', 'score': 1, 'websites': ['td.com'], 'source': 'company_website[ssl]', 'company_website': 'http://www.tdbank.com/tdhelps/default.aspx/what-is-interest-charge-purchases-charge-on-my-td-bank-visa/v/40487597/', 'comparison': {'names': ['toronto dominion bank'], 'websites': ['td.com']}}, {'id': '01cd5cc1', 'value': 'TD Bank, N.A.', 'score': 1, 'wiki_page': 'https://en.wikipedia.org/wiki/TD_Bank,_N.A.', 'internal_scores': {'value_in_websites': False, 'references_count': 1, 'title_count': 32, 'article_title_count': 32, 'url_count': 12, 'rank': 0}, 'websites': ['http://tdbank.com'], 'ticker': None, 'exchange': None, 'source': 'company_website[wikipedia]', 'company_website': 'http://www.tdbank.com/tdhelps/default.aspx/what-is-interest-charge-purchases-charge-on-my-td-bank-visa/v/40487597/', 'comparison': {'names': ['td bank'], 'websites': ['tdbank.com']}, 'parent': [['a32d9186', 'company_website[wikipedia]', 'Toronto-Dominion Bank']]}, {'id': 'a32d9186', 'value': 'Toronto-Dominion Bank', 'score': None, 'wiki_page': 'https://en.wikipedia.org/wiki/Toronto-Dominion_Bank', 'internal_scores': {}, 'websites': ['https://www.td.com/about-tdbfg/our-business/index.jsp'], 'ticker': 'TD', 'exchange': 'NYSE', 'aliases': ['Toronto–Dominion Bank'], 'source': 'company_website[wikipedia]', 'company_website': 'http://www.tdbank.com/tdhelps/default.aspx/what-is-interest-charge-purchases-charge-on-my-td-bank-visa/v/40487597/', 'comparison': {'names': ['toronto dominion bank', 'toronto–dominion bank'], 'websites': ['td.com']}}, {'id': '969601c7', 'value': 'TD Securities', 'score': None, 'wiki_page': 'https://en.wikipedia.org/wiki/TD_Securities', 'source': 'company_website[wikipedia]', 'company_website': 'http://www.tdbank.com/tdhelps/default.aspx/what-is-interest-charge-purchases-charge-on-my-td-bank-visa/v/40487597/', 'comparison': {'names': ['td securities'], 'websites': []}, 'parent': [['a32d9186', 'company_website[wikipedia]', 'Toronto-Dominion Bank']]}, {'id': '0cfa86d0', 'value': 'TD Waterhouse', 'score': None, 'wiki_page': 'https://en.wikipedia.org/wiki/TD_Waterhouse', 'source': 'company_website[wikipedia]', 'company_website': 'http://www.tdbank.com/tdhelps/default.aspx/what-is-interest-charge-purchases-charge-on-my-td-bank-visa/v/40487597/', 'comparison': {'names': ['td waterhouse'], 'websites': []}, 'parent': [['a32d9186', 'company_website[wikipedia]', 'Toronto-Dominion Bank']]}, {'id': '8f47fd2b', 'value': 'TD Bank, N.A.', 'score': None, 'wiki_page': 'https://en.wikipedia.org/wiki/TD_Bank,_N.A.', 'source': 'company_website[wikipedia]', 'company_website': 'http://www.tdbank.com/tdhelps/default.aspx/what-is-interest-charge-purchases-charge-on-my-td-bank-visa/v/40487597/', 'comparison': {'names': ['td bank'], 'websites': []}, 'parent': [['a32d9186', 'company_website[wikipedia]', 'Toronto-Dominion Bank']]}, {'id': '2bc40d82', 'value': 'TD Asset Management', 'score': None, 'source': 'company_website[wikipedia]', 'company_website': 'http://www.tdbank.com/tdhelps/default.aspx/what-is-interest-charge-purchases-charge-on-my-td-bank-visa/v/40487597/', 'comparison': {'names': ['td asset management'], 'websites': []}, 'parent': [['a32d9186', 'company_website[wikipedia]', 'Toronto-Dominion Bank']]}, {'id': '5d913e23', 'value': 'TD Bank, N.A.', 'score': 0.469, 'source': 'company_website[bane]', 'company_website': 'http://www.tdbank.com/tdhelps/default.aspx/what-is-interest-charge-purchases-charge-on-my-td-bank-visa/v/40487597/', 'comparison': {'names': ['td bank'], 'websites': []}}], 'score': 0.4594162404537201}], 'winners': []}], 'winner': None, 'end_time': '2019-03-21 19:05:27.160138', 'exec_time': '0:00:01.728316', 'request': {'headers': {'Host': 'adg-api.us-east-1.elasticbeanstalk.com', 'Accept': 'application/json', 'Accept-Encoding': 'gzip, deflate', 'Content-Type': 'application/json', 'Forwarded': 'for=10.0.103.55;host=api-2445582026130.production.gw.apicast.io;proto=http', 'User-Agent': 'https://github.com/geneman/altdg', 'X-3Scale-Proxy-Secret-Token': 'rkMcYebOqEIRmpFFm0nGIBex7m8pRVtR', 'X-Configurations': '{"CACHE_ERS": False}', 'X-Forwarded-Host': 'api-2445582026130.production.gw.apicast.io', 'X-Real-Ip': '10.1.13.3', 'X-Forwarded-For': '12.97.143.6, 34.231.171.211', 'X-Forwarded-Port': '80', 'X-Forwarded-Proto': 'http', 'Content-Length': '35', 'Connection': 'keep-alive'}, 'args': {'X_User_Key': '55b99f8d0a9db4915ed41a3ea7e04bb8'}, 'url': 'http://adg-api.us-east-1.elasticbeanstalk.com/merchant-mapper-debug?X_User_Key=55b99f8d0a9db4915ed41a3ea7e04bb8', 'data': 'b\'["Interest Charged on Purchases\\\\n"]\'', 'referrer': None, 'remote_addr': '172.30.4.129', 'view_args': {'mode': 'merchant', 'debug': True}, 'configuration': {}}, 'run_uid': 'f264fa96-d8d7-44d5-b5f2-1463d653c510'}}], [{'Original Input': 'EGSFIRST ENERGY CORP  XXXXX0045 XXXXX1630 PA\n', 'Company Name': 'FirstEnergy', 'Confidence': 1.0, 'Confidence Level': 'High', 'Aliases': ['FirstEnergy Corp.'], 'Alternative Company Matches': [], 'Related Entities': [], 'Ticker': 'FE', 'Exchange': 'NYSE', 'Majority Owner': 'FIRSTENERGY CORP', 'FIGI': 'BBG000BB6M98', '_DEBUG': {'original_input': 'EGSFIRST ENERGY CORP  XXXXX0045 XXXXX1630 PA\n', 'mode': 'merchant', 'start_time': '2019-03-21 19:05:25.520993', 'attempts': [{'cleaner': 'CcCleaner', 'cleaned': 'EGSFIRST ENERGY CORP', 'cleaned_score': None, 'er_times': {'bane': 1.004, 'fwfm': 1.001, 'company_website': 0.998, 'google_knowledge_graph': 1.377, 'bloomberg_lite': 1.445, 'wikipedia': 1.565, 'small_parsers': 1.573}, 'losers': [{'graph': [{'id': '3d2eab83', 'value': 'The Stop & Shop Supermarket Company LLC', 'score': 1, 'url': 'https://www.bloomberg.com/research/stocks/private/snapshot.asp?privcapId=319495', 'source': 'bloomberg_lite', 'comparison': {'names': ['stop shop supermarket'], 'websites': []}}], 'score': 0.015193906612694263}], 'winners': [{'graph': [{'id': 'e28b3be5', 'value': 'FirstEnergy', 'score': 0.972, 'source': 'bane', 'comparison': {'names': ['firstenergy'], 'websites': []}, 'occurs': 6, 'winner_prob': 15.9}, {'id': 'ed7fe1a4', 'value': 'FirstEnergy', 'score': 0.844, 'target': 'firstenergy', 'source': 'fwfm', 'comparison': {'names': ['firstenergy'], 'websites': []}, 'occurs': 6, 'winner_prob': 16.15}, {'id': '0a24ee05', 'value': 'FirstEnergy', 'score': None, 'websites': ['http://www.firstenergycorp.com/'], 'company_website': 'http://www.firstenergycorp.com/', 'source': 'company_website', 'comparison': {'names': ['firstenergy'], 'websites': ['firstenergycorp.com']}, 'occurs': 6, 'winner_prob': 15.9}, {'id': '669fe2bf', 'value': 'FirstEnergy Corp.', 'score': 1, 'websites': ['firstenergycorp.com'], 'source': 'company_website[ssl]', 'company_website': 'http://www.firstenergycorp.com/', 'comparison': {'names': ['firstenergy'], 'websites': ['firstenergycorp.com']}, 'occurs': 5, 'winner_prob': 9.043}, {'id': '8c979202', 'value': 'FirstEnergy', 'score': 1.0, 'source': 'company_website[bane]', 'company_website': 'http://www.firstenergycorp.com/', 'comparison': {'names': ['firstenergy'], 'websites': []}, 'occurs': 6, 'winner_prob': 15.9}, {'id': '251134ec', 'value': 'FirstEnergy Corp.', 'score': 1, 'wiki_page': 'https://en.wikipedia.org/wiki/FirstEnergy_Corp', 'internal_scores': {'value_in_websites': True, 'references_count': 1, 'title_count': 133, 'article_title_count': 133, 'url_count': 88, 'rank': 0}, 'websites': ['http://firstenergycorp.com'], 'ticker': 'FE', 'exchange': 'NYSE', 'aliases': ['FirstEnergy'], 'source': 'company_website[wikipedia]', 'company_website': 'http://www.firstenergycorp.com/', 'comparison': {'names': ['firstenergy', 'firstenergy'], 'websites': ['firstenergycorp.com']}, 'occurs': 5, 'winner_prob': 10.293}, {'id': '5145a078', 'value': 'FirstEnergy Corp', 'score': 1, 'websites': ['firstenergycorp.com'], 'source': 'company_website[copyright]', 'company_website': 'http://www.firstenergycorp.com/', 'comparison': {'names': ['firstenergy'], 'websites': ['firstenergycorp.com']}, 'occurs': 1, 'winner_prob': 0.304}, {'id': 'e5ef81bc', 'value': 'FirstEnergy Corp.', 'score': 1, 'websites': ['firstenergycorp.com'], 'source': 'company_website[whois]', 'company_website': 'http://www.firstenergycorp.com/', 'comparison': {'names': ['firstenergy'], 'websites': ['firstenergycorp.com']}, 'occurs': 5, 'winner_prob': 9.043}, {'id': 'f56ce84e', 'value': 'FirstEnergy', 'score': 1, 'url': 'https://www.bloomberg.com/research/stocks/private/snapshot.asp?privcapId=293515', 'source': 'company_website[bloomberg_lite]', 'company_website': 'http://www.firstenergycorp.com/', 'comparison': {'names': ['firstenergy'], 'websites': []}, 'occurs': 6, 'winner_prob': 15.9}, {'id': '883d944d', 'value': 'FirstEnergy', 'score': 0.844, 'target': 'firstenergy', 'source': 'company_website[fwfm]', 'company_website': 'http://www.firstenergycorp.com/', 'comparison': {'names': ['firstenergy'], 'websites': []}, 'occurs': 6, 'winner_prob': 16.15}, {'id': '780d691e', 'value': 'FirstEnergy Corp.', 'score': 1, 'wiki_page': 'https://en.wikipedia.org/wiki/FirstEnergy', 'internal_scores': {'references_count': 0, 'title_count': 144, 'article_title_count': 144, 'url_count': 18, 'rank': 0}, 'websites': ['http://firstenergycorp.com'], 'ticker': 'FE', 'exchange': 'NYSE', 'aliases': ['FirstEnergy'], 'source': 'wikipedia', 'comparison': {'names': ['firstenergy', 'firstenergy'], 'websites': ['firstenergycorp.com']}, 'occurs': 5, 'winner_prob': 10.293}, {'id': '7fd79dca', 'value': 'FirstEnergy Corp.', 'score': 0.894, 'source': 'small_parsers[TwitterParser]', 'from_url': 'https://twitter.com/FirstEnergyCorp', 'position': 2, 'comparison': {'names': ['firstenergy'], 'websites': []}, 'occurs': 5, 'winner_prob': 9.043}], 'score': 0.9970934391021729}]}, {'cleaner': 'MerchantCleaner', 'cleaned': 'EGSFIRST ENERGY CORP XXXXX0045 XXXXX1630 PA', 'cleaned_score': None, 'er_times': {'bane': 0.997, 'fwfm': 0.995, 'company_website': 1.456, 'wikipedia': 1.502, 'small_parsers': 1.558, 'google_knowledge_graph': 1.599, 'bloomberg_lite': 1.609}, 'losers': [{'graph': [{'id': '53d79184', 'value': 'CSL Capital Management, L.P.', 'score': 1, 'url': 'https://www.bloomberg.com/research/stocks/private/snapshot.asp?privcapId=113640181', 'source': 'bloomberg_lite', 'comparison': {'names': ['csl capital management'], 'websites': []}}], 'score': 0.015193906612694263}], 'winners': [{'graph': [{'id': '1ca754e8', 'value': 'FirstEnergy', 'score': 0.553, 'source': 'bane', 'comparison': {'names': ['firstenergy'], 'websites': []}}, {'id': '71cadffc', 'value': 'FirstEnergy', 'score': 0.421, 'target': 'firstenergy', 'source': 'fwfm', 'comparison': {'names': ['firstenergy'], 'websites': []}}, {'id': '29942684', 'value': 'FirstEnergy', 'score': None, 'websites': ['https://www.firstenergycorp.com/content/customer/save_energy/save_energy_pennsylvania.html'], 'company_website': 'https://www.firstenergycorp.com/content/customer/save_energy/save_energy_pennsylvania.html', 'source': 'company_website', 'comparison': {'names': ['firstenergy'], 'websites': ['firstenergycorp.com']}}, {'id': 'e5ef81bc', 'value': 'FirstEnergy Corp.', 'score': 1, 'websites': ['firstenergycorp.com'], 'source': 'company_website[whois]', 'company_website': 'https://www.firstenergycorp.com/content/customer/save_energy/save_energy_pennsylvania.html', 'comparison': {'names': ['firstenergy'], 'websites': ['firstenergycorp.com']}}, {'id': '5145a078', 'value': 'FirstEnergy Corp', 'score': 1, 'websites': ['firstenergycorp.com'], 'source': 'company_website[copyright]', 'company_website': 'https://www.firstenergycorp.com/content/customer/save_energy/save_energy_pennsylvania.html', 'comparison': {'names': ['firstenergy'], 'websites': ['firstenergycorp.com']}}, {'id': '669fe2bf', 'value': 'FirstEnergy Corp.', 'score': 1, 'websites': ['firstenergycorp.com'], 'source': 'company_website[ssl]', 'company_website': 'https://www.firstenergycorp.com/content/customer/save_energy/save_energy_pennsylvania.html', 'comparison': {'names': ['firstenergy'], 'websites': ['firstenergycorp.com']}}, {'id': '251134ec', 'value': 'FirstEnergy Corp.', 'score': 1, 'wiki_page': 'https://en.wikipedia.org/wiki/FirstEnergy_Corp', 'internal_scores': {'value_in_websites': True, 'references_count': 1, 'title_count': 133, 'article_title_count': 133, 'url_count': 88, 'rank': 0}, 'websites': ['http://firstenergycorp.com'], 'ticker': 'FE', 'exchange': 'NYSE', 'aliases': ['FirstEnergy'], 'source': 'company_website[wikipedia]', 'company_website': 'https://www.firstenergycorp.com/content/customer/save_energy/save_energy_pennsylvania.html', 'comparison': {'names': ['firstenergy', 'firstenergy'], 'websites': ['firstenergycorp.com']}}, {'id': 'f56ce84e', 'value': 'FirstEnergy', 'score': 1, 'url': 'https://www.bloomberg.com/research/stocks/private/snapshot.asp?privcapId=293515', 'source': 'company_website[bloomberg_lite]', 'company_website': 'https://www.firstenergycorp.com/content/customer/save_energy/save_energy_pennsylvania.html', 'comparison': {'names': ['firstenergy'], 'websites': []}}, {'id': '8c979202', 'value': 'FirstEnergy', 'score': 1.0, 'source': 'company_website[bane]', 'company_website': 'https://www.firstenergycorp.com/content/customer/save_energy/save_energy_pennsylvania.html', 'comparison': {'names': ['firstenergy'], 'websites': []}}, {'id': '883d944d', 'value': 'FirstEnergy', 'score': 0.844, 'target': 'firstenergy', 'source': 'company_website[fwfm]', 'company_website': 'https://www.firstenergycorp.com/content/customer/save_energy/save_energy_pennsylvania.html', 'comparison': {'names': ['firstenergy'], 'websites': []}}, {'id': '376c54ec', 'value': 'FirstEnergy Careers', 'score': 0.953, 'websites': ['www.firstenergycorp.com'], 'source': 'small_parsers[FacebookParser]', 'from_url': 'https://www.facebook.com/FirstEnergyCareers', 'position': 1, 'comparison': {'names': ['firstenergy careers'], 'websites': ['firstenergycorp.com']}}], 'score': 0.9853554964065552}]}], 'winner': {'id': 'ed7fe1a4', 'value': 'FirstEnergy', 'score': 0.844, 'target': 'firstenergy', 'source': 'fwfm', 'comparison': {'names': ['firstenergy'], 'websites': []}, 'occurs': 6, 'winner_prob': 16.15}, 'aliases': ['FirstEnergy Corp.'], 'alternatives': [], 'ticker': ['FE', 'NYSE', {'source': {'id': '251134ec', 'value': 'FirstEnergy Corp.', 'score': 1, 'wiki_page': 'https://en.wikipedia.org/wiki/FirstEnergy_Corp', 'internal_scores': {'value_in_websites': True, 'references_count': 1, 'title_count': 133, 'article_title_count': 133, 'url_count': 88, 'rank': 0}, 'websites': ['http://firstenergycorp.com'], 'ticker': 'FE', 'exchange': 'NYSE', 'aliases': ['FirstEnergy'], 'source': 'company_website[wikipedia]', 'company_website': 'http://www.firstenergycorp.com/', 'comparison': {'names': ['firstenergy', 'firstenergy'], 'websites': ['firstenergycorp.com']}, 'occurs': 5, 'winner_prob': 10.293}}], 'related': [], 'end_time': '2019-03-21 19:05:27.444058', 'exec_time': '0:00:01.923065', 'request': {'headers': {'Host': 'adg-api.us-east-1.elasticbeanstalk.com', 'Accept': 'application/json', 'Accept-Encoding': 'gzip, deflate', 'Content-Type': 'application/json', 'Forwarded': 'for=10.0.103.55;host=api-2445582026130.production.gw.apicast.io;proto=http', 'User-Agent': 'https://github.com/geneman/altdg', 'X-3Scale-Proxy-Secret-Token': 'rkMcYebOqEIRmpFFm0nGIBex7m8pRVtR', 'X-Configurations': '{"CACHE_ERS": False}', 'X-Forwarded-Host': 'api-2445582026130.production.gw.apicast.io', 'X-Real-Ip': '10.1.13.3', 'X-Forwarded-For': '12.97.143.6, 34.234.193.210', 'X-Forwarded-Port': '80', 'X-Forwarded-Proto': 'http', 'Content-Length': '50', 'Connection': 'keep-alive'}, 'args': {'X_User_Key': '55b99f8d0a9db4915ed41a3ea7e04bb8'}, 'url': 'http://adg-api.us-east-1.elasticbeanstalk.com/merchant-mapper-debug?X_User_Key=55b99f8d0a9db4915ed41a3ea7e04bb8', 'data': 'b\'["EGSFIRST ENERGY CORP  XXXXX0045 XXXXX1630 PA\\\\n"]\'', 'referrer': None, 'remote_addr': '172.30.0.143', 'view_args': {'mode': 'merchant', 'debug': True}, 'configuration': {}}, 'run_uid': '2153730f-6a91-467d-9168-0793703484b1'}}], [{'Original Input': 'PAYPAL *WIZCLIPZ         402-935-7733 FL\n', 'Company Name': 'Wiz Clipz', 'Confidence': 0.68, 'Confidence Level': 'Low', 'Aliases': ['-'], 'Alternative Company Matches': [], 'Related Entities': [], 'Ticker': None, 'Exchange': None, 'Majority Owner': None, 'FIGI': None, '_DEBUG': {'original_input': 'PAYPAL *WIZCLIPZ         402-935-7733 FL\n', 'mode': 'merchant', 'start_time': '2019-03-21 19:05:25.329559', 'attempts': [{'cleaner': 'CcCleaner', 'cleaned': 'WIZCLIPZ', 'cleaned_score': None, 'er_times': {'bane': 0.347, 'fwfm': 1.125, 'company_website': 1.179, 'small_parsers': 1.152, 'bloomberg_lite': 1.177, 'wikipedia': 1.179, 'google_knowledge_graph': 1.169}, 'losers': [{'graph': [{'id': 'db4899b8', 'value': 'Tax Management, Inc.', 'score': 1, 'url': 'https://www.bloomberg.com/research/stocks/private/snapshot.asp?privcapId=4259664', 'source': 'bloomberg_lite', 'comparison': {'names': ['tax management'], 'websites': []}}], 'score': 0.015193906612694263}], 'winners': [{'graph': [{'id': 'bc45850b', 'value': '-', 'score': 1, 'websites': ['wizclipz.com'], 'source': 'company_website[whois]', 'company_website': 'https://wizclipz.com/', 'comparison': {'names': [''], 'websites': ['wizclipz.com']}, 'occurs': 1, 'winner_prob': 0.304}, {'id': '67c6b8ee', 'value': 'Wiz Clipz', 'score': 1, 'websites': ['wizclipz.com'], 'source': 'company_website[copyright]', 'company_website': 'https://wizclipz.com/', 'comparison': {'names': ['wiz clipz'], 'websites': ['wizclipz.com']}, 'occurs': 2, 'winner_prob': 0.735}, {'id': '8c3198ef', 'value': 'Wiz Clipz', 'score': 0.953, 'websites': ['wizclipz.com'], 'source': 'small_parsers[FacebookParser]', 'from_url': 'https://www.facebook.com/wizclipz', 'position': 1, 'comparison': {'names': ['wiz clipz'], 'websites': ['wizclipz.com']}, 'occurs': 2, 'winner_prob': 0.735}], 'score': 0.6837949156761169}]}, {'cleaner': 'MerchantCleaner', 'cleaned': 'PAYPAL *WIZCLIPZ 402-935-7733 FL', 'cleaned_score': None, 'er_times': {'bane': 1.121, 'bloomberg_lite': 1.151, 'company_website': 1.17, 'wikipedia': 1.551, 'google_knowledge_graph': 1.539, 'fwfm': 1.557, 'small_parsers': 1.538}, 'losers': [{'graph': [{'id': '0bb96fbb', 'value': 'Rincon Ltd.', 'score': 1, 'url': 'https://www.bloomberg.com/research/stocks/private/snapshot.asp?privcapId=117275158', 'source': 'bloomberg_lite', 'comparison': {'names': ['rincon'], 'websites': []}}], 'score': 0.015193906612694263}], 'winners': [{'graph': [{'id': '762c4aa4', 'value': 'PayPal', 'score': 0.122, 'source': 'bane', 'comparison': {'names': ['paypal'], 'websites': []}}, {'id': '30d5947c', 'value': 'Privacy', 'score': 1, 'websites': ['paypal.com'], 'source': 'company_website[copyright]', 'company_website': 'https://www.paypal.com/us/smarthelp/article/why-is-the-number-402-935-7733-showing-on-my-bank-or-credit-card-statement-faq3722', 'comparison': {'names': ['privacy'], 'websites': ['paypal.com']}}, {'id': 'f4c0e5e4', 'value': 'PayPal, Inc.', 'score': 1, 'websites': ['paypal.com'], 'source': 'company_website[ssl]', 'company_website': 'https://www.paypal.com/us/smarthelp/article/why-is-the-number-402-935-7733-showing-on-my-bank-or-credit-card-statement-faq3722', 'comparison': {'names': [''], 'websites': ['paypal.com']}}, {'id': '079a1293', 'value': 'PayPal', 'score': 1, 'url': 'https://www.bloomberg.com/research/stocks/private/snapshot.asp?privcapId=112732', 'source': 'company_website[bloomberg_lite]', 'company_website': 'https://www.paypal.com/us/smarthelp/article/why-is-the-number-402-935-7733-showing-on-my-bank-or-credit-card-statement-faq3722', 'comparison': {'names': ['paypal'], 'websites': []}}, {'id': '679db651', 'value': 'PayPal Holdings Inc.', 'score': 1, 'wiki_page': 'https://en.wikipedia.org/wiki/Paypal', 'internal_scores': {'value_in_websites': True, 'references_count': 1, 'title_count': 289, 'article_title_count': 289, 'url_count': 77, 'rank': 0}, 'websites': ['https://www.paypal.com'], 'ticker': 'PYPL', 'exchange': 'NASDAQ', 'aliases': ['PayPal'], 'source': 'company_website[wikipedia]', 'company_website': 'https://www.paypal.com/us/smarthelp/article/why-is-the-number-402-935-7733-showing-on-my-bank-or-credit-card-statement-faq3722', 'comparison': {'names': ['holdings', 'paypal'], 'websites': ['paypal.com']}}, {'id': 'ed69c336', 'value': 'Braintree', 'score': None, 'wiki_page': 'https://en.wikipedia.org/wiki/Braintree_(company)', 'source': 'company_website[wikipedia]', 'company_website': 'https://www.paypal.com/us/smarthelp/article/why-is-the-number-402-935-7733-showing-on-my-bank-or-credit-card-statement-faq3722', 'comparison': {'names': ['braintree'], 'websites': []}, 'parent': [['679db651', 'company_website[wikipedia]', 'PayPal Holdings Inc.']]}, {'id': 'd318cc2d', 'value': 'Paydiant', 'score': None, 'wiki_page': 'https://en.wikipedia.org/wiki/Paydiant', 'source': 'company_website[wikipedia]', 'company_website': 'https://www.paypal.com/us/smarthelp/article/why-is-the-number-402-935-7733-showing-on-my-bank-or-credit-card-statement-faq3722', 'comparison': {'names': ['paydiant'], 'websites': []}, 'parent': [['679db651', 'company_website[wikipedia]', 'PayPal Holdings Inc.']]}, {'id': '1de4432a', 'value': 'Venmo', 'score': None, 'wiki_page': 'https://en.wikipedia.org/wiki/Venmo', 'source': 'company_website[wikipedia]', 'company_website': 'https://www.paypal.com/us/smarthelp/article/why-is-the-number-402-935-7733-showing-on-my-bank-or-credit-card-statement-faq3722', 'comparison': {'names': ['venmo'], 'websites': []}, 'parent': [['679db651', 'company_website[wikipedia]', 'PayPal Holdings Inc.']]}, {'id': 'a446c842', 'value': 'PayPal Credit', 'score': None, 'wiki_page': 'https://en.wikipedia.org/wiki/PayPal_Credit', 'source': 'company_website[wikipedia]', 'company_website': 'https://www.paypal.com/us/smarthelp/article/why-is-the-number-402-935-7733-showing-on-my-bank-or-credit-card-statement-faq3722', 'comparison': {'names': [''], 'websites': []}, 'parent': [['679db651', 'company_website[wikipedia]', 'PayPal Holdings Inc.']]}, {'id': '12c77120', 'value': 'Xoom Corporation', 'score': None, 'wiki_page': 'https://en.wikipedia.org/wiki/Xoom_Corporation', 'source': 'company_website[wikipedia]', 'company_website': 'https://www.paypal.com/us/smarthelp/article/why-is-the-number-402-935-7733-showing-on-my-bank-or-credit-card-statement-faq3722', 'comparison': {'names': ['xoom'], 'websites': []}, 'parent': [['679db651', 'company_website[wikipedia]', 'PayPal Holdings Inc.']]}, {'id': '40f5bb09', 'value': 'TIO Networks', 'score': None, 'source': 'company_website[wikipedia]', 'company_website': 'https://www.paypal.com/us/smarthelp/article/why-is-the-number-402-935-7733-showing-on-my-bank-or-credit-card-statement-faq3722', 'comparison': {'names': ['tio networks'], 'websites': []}, 'parent': [['679db651', 'company_website[wikipedia]', 'PayPal Holdings Inc.']]}, {'id': '68f72d19', 'value': 'card.io', 'score': None, 'source': 'company_website[wikipedia]', 'company_website': 'https://www.paypal.com/us/smarthelp/article/why-is-the-number-402-935-7733-showing-on-my-bank-or-credit-card-statement-faq3722', 'comparison': {'names': ['cardio'], 'websites': []}, 'parent': [['679db651', 'company_website[wikipedia]', 'PayPal Holdings Inc.']]}, {'id': '5267767c', 'value': 'iZettle', 'score': None, 'wiki_page': 'https://en.wikipedia.org/wiki/IZettle', 'source': 'company_website[wikipedia]', 'company_website': 'https://www.paypal.com/us/smarthelp/article/why-is-the-number-402-935-7733-showing-on-my-bank-or-credit-card-statement-faq3722', 'comparison': {'names': ['izettle'], 'websites': []}, 'parent': [['679db651', 'company_website[wikipedia]', 'PayPal Holdings Inc.']]}, {'id': '0a108c7d', 'value': 'Tradera', 'score': None, 'wiki_page': 'https://en.wikipedia.org/wiki/Tradera', 'source': 'company_website[wikipedia]', 'company_website': 'https://www.paypal.com/us/smarthelp/article/why-is-the-number-402-935-7733-showing-on-my-bank-or-credit-card-statement-faq3722', 'comparison': {'names': ['tradera'], 'websites': []}, 'parent': [['679db651', 'company_website[wikipedia]', 'PayPal Holdings Inc.']]}, {'id': '46386bbc', 'value': 'PayPal', 'score': 1.0, 'source': 'company_website[bane]', 'company_website': 'https://www.paypal.com/us/smarthelp/article/why-is-the-number-402-935-7733-showing-on-my-bank-or-credit-card-statement-faq3722', 'comparison': {'names': ['paypal'], 'websites': []}}, {'id': 'c884307f', 'value': 'PayPal', 'score': 0.844, 'target': 'paypal', 'aliases': ['PayPal Credit'], 'source': 'company_website[fwfm]', 'company_website': 'https://www.paypal.com/us/smarthelp/article/why-is-the-number-402-935-7733-showing-on-my-bank-or-credit-card-statement-faq3722', 'comparison': {'names': ['paypal', ''], 'websites': []}}, {'id': '95c713c9', 'value': 'PayPal', 'score': 0.82, 'target': 'paypal', 'aliases': ['PayPal Credit'], 'source': 'fwfm', 'comparison': {'names': ['paypal', ''], 'websites': []}}], 'score': 0.9846509695053101}]}], 'winner': {'id': '67c6b8ee', 'value': 'Wiz Clipz', 'score': 1, 'websites': ['wizclipz.com'], 'source': 'company_website[copyright]', 'company_website': 'https://wizclipz.com/', 'comparison': {'names': ['wiz clipz'], 'websites': ['wizclipz.com']}, 'occurs': 2, 'winner_prob': 0.735}, 'aliases': ['-'], 'alternatives': [], 'ticker': [None, None, {}], 'related': [], 'end_time': '2019-03-21 19:05:27.116721', 'exec_time': '0:00:01.787162', 'request': {'headers': {'Host': 'adg-api.us-east-1.elasticbeanstalk.com', 'Accept': 'application/json', 'Accept-Encoding': 'gzip, deflate', 'Content-Type': 'application/json', 'Forwarded': 'for=10.0.103.55;host=api-2445582026130.production.gw.apicast.io;proto=http', 'User-Agent': 'https://github.com/geneman/altdg', 'X-3Scale-Proxy-Secret-Token': 'rkMcYebOqEIRmpFFm0nGIBex7m8pRVtR', 'X-Configurations': '{"CACHE_ERS": False}', 'X-Forwarded-Host': 'api-2445582026130.production.gw.apicast.io', 'X-Real-Ip': '10.1.26.2', 'X-Forwarded-For': '12.97.143.6, 54.175.240.103', 'X-Forwarded-Port': '80', 'X-Forwarded-Proto': 'http', 'Content-Length': '46', 'Connection': 'keep-alive'}, 'args': {'X_User_Key': '55b99f8d0a9db4915ed41a3ea7e04bb8'}, 'url': 'http://adg-api.us-east-1.elasticbeanstalk.com/merchant-mapper-debug?X_User_Key=55b99f8d0a9db4915ed41a3ea7e04bb8', 'data': 'b\'["PAYPAL *WIZCLIPZ         402-935-7733 FL\\\\n"]\'', 'referrer': None, 'remote_addr': '172.30.1.83', 'view_args': {'mode': 'merchant', 'debug': True}, 'configuration': {}}, 'run_uid': '295dce26-56da-497f-8367-749d529d37d7'}}], [{'Original Input': 'FIVE GUYS OK-13          NORMAN       OK~~08888~~5109820335830345~~46204~~0~~~~0079\n', 'Company Name': 'Five Guys', 'Confidence': 0.98, 'Confidence Level': 'High', 'Aliases': ['Five Guys Enterprises, LLC'], 'Alternative Company Matches': [], 'Related Entities': [], 'Ticker': None, 'Exchange': None, 'Majority Owner': None, 'FIGI': None, '_DEBUG': {'original_input': 'FIVE GUYS OK-13          NORMAN       OK~~08888~~5109820335830345~~46204~~0~~~~0079\n', 'mode': 'merchant', 'start_time': '2019-03-21 19:05:25.324941', 'attempts': [{'cleaner': 'CcCleaner', 'cleaned': 'FIVE GUYS OK-13', 'cleaned_score': None, 'er_times': {'company_website': 1.165, 'bane': 1.17, 'google_knowledge_graph': 1.155, 'bloomberg_lite': 1.159, 'fwfm': 1.559, 'small_parsers': 1.543, 'wikipedia': 1.56}, 'losers': [{'graph': [{'id': '02116961', 'value': 'Tax Management, Inc.', 'score': 1, 'url': 'https://www.bloomberg.com/research/stocks/private/snapshot.asp?privcapId=4259664', 'source': 'bloomberg_lite', 'comparison': {'names': ['tax management'], 'websites': []}}], 'score': 0.015193906612694263}], 'winners': [{'graph': [{'id': '7f028fef', 'value': 'Five Guys', 'score': 0.824, 'target': 'five guys', 'source': 'fwfm', 'comparison': {'names': ['five guys'], 'websites': []}, 'occurs': 3, 'winner_prob': 2.248}, {'id': 'd21b896f', 'value': 'Five Guys', 'score': 1.015, 'source': 'small_parsers[YelpParser]', 'from_url': 'https://www.yelp.com/biz/five-guys-broken-arrow', 'position': 0, 'comparison': {'names': ['five guys'], 'websites': []}, 'occurs': 3, 'winner_prob': 1.998}, {'id': '7350e279', 'value': 'Five Guys', 'score': 0.953, 'source': 'small_parsers[YelpParser]', 'from_url': 'https://www.yelp.com/biz/five-guys-oklahoma-city-2?start=20', 'position': 1, 'comparison': {'names': ['five guys'], 'websites': []}, 'occurs': 3, 'winner_prob': 1.998}, {'id': '4355dd7b', 'value': 'Five Guys Enterprises, LLC', 'score': 1, 'wiki_page': 'https://en.wikipedia.org/wiki/Five_Guys', 'internal_scores': {'references_count': 0, 'title_count': 30, 'article_title_count': 30, 'url_count': 0, 'rank': 0}, 'websites': ['http://fiveguys.com'], 'ticker': None, 'exchange': None, 'aliases': ['Five Guys'], 'source': 'wikipedia', 'comparison': {'names': ['five guys', 'five guys'], 'websites': ['fiveguys.com']}, 'occurs': 1, 'winner_prob': 0.804}], 'score': 0.978506863117218}]}, {'cleaner': 'MerchantCleaner', 'cleaned': 'FIVE GUYS OK-13 NORMAN OK~~08888~~5109820335830345~~46204~~0~~~~0079', 'cleaned_score': None, 'er_times': {'fwfm': 0.35, 'wikipedia': 0.52, 'company_website': 0.53, 'bloomberg_lite': 0.713, 'bane': 0.951, 'google_knowledge_graph': 1.18, 'small_parsers': 1.176}, 'losers': [{'graph': [{'id': '03a3ddc1', 'value': 'CCC Information Services', 'score': 1, 'url': 'https://www.bloomberg.com/research/stocks/private/snapshot.asp?privcapId=6860584', 'source': 'bloomberg_lite', 'comparison': {'names': ['information services'], 'websites': []}}], 'score': 0.015193906612694263}], 'winners': [{'graph': [{'id': '63c4cbfd', 'value': 'Five Guys', 'score': 0.156, 'target': 'five guys', 'source': 'fwfm', 'comparison': {'names': ['five guys'], 'websites': []}}, {'id': '71cce9dd', 'value': 'Five Guys', 'score': None, 'websites': ['https://order.fiveguys.com/menu/norman'], 'company_website': 'https://order.fiveguys.com/menu/norman', 'source': 'company_website', 'comparison': {'names': ['five guys'], 'websites': ['order.fiveguys.com']}}, {'id': 'eed3d6dc', 'value': 'Five Guys', 'score': 1, 'url': 'https://www.bloomberg.com/research/stocks/private/snapshot.asp?privcapId=20703352', 'source': 'company_website[bloomberg_lite]', 'company_website': 'https://order.fiveguys.com/menu/norman', 'comparison': {'names': ['five guys'], 'websites': []}}, {'id': '2cc98d13', 'value': 'Five Guys Enterprises, LLC', 'score': 1, 'wiki_page': 'https://en.wikipedia.org/wiki/Five_Guys', 'internal_scores': {'value_in_websites': False, 'references_count': 0, 'title_count': 160, 'article_title_count': 160, 'url_count': 39, 'rank': 0}, 'websites': ['http://fiveguys.com'], 'ticker': None, 'exchange': None, 'aliases': ['Five Guys'], 'source': 'company_website[wikipedia]', 'company_website': 'https://order.fiveguys.com/menu/norman', 'comparison': {'names': ['five guys', 'five guys'], 'websites': ['fiveguys.com']}}, {'id': 'd41d95cc', 'value': 'Five Guys Enterprises', 'score': 1, 'websites': ['order.fiveguys.com'], 'source': 'company_website[whois]', 'company_website': 'https://order.fiveguys.com/menu/norman', 'comparison': {'names': ['five guys'], 'websites': ['order.fiveguys.com']}}, {'id': '89b93d20', 'value': 'Five Guys', 'score': 0.793, 'source': 'company_website[bane]', 'company_website': 'https://order.fiveguys.com/menu/norman', 'comparison': {'names': ['five guys'], 'websites': []}}, {'id': 'f1ddfb7c', 'value': 'Five Guys', 'score': 0.844, 'target': 'five guys', 'source': 'company_website[fwfm]', 'company_website': 'https://order.fiveguys.com/menu/norman', 'comparison': {'names': ['five guys'], 'websites': []}}, {'id': '277a272c', 'value': 'Five Guys', 'score': 0.169, 'source': 'bane', 'comparison': {'names': ['five guys'], 'websites': []}}, {'id': 'a0b5f29e', 'value': 'Five Guys', 'score': 0.598, 'source': 'small_parsers[MapquestParser]', 'from_url': 'https://www.mapquest.com/us/oklahoma/five-guys-284086630', 'position': 8, 'comparison': {'names': ['five guys'], 'websites': []}}, {'id': '9e721fdd', 'value': 'Five Guys', 'score': 0.518, 'source': 'small_parsers[MapquestParser]', 'from_url': 'https://www.mapquest.com/us/oklahoma/five-guys-356622544', 'position': 10, 'comparison': {'names': ['five guys'], 'websites': []}}, {'id': '6871af97', 'value': 'Five Guys', 'score': 0.296, 'source': 'small_parsers[YahooLocalParser]', 'from_url': 'https://local.yahoo.com/info-137365792-five-guys-tulsa', 'position': 17, 'comparison': {'names': ['five guys'], 'websites': []}}, {'id': 'd9556983', 'value': 'Five Guys', 'score': 0.27, 'source': 'small_parsers[YelpParser]', 'from_url': 'https://www.yelp.com/biz/five-guys-broken-arrow', 'position': 18, 'comparison': {'names': ['five guys'], 'websites': []}}], 'score': 0.9489613771438599}]}], 'winner': {'id': '7f028fef', 'value': 'Five Guys', 'score': 0.824, 'target': 'five guys', 'source': 'fwfm', 'comparison': {'names': ['five guys'], 'websites': []}, 'occurs': 3, 'winner_prob': 2.248}, 'aliases': ['Five Guys Enterprises, LLC'], 'alternatives': [], 'ticker': [None, None, {}], 'related': [], 'end_time': '2019-03-21 19:05:27.473957', 'exec_time': '0:00:02.149016', 'request': {'headers': {'Host': 'adg-api.us-east-1.elasticbeanstalk.com', 'Accept': 'application/json', 'Accept-Encoding': 'gzip, deflate', 'Content-Type': 'application/json', 'Forwarded': 'for=10.0.103.55;host=api-2445582026130.production.gw.apicast.io;proto=http', 'User-Agent': 'https://github.com/geneman/altdg', 'X-3Scale-Proxy-Secret-Token': 'rkMcYebOqEIRmpFFm0nGIBex7m8pRVtR', 'X-Configurations': '{"CACHE_ERS": False}', 'X-Forwarded-Host': 'api-2445582026130.production.gw.apicast.io', 'X-Real-Ip': '10.1.26.2', 'X-Forwarded-For': '12.97.143.6, 34.231.171.211', 'X-Forwarded-Port': '80', 'X-Forwarded-Proto': 'http', 'Content-Length': '89', 'Connection': 'keep-alive'}, 'args': {'X_User_Key': '55b99f8d0a9db4915ed41a3ea7e04bb8'}, 'url': 'http://adg-api.us-east-1.elasticbeanstalk.com/merchant-mapper-debug?X_User_Key=55b99f8d0a9db4915ed41a3ea7e04bb8', 'data': 'b\'["FIVE GUYS OK-13          NORMAN       OK~~08888~~5109820335830345~~46204~~0~~~~0079\\\\n"]\'', 'referrer': None, 'remote_addr': '172.30.3.39', 'view_args': {'mode': 'merchant', 'debug': True}, 'configuration': {}}, 'run_uid': '81a13fcb-8888-4b56-90d6-9f427e019ea3'}}]]

                if any(self.has_wrong_key(response[0]['Company Name']) for response in list_json_response):
                    logger.info(f"wrong key")
                    sys.exit()

                if any(self.has_key_limit(response[0]['Company Name']) for response in list_json_response):
                    had_timeout = True
                    if num_threads > 4:
                        num_threads -= 2
                    elif num_threads > 1:
                        num_threads -= 1
                    logger.info(f"Number of threads in the chunk after number of requests limitation error for key: {num_threads}")
                    logger.info(f"Continue in {self.N_sec_sleep_key_limit} seconds")
                    time.sleep(self.N_sec_sleep_key_limit)

                if any(self.has_timeout(response[0]['Company Name']) for response in list_json_response):
                    had_timeout = True
                    if num_threads > 4:
                        num_threads -= 2
                    elif num_threads > 1:
                        num_threads -= 1
                    logger.info(f"Number of threads in the chunk after timeout error: {num_threads}")

                if not any(self.has_timeout(response[0]['Company Name']) or self.has_key_limit(response[0]['Company Name']) for response in list_json_response):
                    break

            if had_timeout:
                chunks_without_timeout_streak = 0
            else:
                chunks_without_timeout_streak += 1

            if chunks_without_timeout_streak >= self.N_const_thread:
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
#
# def is_valid_file(parser, arg):
#     if not os.path.exists(arg):
#         parser.error("The input file %s does not exist." % arg)
#     else:
#         return open(arg, 'r')

if __name__ == '__main__':

    import sys

    if float(sys.version[:3]) < 3.6:
        sys.exit("This script requires Python version 3.6")

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
    # parser.add_argument(help='Path to input file', dest='in_file', type=lambda x: is_valid_file(parser, x))
    parser.add_argument(help='Path to input file', dest='in_file')
    args = parser.parse_args()

    if not args.out_file:
        in_file_dir, in_file_name = os.path.split(args.in_file)
        args.out_file = os.path.join(
            in_file_dir,
            f'{os.path.splitext(in_file_name)[0]}-{now_str}.csv'
        )

    Mapper(**vars(args)).bulk()