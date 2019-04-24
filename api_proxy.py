#!/usr/bin/env python

import logging
import logging.config
import requests
import requests.auth
import json
from datetime import datetime, timedelta
import pandas as pd

import lib

class Velocity_Client(object):
    def __init__(self):
        logging.config.fileConfig("./config/logging.config")
        logging.info("Created instance of API Proxy")
        self.client_id = lib.get_config("client_id")
        self.token_type, self.token, self.token_expiry = self.create_auth_token()

    def is_token_valid(self):
        '''
        Check if the existing token has already expired
        :return: boolean
        '''
        return datetime.now() < self.token_expiry

    def refresh_token(self):
        '''
        Refresh token before sending request
        :return: void
        '''
        if not self.is_token_valid():
            logging.info("Old token expired. Requesting for new token")
            self.token_type, self.token, self.token_expiry = self.create_auth_token()

    def request(self, api_url, headers, payload, method="GET"):
        '''
        Generic request method for Historical, Metadata, Tag Browser, Tag List
        :param api_url: string
        :param headers: dict
        :param payload: dict
        :param method: string (default = GET)
        :return: dict
        '''
        if method == "GET":
            response = requests.get(api_url, headers=headers, data=payload)
        else:
            response = requests.post(api_url, headers=headers, data=payload)
        logging.info("response: %s" % response.text)
        data = json.loads(response.text)
        return data


    def create_auth_token(self):
        '''
        Request for authorization token
        :return: string, string, datetime.datetime
        '''
        logging.info("Sending request for token")

        # retrieve API call configs from config file
        client_secret = lib.get_config("client_secret")
        api_url = lib.get_config("token_api_url")

        # construct data that passes to the POST request
        payload = {"grant_type": "client_credentials", "scope": "/api", "client_id": self.client_id,
                   "client_secret": client_secret}
        headers = {"accept": "application/json", "content-type": "application/x-www-form-urlencoded"}

        token = None
        try:
            data = self.request(api_url, headers, payload, method="POST")
            token_type, token, expires_in = data["token_type"], data["access_token"], data["expires_in"]
            # the token is valid for one hour only, calculate the expiry time
            token_expiry = datetime.now() + timedelta(seconds=expires_in - 1)
            logging.info("This new token will be expired at %s" % token_expiry)
        except:
            logging.exception("Error when retriving token")
        return token_type, token, token_expiry

    def retrieve_historical_data(self, start_date, end_date, tags, start_time=None, end_time=None,
                                 frequency="DAILY", price_points="C", latest_only=False):
        '''
        Request for the historical time series within given time period, frequency and price point
        :param start_date: int (e.g. 20181107)
        :param end_date: int (e.g. 20181107)
        :param tags: list (size of 1 to 100 distinct tags)
        :param start_time: int (e.g. hhmm, two digit (24) hour * 100 + two digit minute)
        :param end_time: int (e.g. hhmm, two digit (24) hour * 100 + two digit minute)
        :param frequency: string (MONTHLY, WEEKLY, DAILY (default), HOURLY, MI10, MI01)
        :param price_points: string (C (default), OHLC)
        :param latest_only: boolean (default = False)
        :return: dict
        '''
        self.refresh_token()
        api_url = lib.get_config("historical_data_api_url") + "?client_id=" + self.client_id
        headers = {"accept": "application/json", "content-type": "application/json",
                   "authorization": self.token_type.capitalize() + " " + self.token}
        payload = json.dumps({"startDate": start_date, "endDate": end_date,
                              "tags": tags, "startTime": start_time, "endTime": end_time,
                              "frequency": frequency, "pricePoints": price_points, "latestOnly": latest_only})

        logging.info("Requesting Historical Data: %s" % payload)
        data = self.request(api_url, headers, payload, method="POST")
        return data

    def retrieve_metadata(self, tags, frequency="EOD"):
        '''
        Request for Metadata for a tag or group of tags
        :param tags: list
        :param frequency: string (EOD (default), INTRADAY)
        :return: dict
        '''
        self.refresh_token()
        api_url = lib.get_config("metadata_api_url") + "?client_id=" + self.client_id
        headers = {"accept": "application/json", "content-type": "application/json",
                   "authorization": self.token_type.capitalize() + " " + self.token}
        payload = json.dumps({"tags": tags, "frequency": frequency})

        logging.info("Requesting Metadata: %s" % payload)
        data = self.request(api_url, headers, payload, method="POST")
        return data

    def retrieve_tag_browser(self, prefix):
        '''
        Request for Tag tree structure at the prefix level. prefix = "" means root level
        :param prefix: string
        :return: dict
        '''
        self.refresh_token()
        api_url = lib.get_config("tag_browser_api_url") + "?client_id=" + self.client_id
        headers = {"accept": "application/json", "content-type": "application/json",
                   "authorization": self.token_type.capitalize() + " " + self.token}
        payload = json.dumps({"prefix": prefix})

        logging.info("Requesting Tag Browser: %s" % payload)
        data = self.request(api_url, headers, payload, method="POST")
        return data

    def retrieve_tag_list(self, prefix, regex=".*"):
        '''
        Request for list of tags that belongs to the prefix tag
        :param prefix: string (e.g. COMMODITIES.SPOT.SPOT_GOLD, needs at least the first two levels, namely
        Category and Sub Category in the Data Browser)
        :param regex: string (e.g. .*(GOLD|SILVER), default returns everything)
        :return: dict
        '''
        self.refresh_token()
        api_url = lib.get_config("tag_list_api_url") + "?client_id=" + self.client_id
        headers = {"accept": "application/json", "content-type": "application/json",
                   "authorization": self.token_type.capitalize() + " " + self.token}
        payload = json.dumps({"prefix": prefix, "regex": regex})

        logging.info("Requesting Tag List: %s" % payload)
        data = self.request(api_url, headers, payload, method="POST")
        return data

    def retrieve_all_tags(self, tag=""):
        '''
        Return all tags that are available via API call
        :param tag: string (default = '' the root tag)
        :return: list
        '''
        tag_list = []
        # check which level is given in tag because if level <= 1, then needs to construct tag in order to call
        # retrieve_tag_list, which requires at least 2 levels
        tag_level = len([t for t in tag.split(".") if t != ""])
        if tag_level < 2:
            root_data = self.retrieve_tag_browser(tag)
            category = root_data["fields"].keys()
            if tag_level == 0:
                for c in category:
                    data = self.retrieve_tag_browser(c)
                    sub_category = [c + "." + field for field in data["fields"].keys()]
            else:
                sub_category = [tag + "." + c for c in category]
            tag_list += sub_category

        # retrieve the tag list from each tag (category + sub_category combo) or the tag parameter
        res = []
        logging.info("tag_list: %s" % tag_list)
        for t in tag_list:
            data = self.retrieve_tag_list(t)
            res += data["tags"]
        return res

    def dump_time_series_to_df(self, series):
        '''
        Take in a list of tags and retrieve the time series and put into dict of DataFrame
        :param series: list
        :return: dict
        '''
        # extract metadata because startDate and endDate of the series are needed
        metadata = self.retrieve_metadata(series)
        body = metadata['body']
        tags = body.keys()
        series_dict = dict()
        for tag in tags:
            startDate = body[tag]['startDate']
            endDate = body[tag]['endDate']
            # historical API can take tag list of size 100 but in this case, we make API call tag by tag
            # due to date inconsistency. Otherwise, we should call the historical data API with
            # as many tags as possible to minimize the counting limits
            data = self.retrieve_historical_data(start_date=startDate, end_date=endDate, tags=[tag])
            df = pd.DataFrame.from_dict(data['body'][tag])
            df['x'] = pd.to_datetime(df['x'], format='%Y%m%d')
            df.drop('type', inplace=True, axis=1)
            df.rename(columns={'c': tag, 'x': 'time'}, inplace=True)
            series_dict[tag] = df
        return series_dict