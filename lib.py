#!/usr/bin/env python
import yaml
import logging
import pandas as pd

CONFIGS = None

def load_config(config_file="./config/config.yaml"):
    global CONFIGS
    if not CONFIGS:
        try:
            logging.info('Attempting to load config file from path: %s' % (config_file))
            CONFIGS = yaml.load(open(config_file))
        except IOError:
            logging.warning('Unable to open config file.')
    return CONFIGS

def get_config(config_name):
    if not CONFIGS:
        load_config()
    return CONFIGS[config_name]


def merge_all_dfs(dfs):
    from functools import reduce
    return reduce(lambda left, right: pd.merge(left, right, how='outer', on='time'), dfs)