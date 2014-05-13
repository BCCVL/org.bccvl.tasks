from __future__ import absolute_import

import json
import os
import logging
from importlib import import_module
from celery import Celery
from celery.app.defaults import NAMESPACES
from celery.signals import after_setup_logger

LOG = logging.getLogger(__name__)
# TODO: early startup logs are usually not printed?
#       need to setup logging as soon as possible?


def read_ini_file():
    """
    Looks for environment variable CELER_JSON_CONFIG which should
    point to a json file. This file is read and used as configuration
    for the current celery process.
    """
    jsonfile = os.environ.get('CELERY_JSON_CONFIG')
    if jsonfile is None:
        LOG.warn('Environment variable CELERY_JSON_CONFIG not set. using '
                 'default settings')
        return {}
    try:
        # try to parse ini file
        jsonconfig = json.load(open(jsonfile, 'r'))
    except Exception as e:
        print e
        LOG.warn('Could not read config file %s: %s',
                 jsonfile, e)
        return {}
    return jsonconfig


def parse_celery_config(jsonconfig):
    # extract all celery related configurations and return
    # as "flat" dictionary
    config = {}
    for key, value in NAMESPACES.items():
        # config parser converts all options to lower case

        if isinstance(value, dict) and key in jsonconfig:
            # found a section
            for subkey, option in value.items():
                if subkey in jsonconfig[key]:
                    confkey = '{0}_{1}'.format(key, subkey)
                    # TODO: there is a bug in celery config where to_ptyhon
                    #       for type='list' fails. however, json should just
                    #       parse fine
                    #confval = option.to_python(jsonconfig[key][subkey])
                    #config[confkey] = confval
                    config[confkey] = jsonconfig[key][subkey]
        else:
            if key in jsonconfig:
                # config[key] = value.to_python(jsonconfig[key])
                config[key] = jsonconfig[key]

    return config


def resolve(dottedname):
    modname, modattr = dottedname.rsplit('.', 1)
    module = import_module(modname)
    return getattr(module, modattr)


@after_setup_logger.connect
def my_logging(sender, signal, logger, loglevel, logfile, format, colorize):
    # tweak root logger according to logging config from json file
    logconf = jsonconfig.get('logging', {}).copy()
    if not logconf:
        return
    level = logconf.pop('level', loglevel) or logging.INFO
    klass = logconf.pop('class', None)
    if klass:
        # create new handler
        cls = resolve(klass)
        handler = cls(**logconf)
        # remove all old handlers
        logger.handlers = []
        # add new handler
        logger.addHandler(handler)
    logger.setLevel(level)


app = Celery()
jsonconfig = read_ini_file()
app.conf.update(parse_celery_config(jsonconfig))
