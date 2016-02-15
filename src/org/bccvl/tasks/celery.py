from __future__ import absolute_import

from ConfigParser import SafeConfigParser
import os
import logging
import logging.config
from celery import Celery
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
    if not os.path.exists(os.environ.get('BCCVL_CONFIG')):
        print os.environ.get('BCCVL_CONFIG')
        return {}
    ini = SafeConfigParser()
    ini.read([os.environ.get('BCCVL_CONFIG')])
    config = {}
    if ini.has_section('cookie'):
        config['cookie'] = dict(ini.items('cookie'))
        if ini.has_option('cookie', 'secure'):
            config['cookie']['secure'] = ini.getboolean('cookie', 'secure')
        else:
            config['cookie']['secure'] = True

    if ini.has_section('ssl'):
        config['ssl'] = {}
        if ini.has_option('ssl', 'verify'):
            config['ssl']['verify'] = ini.getboolean('ssl', 'verify')
        else:
            config['ssl']['verify'] = True

    if ini.has_section('swift'):
        config['swift'] = {}
        config['swift'] = dict(ini.items('swift'))

    if ini.has_section('oauth'):
        config['oauth'] = {}
        for (key, value) in ini.items['oauth']:
            provider, key = key.split('_', 1)
            if provider not in config['oauth']:
                config['oauth'][provider] = {}
            config['oauth'][provider][key] = value

    if ini.has_section('sentry'):
        config['sentry'] = dict(ini.items('sentry'))
    return {'bccvl': config}


@after_setup_logger.connect
def my_logging(sender, signal, logger, loglevel, logfile, format, colorize):
    # tweak root logger according to logging config from json file
    if not os.path.exists(os.environ.get('BCCVL_CONFIG')):
        return
    # read logging config from ini file
    # remove current handlers
    logger.handlers = []
    logging.config.fileConfig(os.environ['BCCVL_CONFIG'], disable_existing_loggers=False)


app = Celery()
# load common package bundled settings
app.config_from_object('org.bccvl.tasks.celeryconfig')
app.config_from_envvar('CELERY_CONFIG_MODULE', silent=True)
app.conf.update(read_ini_file())
# check sentry config
if 'sentry' in app.conf['bccvl']:
    # configure sentry logging
    from raven import Client
    from raven.contrib.celery import register_signal, register_logger_signal
    client = Client(str(app.conf['bccvl']['sentry']['dsn']))
    # register a custom filter to filter out duplicate logs
    register_logger_signal(client)
    # hook into the Celery error handler
    register_signal(client)
    # The register_logger_signal function can also take an optional argument
    # `loglevel` which is the level used for the handler created.
    # Defaults to `logging.ERROR`
    register_logger_signal(client, loglevel=logging.INFO)
