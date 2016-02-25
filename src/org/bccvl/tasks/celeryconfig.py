import os

BROKER_URL = os.environ.get('BROKER_URL')
ADMINS = os.environ.get('ADMINS')
CELERY_TIMEZONE = os.environ.get('CELERY_TIMEZONE')
CELERYD_CONCURRENCY = os.environ.get('CELERYD_CONCURRENCY')
BROKER_USE_SSL = {}
for key in ('CA_CERTS', 'KEYFILE', 'CERTFILE', 'CERT_REQS'):
    envvar = 'BROCKER_USE_SSL_{}'.format(key)
    if envvar not in os.environ:
        continue
    if key in 'CERT_REQS':
        # convert to int?
        BROKER_USE_SSL[key.lower()] = int(os.environ[envvar])
    else:
        BROKER_USE_SSL[key.lower()] = os.environ[envvar]

CELERY_ENABLE_UTC = True


CELERY_ALWAYS_EAGER = False

CELERY_ACKS_LATE = True

CELERY_ACCEPT_CONTENT = ["json", "msgpack", "yaml"]
CELERY_TASK_SERIALIZER = "json"

CELERY_DEFAULT_ROUTING_KEY = "default"
CELERY_DEFAULT_QUEUE = "default"
CELERY_DEFAULT_EXCHANGE = "default"
CELERY_DEFAULT_EXCHANGE_TYPE = "direct"

CELERY_IMPORTS = []

CELERY_IGNORE_RESULT = True

#CELERY_RESULT_BACKEND = amqp
#CELERY_RESULT_SERIALIZER = "json"
#CELERY_TASK_RESULT_EXPIRES = 3600

CELERY_ROUTES = [
    {
        "org.bccvl.tasks.plone.import_ala": {
            "queue": "plone",
            "routing_key": "plone"
        }
    },
    {
        "org.bccvl.tasks.plone.import_cleanup": {
            "queue": "plone",
            "routing_key": "plone"
        }
    },
    {
        "org.bccvl.tasks.plone.import_file_metadata": {
            "queue": "plone",
            "routing_key": "plone"
        }
    },
    {
        "org.bccvl.tasks.plone.import_result": {
            "queue": "plone",
            "routing_key": "plone"
        }
    },
    {
        "org.bccvl.tasks.plone.set_progress": {
            "queue": "plone",
            "routing_key": "plone"
        }
    },
    {
        "org.bccvl.tasks.datamover.move": {
            "queue": "datamover",
            "routing_key": "datamover"
        }
    },
    {
        "org.bccvl.tasks.datamover.move": {
            "queue": "datamover",
            "routing_key": "datamover"
        }
    },
    {
        "org.bccvl.tasks.datamover.pull_occurrences_from_ala": {
            "queue": "datamover",
            "routing_key": "datamover"
        }
    },
    {
        "org.bccvl.tasks.datamover.pull_occurrences_from_gbif": {
            "queue": "datamover",
            "routing_key": "datamover"
        }
    },
    {
        "org.bccvl.tasks.datamover.import_multi_species_csv": {
            "queue": "datamover",
            "routing_key": "datamover"
        }
    },
    {
        "org.bccvl.tasks.datamover.update_metadata": {
            "queue": "datamover",
            "routing_key": "datamover"
        }
    },
    {
        "org.bccvl.tasks.export_services.export_result": {
            "queue": "datamover",
            "routing_key": "datamover"
        }
    },
    {
        "org.bccvl.tasks.compute.r_task": {
            "queue": "worker",
            "routing_key": "worker"
        }
    },
    {
        "org.bccvl.tasks.compute.perl_task": {
            "queue": "worker",
            "routing_key": "worker"
        }
    },
    {
        "org.bccvl.tasks.compute.demo_task": {
            "queue": "worker",
            "routing_key": "worker"
        }
    }
]

CELERY_QUEUES = {
    "worker": {"routing_key": "worker"},
    "datamover": {"routing_key": "datamover"},
    "plone": {"routing_key": "plone"}
}

CELERY_SEND_EVENTS = True
CELERY_SEND_TASK_SENT_EVENT = True


CELERYD_PREFETCH_MULTIPLIER = 1
CELERYD_FORCE_EXECV = False
