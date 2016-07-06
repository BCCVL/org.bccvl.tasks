import logging

from .utils import zope_task

from org.bccvl.site.interfaces import IExperimentJobTracker
from org.bccvl.site.job.interfaces import IJobTracker
# TODO: look through all start_job calls....
# TODO: update names for plone tasks (moved to sub package)

LOG = logging.getLogger(__name__)


# TODO: these jobs need to run near a zodb or plone instance
@zope_task()
def submit_experiment(context, **kw):
    experiment = kw['_context']
    expjt = IExperimentJobTracker(experiment)
    from celery.contrib import rdb; rdb.set_trace()
    msgtype, msg = expjt.start_job(None)

    # TODO: check error from start_job?, catch exceptions?
    jt = IJobTracker(experiment)
    jt.set_progress('COMPLETED', 'Experiment Submitted')
