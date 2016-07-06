from __future__ import absolute_import

from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import logging
import os
import pkg_resources
import shutil
import time
from urlparse import urlparse

from plone import api
from zope.component import getUtility

from org.bccvl.site.job.interfaces import IJobTracker, IJobUtility
from org.bccvl.site.interfaces import IExperimentJobTracker
from org.bccvl.tasks.celery import app

from .utils import zope_task


LOG = logging.getLogger(__name__)


# TODO: these jobs need to run near a zodb or plone instance
@zope_task()
def import_ala(items, results_dir, context, **kw):
    from collective.transmogrifier.transmogrifier import Transmogrifier
    # transmogrifier context needs to be the parent object, in case
    # we have to create the dataset as well
    LOG.info("import ala %s to %s", results_dir, context)
    # transmogrifier context needs to be the parent folder
    transmogrifier = Transmogrifier(kw['_context'].__parent__)
    transmogrifier(u'org.bccvl.site.alaimport',
                   contextsource={'path': results_dir,
                                  'content_id': kw['_context'].getId(),
                                  'items': items})


# TODO: these jobs need to run near a zodb or plone instance
@zope_task()
def import_file_metadata(items, results_dir, context, **kw):
    from collective.transmogrifier.transmogrifier import Transmogrifier
    # transmogrifier context needs to be the parent object, in case
    # we have to create the dataset as well
    LOG.info("update metadata for %s,  %s", results_dir, context)
    transmogrifier = Transmogrifier(kw['_context'].__parent__)
    transmogrifier(u'org.bccvl.site.add_file_metadata',
                   contextsource={'path': results_dir,
                                  'content_id': kw['_context'].getId(),
                                  'items': items})


# TODO: this may not need a plone instance?
# TODO: this task is not allowed to fail
@app.task()
def import_cleanup(path, context, **kw):
    # In case a previous step failed we still have to clean up
    # FIXME: may throw exception ...
    #        just catch all exceptions ... log an error and continue as if nothing happened
    path = urlparse(path).path
    if os.path.exists(path):
        shutil.rmtree(path)
    LOG.info("cleanup ala %s to %s", path, context)


# TODO: maybe do cleanup here? and get rid of above task?
@zope_task()
def import_result(items, results_dir, context, **kw):
    from collective.transmogrifier.transmogrifier import Transmogrifier
    # transmogrifier context needs to be the parent object, in case
    # we have to create the dataset as well
    LOG.info("import results %s to %s", results_dir, context)
    transmogrifier = Transmogrifier(kw['_context'])
    # FIXME: 'path': results_dir is not being used anymore
    transmogrifier(u'org.bccvl.compute.resultimport',
                   resultsource={'path': results_dir,
                                 'items': items})


# TODO: this task is not allowed to fail
@zope_task()
def set_progress(state, message, rusage, context, **kw):
    jobtool = getUtility(IJobUtility)
    if '_jobid' in kw:
        # TODO: should we do some security check here?
        #       e.g. only admin and user who owns the job can update it?
        # TODO: jobid may not exist
        job = jobtool.get_job_by_id(kw['_jobid'])
    else:
        jt = IJobTracker(kw['_context'])
        job = jt.get_job()
    jobtool.set_progress(job, state, message, rusage)
    if state in ('COMPLETED', 'FAILED'):
        jobtool.set_state(job, state)
        LOG.info("Plone: Update job state %s", state)

        # FIXME: we sholud probably send emails in another place (or as additional task in chain?)
        #        there are too many things that can go wrong here and this task is not allowed to
        #        fail (throw an exception) otherwise the user will never see a status update
        # FIXME: should be a better check here, we want to send email only
        #        for experiment results, not for dataset imports (i.e. ala)
        try:
            if 'experiment' in context:
                # Send email to notify results are ready
                fullname = context['user']['fullname']
                email_addr = context['user']['email']
                experiment_name = context['experiment']['title']
                experiment_url = context['experiment']['url']
                success = (job.state == 'COMPLETED')
                if fullname and email_addr and experiment_name and experiment_url:
                    send_mail(fullname, email_addr, experiment_name, experiment_url, success)
                else:
                    LOG.warn("Not sending email. Invalid parameters")
        except Exception as e:
            LOG.error('Got an exception in plone.set_progress while trying to send an email: %s', e)
    else:
        jobtool.set_state(job, state)
        LOG.info("Plone: Update job state RUNNING")
    if not '_jobid' in kw:
        kw['_context'].reindexObject() # TODO: reindex job state only?
        # Compute the experiement run time if all its jobs are completed
        # The experiment is the parent job
        jt = IExperimentJobTracker(kw['_context'].__parent__, None)
        if jt and jt.state in ('COMPLETED', 'FAILED'):
            exp = jt.context
            exp.runtime = time.time() - (exp.created().millis()/1000.0)
    LOG.info("Plone: Update job progress: %s, %s, %s", state, message, context)


def send_mail(fullname, user_address, experiment_name, experiment_url, success):
    if success:
        job_status = 'completed'
    else:
        job_status = 'failed'

    subject = "Your BCCVL experiment has %s" %job_status
    body = pkg_resources.resource_string("org.bccvl.tasks", "complete_email.txt")
    body = body.format(fullname=fullname, experiment_name=experiment_name, job_status=job_status, experiment_url=experiment_url)

    htmlbody = pkg_resources.resource_string("org.bccvl.tasks", "complete_email.html")
    htmlbody = htmlbody.format(fullname=fullname, experiment_name=experiment_name, job_status=job_status, experiment_url=experiment_url)

    msg = MIMEMultipart('alternative')
    msg.attach(MIMEText(body, 'plain'))
    msg.attach(MIMEText(htmlbody, 'html'))

    api.portal.send_email(recipient=user_address, subject=subject, body=msg.as_string())
