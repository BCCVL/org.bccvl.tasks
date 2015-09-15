from __future__ import absolute_import

from org.bccvl.tasks.celery import app
import logging

LOG = logging.getLogger(__name__)


from celery import Task
import transaction
import sys
import os
import shutil
import time
from AccessControl.SecurityManagement import noSecurityManager
from AccessControl.SecurityManagement import newSecurityManager
from AccessControl.SecurityManagement import getSecurityManager
from AccessControl.SecurityManagement import setSecurityManager
from AccessControl import SpecialUsers
from Testing.makerequest import makerequest
from ZODB.POSException import ConflictError
from ZPublisher.Publish import Retry as RetryException
import Zope2
from zope.event import notify
from zope.app.publication.interfaces import BeforeTraverseEvent
from zope.component.hooks import setSite, getSite
# TODO: decide which one to use
from Products.CMFPlone.interfaces import IPloneSiteRoot
#from Products.CMFCore.interfaces import ISiteRoot
from org.bccvl.site.interfaces import IJobTracker
import pkg_resources
import smtplib
from email.mime.text import MIMEText


class AfterCommitTask(Task):
    # TODO: looks like we don't need this class'
    """Base for tasks that queue themselves after commit.

    This is intended for tasks scheduled from inside Zope.
    """
    abstract = True

    # Override apply_async to register an after-commit hook
    # instead of queueing the task right away.
    def apply_async(self, *args, **kw):
        def hook(success):
            # TODO: maybe if apply fails try to send a state update failed
            if success:
                super(AfterCommitTask, self).apply_async(*args, **kw)
        transaction.get().addAfterCommitHook(hook)
        # apply_async normally returns a deferred result object,
        # but we don't have one available yet


def after_commit_task(task, *args, **kw):
    # send task after a successful transaction
    def hook(success):
        if success:
            # TODO: maybe if apply fails try to send a state update failed
            task.apply_async(args=args, kwargs=kw)
    transaction.get().addAfterCommitHook(hook)


def zope_task(**task_kw):
    """Decorator of celery tasks that should be run in a Zope context.

    The decorator function takes a path as a first argument,
    and will take care of traversing to it and passing it
    (presumably a portal) as the first argument to the decorated function.

    Also takes care of initializing the Zope environment,
    running the task within a transaction, and retrying on
    ZODB conflict errors.
    """
    # NOTE: each celery worker process configures a Zope environment only once
    #       the same Zope env is then used again within the same process.
    #       (each job uses a new zodb connection though)

    # kw['site_path'] ... path to plone root
    # os.environ['ZOPE_CONFIG']
    def wrap(func):
        bind = task_kw.get('bind', False)

        def new_func(self, *args, **kw):
            # This is a super ugly way of getting Zope to configure itself
            # from the main instance's zope.conf. XXX FIXME
            sys.argv = ['']
            if 'ZOPE_CONFIG' not in os.environ:
                os.environ['ZOPE_CONFIG'] = 'parts/instance/etc/zope.conf'
            zapp = makerequest(Zope2.app())

            oldsm = getSecurityManager()
            oldsite = getSite()
            try:
                zodb_retries = 3
                retryable = (ConflictError, RetryException)
                while zodb_retries > 0:
                    try:
                        transaction.begin()

                        # assume zope context info is either in kw or last in args
                        ctxt = kw.get('context', args[-1])
                        userid = ctxt['user']['id']
                        #-> split path components in
                        #   context['context_path'], convert to str and
                        #   traverse each one separately checking the
                        #   result for ISiteRoot
                        ctxt_path = ctxt['context'].strip().strip('/').split('/')
                        site = obj = zapp
                        for name in ctxt_path:
                            obj = obj.unrestrictedTraverse(str(name))
                            if IPloneSiteRoot.providedBy(obj):
                                # fire traversal event so various things get set up
                                site = obj
                                notify(BeforeTraverseEvent(site, site.REQUEST))
                        # if we are still here, context has been found
                        # let's extend kw with what we have found
                        kw['_site'] = site
                        kw['_context'] = obj

                        # set up security manager
                        uf = getattr(site, 'acl_users', None)
                        user = None
                        while uf is not None:
                            # FIXME: should this be user id or user name?
                            user = uf.getUser(name=userid)
                            if user is not None:
                                break
                            parent = uf.__parent__.__parent__
                            uf = getattr(parent, 'acl_users', None)
                        if user is None:
                            # No user found anywhere, so let's us our
                            # special nobody user
                            user = SpecialUsers.nobody
                        newSecurityManager(None, user)

                        # run the task
                        if bind:
                            result = func(self, *args, **kw)
                        else:
                            result = func(*args, **kw)

                        # commit transaction
                        transaction.commit()
                        # seems like all wont well. let's jump out of retry loop
                        break
                    except retryable as e:
                        # On ZODB conflicts, retry using celery's mechanism
                        LOG.info("ConflictError, retrying task %s: %d retries left",
                                 self.name, zodb_retries)
                        transaction.abort()
                        # first retry the transaction ourselves
                        zodb_retries -= 1
                        if zodb_retries > 0:
                            # we still have retries left
                            continue
                        # let celery re-schedule it
                        LOG.warn("Couldn't recover task %s with normal zodb retries, reschedule for the %d time.",
                                 self.name, self.request.retries)
                        raise self.retry(exc=e)
                    except:
                        # a non retrieable error happened
                        transaction.abort()
                        # TODO: preserve stack trace somehow
                        raise
            finally:
                # noSecurityManager()
                # setSite(None)
                setSecurityManager(oldsm)
                setSite(oldsite)
                zapp._p_jar.close()

            return result

        new_func.__name__ = func.__name__

        aftercommit = task_kw.pop('aftercommit', False)
        task_kw['bind'] = True
        if aftercommit:
            return app.task(base=AfterCommitTask, **task_kw)(new_func)
        else:
            return app.task(**task_kw)(new_func)
    return wrap

# TODO: these jobs need to run near a zodb or plone instance
@zope_task()
def import_ala(path, lsid, context, **kw):
    from collective.transmogrifier.transmogrifier import Transmogrifier
    metadata_file = 'ala_dataset.json'
    # transmogrifier context needs to be the parent object, in case
    # we have to create the dataset as well
    LOG.info("import ala %s to %s", path, context)
    transmogrifier = Transmogrifier(kw['_context'].__parent__)
    transmogrifier(u'org.bccvl.site.alaimport',
                   alasource={'file': os.path.join(path, metadata_file),
                              'lsid': lsid,
                              'id': kw['_context'].getId()})


# TODO: this may not need a plone instance?
# TODO: this task is not allowed to fail
@app.task()
def import_cleanup(path, context, **kw):
    # In case a previous step failed we still have to clean up
    # TODO: may throw exception ... do we care?
    shutil.rmtree(path)
    LOG.info("cleanup ala %s to %s", path, context)


# TODO: maybe do cleanup here? and get rid of above task?
@zope_task()
def import_result(params, context, **kw):
    from collective.transmogrifier.transmogrifier import Transmogrifier
    # transmogrifier context needs to be the parent object, in case
    # we have to create the dataset as well
    LOG.info("import results %s to %s", params['result']['results_dir'], context)
    transmogrifier = Transmogrifier(kw['_context'])
    transmogrifier(u'org.bccvl.compute.resultimport',
                   resultsource={'path': params['result']['results_dir'],
                                 'outputmap': params['result']['outputs']})
                                 
    jt = IJobTracker(kw['_context'])
                                     
    # Send email to notify results are ready
    fullname = context['user']['fullname']
    email_addr = context['user']['email']
    experiment_name = context['experiment']['title']
    experiment_url = context['experiment']['url']
    success = (jt.state == 'COMPLETED')
    if fullname and email_addr and experiment_name and experiment_url:
        send_mail(fullname, email_addr, experiment_name, experiment_url, success)
    else:
        LOG.warn("Not sending email. Invalid parameters")

    
# TODO: this task is not allowed to fail
@zope_task()
def set_progress(state, message, context, **kw):
    jt = IJobTracker(kw['_context'])
    jt.set_progress(state, message)
    if state in ('COMPLETED', 'FAILED'):
        jt.state = state
        LOG.info("Plone: Update job state %s", state)
    else:
        jt.state = 'RUNNING'
        LOG.info("Plone: Update job state RUNNING")
    kw['_context'].reindexObject() # TODO: reindex job state only?
    LOG.info("Plone: Update job progress: %s, %s, %s", state, message, context)
    
# compute the experiement run time if all its jobs are completed
    # The experiment is the parent job
    jt = IJobTracker(kw['_context'].__parent__)
    if jt.state in ('COMPLETED', 'FAILED'):
        exp = jt.context
        exp.runtime = time.time() - (exp.created().millis()/1000.0)

def send_mail(fullname, user_address, experiment_name, experiment_url, success):
    if success:
        job_status = 'completed'
    else:
        job_status = 'failed'
    body = pkg_resources.resource_string("org.bccvl.tasks", "complete_email.txt")
    body = body.format(fullname=fullname, experiment_name=experiment_name, job_status=job_status, experiment_url=experiment_url)

    htmlbody = pkg_resources.resource_string("org.bccvl.tasks", "complete_email.html")
    htmlbody = htmlbody.format(fullname=fullname, experiment_name=experiment_name, job_status=job_status, experiment_url=experiment_url)
  
    msg = MIMEMultipart('alternative')
    msg['Subject'] = "Your BCCVL experiment is complete"
    msg['From'] = "Biodiversity & Climate Change Virtual Lab <bccvl@griffith.edu.au>"
    msg['To'] = user_address
    
    msg.attach(body, 'plain')
    msg.attach(htmlbody, 'html')

    server = smtplib.SMTP("localhost")
    server.sendmail("bccvl@griffith.edu.au", user_address, msg.as_string())
    server.quit()
