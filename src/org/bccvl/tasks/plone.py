from __future__ import absolute_import

from org.bccvl.tasks.celery import app
import logging

LOG = logging.getLogger(__name__)


from celery import Task
import transaction
import sys
import os
import shutil
from AccessControl.SecurityManagement import noSecurityManager
from AccessControl.SecurityManagement import newSecurityManager
from AccessControl import SpecialUsers
from Testing.makerequest import makerequest
from ZODB.POSException import ConflictError
import Zope2
from zope.event import notify
from zope.app.publication.interfaces import BeforeTraverseEvent
from zope.component.hooks import setSite
# TODO: decide which one to use
from Products.CMFPlone.interfaces import IPloneSiteRoot
#from Products.CMFCore.interfaces import ISiteRoot
from org.bccvl.site.interfaces import IJobTracker


class AfterCommitTask(Task):
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

            transaction.begin()

            try:
                try:
                    # assume zope context info is either in kw or last in args
                    ctxt = kw.get('context', args[-1])
                    userid = ctxt['userid']
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
                        user = uf.getUserById(userid)
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
                except ConflictError, e:
                    # On ZODB conflicts, retry using celery's mechanism
                    LOG.info("ConflictError, retrying task %s for the %d time",
                             self.name, self.request.retries)
                    transaction.abort()
                    raise self.retry(exc=e)
                except:
                    transaction.abort()
                    raise
            finally:
                noSecurityManager()
                setSite(None)
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
@zope_task(throws=(Exception, ))
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
@app.task(throws=(Exception, ))
def import_cleanup(path, context, **kw):
    # In case a previous step failed we still have to clean up
    # TODO: may throw exception ... do we care?
    shutil.rmtree(path)
    LOG.info("cleanup ala %s to %s", path, context)


@zope_task(throws=(Exception, ))
def import_result(params, context, **kw):
    set_progress.delay('RUNNING', 'IMPORT', context)
    from collective.transmogrifier.transmogrifier import Transmogrifier
    # transmogrifier context needs to be the parent object, in case
    # we have to create the dataset as well
    try:
        LOG.info("import results %s to %s", params['result']['results_dir'], context)
        transmogrifier = Transmogrifier(kw['_context'])
        transmogrifier(u'org.bccvl.compute.resultimport',
                       resultsource={'path': params['result']['results_dir'],
                                     'outputmap': params['result']['outputs']})
        after_commit_task(set_progress.si('COMPLETED', 'SUCCESS', context))
    except Exception as e:
        # TODO: don't send state in case we want to retry'
        set_progress.delay('FAILED', str(e), context)
        raise
    #raise ValueError('move failed')


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
    LOG.info("Plone: Update job progress: %s, %s, %s", state, message, context)
