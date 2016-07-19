import functools
import logging
import os
import sys
from urlparse import urlsplit

from celery import Task
import transaction

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
from plone import api

from org.bccvl.tasks.celery import app


LOG = logging.getLogger(__name__)


def create_task_context(context, member=None):
    if not member:
        member = api.user.get_current()
    req = None
    if context.REQUEST:
        other = ['VirtualRootPhysicalPath', 'SERVER_URL']  # 'VIRTUAL_URL', 'ACTUAL_URL', 'method', ...
        environ = ['HTTP_HOST', 'HTTP_X_FORWARDED_HOST', 'HTTP_X_FORWARDED_FOR', 'HTTP_X_FORWARDED_PROTO', 'REMOTE_ADDR', 'REQUEST_METHOD', 'SCRIPT_NAME', 'SERVER_NAME', 'SERVER_PORT']  # HTTPS, SERVER_PORT_SECURE
        req = {
            'environ': {var: context.REQUEST._orig_env[var] for var in environ if var in context.REQUEST._orig_env},
            'other': {var: context.REQUEST.other[var] for var in other if var in context.REQUEST.other}
        }
    return {
        'request': req,
        'context': '/'.join(context.getPhysicalPath()),
        'user': {
            'id': member.getUserName(),
            'email': member.getProperty('email'),
            'fullname': member.getProperty('fullname')
        }
    }


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
                # FIXME: this is the earliest possible place wher i can get a taskid
                super(AfterCommitTask, self).apply_async(*args, **kw)
        transaction.get().addAfterCommitHook(hook)
        # apply_async normally returns a deferred result object,
        # but we don't have one available yet


def after_commit_task(task, *args, **kw):
    # send task after a successful transaction
    def hook(success):
        if success:
            # TODO: maybe if apply fails try to send a state update failed
            # FIXME: this is the earliest possible place wher i can get a taskid
            result = task.apply_async(args=args, kwargs=kw)
    transaction.get().addAfterCommitHook(hook)


# TODO: use celery retry in case of a more severe error
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
    # os.environ['Z_CONFIG_FILE']
    def wrap(func):
        bind = task_kw.get('bind', False)

        def new_func(self, *args, **kw):
            # This is a super ugly way of getting Zope to configure itself
            # from the main instance's zope.conf. XXX FIXME
            sys.argv = ['']
            if 'ZOPE_CONFIG' not in os.environ:
                os.environ['ZOPE_CONFIG'] = os.environ.get('Z_CONFIG_FILE', 'parts/instance/etc/zope.conf')
            ctxt = kw.get('context', {})
            zapp = makerequest(Zope2.app(), environ=ctxt.get('request', {}).get('environ'))
            # if we have a virtual path, update the request object with virtual host info
            other = ctxt.get('request', {}).get('other', {})
            if 'SERVER_URL' in other:
                zapp.REQUEST.other['SERVER_URL'] = other['SERVER_URL']
            if 'VirtualRootPhysicalPath' in other:
                vrootpath = '/'.join(other['VirtualRootPhysicalPath']).encode('utf-8')  # encoding should be ascii?
                vroot = zapp.unrestrictedTraverse(vrootpath)
                # build list of parents
                parents = []
                while getattr(vroot, '__parent__', None):
                    parents.append(vroot)
                    vroot = vroot.__parent__
                # update list of parents
                # setVirtualRoot is meant to be called during traversal... set PARENTS in reverse order
                zapp.REQUEST['PARENTS'] = list(reversed(parents))
                # set virtual root (need 'PARENTS')
                zapp.REQUEST.setVirtualRoot('/')  # vrootpath)
                # set PARENTS in correct order (after traversal)
                zapp.REQUEST['PARENTS'] = parents

            oldsm = getSecurityManager()
            oldsite = getSite()
            try:
                zodb_retries = 3
                retryable = (ConflictError, RetryException)
                while zodb_retries > 0:
                    try:
                        transaction.begin()

                        # assume zope context is always passed as kw
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
                        if 'jobid' in ctxt:
                            kw['_jobid'] = ctxt['jobid']

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

        functools.update_wrapper(new_func, func)
        #new_func.__name__ = func.__name__

        aftercommit = task_kw.pop('aftercommit', False)
        task_kw['bind'] = True
        if aftercommit:
            return app.task(base=AfterCommitTask, **task_kw)(new_func)
        else:
            return app.task(**task_kw)(new_func)
    return wrap
