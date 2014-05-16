from __future__ import absolute_import

from org.bccvl.tasks import datamover
from org.bccvl.tasks import plone

# FIXME: this module shouldn't be here better suited to live in site package, because it doesn't define a task, but rather builds a task chain to send off

# ALA import is a chain of tasks
# 1. start download
# 2. initate import

# TODO:  this is a very short lived task and can basically run anywhere
#        maybe put in default queue?
# TODO: could add callbacks with each task. It would be possible to modify
#       list of callbacks and errbacks within each task

# submit after transaction commit, otherwise we may submit it
# multiple times in case of conflicterrors, or kick off task for
# non existent content, in case task starts before this transaction
# commits
def ala_import(lsid, path, context):
    # creates task chain that can be submitted to queue
    """
    lsid .. species id
    path ... destination path for ala import files
    context ... a dictionary with keys:
      - context: path to context object
      - userid: zope userid
    """
    # TODO: maybe add current job id to context?
    # TODO: I could add callbacks/errbacks in job themselves,
    #       allows to add additional error information without waiting for return values here
    plone.set_progress.delay('SUBMITTED', 'SUBMITTED', context)

    # failed_job = plone.set_progress.si('FAILED', 'FAILED', context)

    # TODO: make context(userid, and context path explicit parameters?)
    import_job = plone.import_ala.si(path, lsid, context)
    # in case the import failed, set job state
    # import_job.link_error(failed_job)

    cleanup_job = plone.import_cleanup.si(path, context)
    # if we run a cleanup the job has failed
    # cleanup_job.link(failed_job)
    # cleanup_job.link_error(failed_job)

    move_job = datamover.move.si([
        ({'type': 'ala', 'lsid': lsid},
         {'host': 'plone', 'path': path})],
        context)
    # we'll have to clean up if move fails'
    move_job.link_error(cleanup_job)

    success_job = plone.set_progress.si('COMPLETED', 'SUCCESS', context)

    ala_job = move_job | import_job | success_job

    return ala_job
