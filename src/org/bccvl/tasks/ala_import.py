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

    # 1. set progress info
    # TODO: can this one fail as well?
    start_download = plone.set_progress.si('RUNNING', 'Download {0} from ala'.format(lsid), context)
    # 2. do move
    move_job = datamover.move.si([
        ('ala://ala?lsid=' + lsid,
         'scp://plone@127.0.0.1' + path)],
        context)
    # we'll have to clean up if move fails'
    move_job.link_error(
        # TODO: allow passing in result/exception of previous job
        plone.set_progress.si('FAILED',
                             'Datamover failed to download {0} from ala'.format(lsid), context))
    move_job.link_error(plone.import_cleanup.si(path, context))

    # 3. import data
    start_import = plone.set_progress.si('RUNNING', 'Import {0} from ala'.format(lsid), context)

    import_job = plone.import_ala.si(path, lsid, context)
    # in case the import failed, set job state
    import_job.link_error(
        # TODO: allow passing in result/exception of previous job
        plone.set_progress.si('FAILED',
                             'Import of ala data for {0} failed.'.format(lsid), context))
    import_job.link_error(plone.import_cleanup.si(path, lsid, context))

    # 4. success and cleanup
    success_job = plone.set_progress.si(
        'COMPLETED',
        'Import of ala data for {0} finished.'.format(lsid), context)
    cleanup_job = plone.import_cleanup.si(path, lsid, context)

    # chain all jobs together
    ala_job = start_download | move_job | start_import | import_job | success_job | cleanup_job

    return ala_job
