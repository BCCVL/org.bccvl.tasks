from __future__ import absolute_import

from org.bccvl.tasks import datamover
from org.bccvl.tasks import plone

# FIXME: this module shouldn't be here better suited to live in site package,
# because it doesn't define a task, but rather builds a task chain to send off


def result_export(zipurl, serviceid, context):
    # creates task chain that can be submitted to queue

    # 1. set progress info
    # TODO: can this one fail as well?
    start_job = plone.set_progress.si('RUNNING', 'Download {0} from ala'.format(zipurl), context)
    # 2. do export

    export_job = datamover.export_result.si(zipurl, serviceid, context)

    # set error status on failure
    export_job.link_error(
        # TODO: allow passing in result/exception of previous job
        plone.set_progress.si('FAILED',
                              'Datamover failed to download {0} from ala'.format(zipurl), context))

    # TODO: some sort of user notification....
    #       ... email, link on webpage,

    # 4. success and cleanup
    success_job = plone.set_progress.si(
        'COMPLETED',
        'Import of ala data for {0} finished.'.format(zipurl), context)

    # chain all jobs together
    export_job = start_job | export_job | success_job

    return export_job
