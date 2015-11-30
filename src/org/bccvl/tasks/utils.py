import logging

from org.bccvl.tasks.celery import app
from org.bccvl.tasks.mdextractor import MetadataExtractor


LOG = logging.getLogger(__name__)


def set_progress(state, statusmsg, context):
    app.signature("org.bccvl.tasks.plone.set_progress",
                  args=(state, statusmsg, context)).delay()


def import_cleanup(results_dir, context):
    app.signature("org.bccvl.tasks.plone.import_cleanup",
                  args=(results_dir, context)).delay()


def set_progress_job(state, statusmsg, context):
    return app.signature("org.bccvl.tasks.plone.set_progress",
                         args=(state, statusmsg, context),
                         immutable=True)


def import_result_job(items, params, context):
    return app.signature("org.bccvl.tasks.plone.import_result",
                         args=(items, params, context),
                         immutable=True)


def import_file_metadata_job(items, params, context):
    return app.signature("org.bccvl.tasks.plone.import_file_metadata",
                         args=(items, params, context),
                         immutable=True)


def import_ala_job(items, params, context):
    return app.signature("org.bccvl.tasks.plone.import_ala",
                         args=(items, params, context),
                         immutable=True)


def import_cleanup_job(results_dir, context):
    return app.signature("org.bccvl.tasks.plone.import_cleanup",
                         args=(results_dir, context),
                         immutable=True)


def extract_metadata(filepath, filect):
    mdextractor = MetadataExtractor()
    try:
        return mdextractor.from_file(filepath, filect)
    except Exception as ex:
        LOG.warn("Couldn't extract metadata from file: %s : %s", filepath, repr(ex))
        raise


def traverse_dict(source, path):
    current = source
    try:
        for el in path.split('/'):
            if isinstance(current, list):
                el = int(el)
            current = current[el]
    except:
        # TODO: at least log error?
        current = None
    return current
