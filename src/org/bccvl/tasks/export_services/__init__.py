from __future__ import absolute_import
from org.bccvl.tasks.celery import app
from .dropboxupload import export_dropbox
from .googledriveupload import export_googledrive
from .figshareupload import export_figshare



def unsupported_service(zipurl, serviceid, context):
    raise NotImplementedError(
        "{} is currently not a supported service".format(serviceid))

@app.task()
def export_result(zipurl, serviceid, context):
    zipurl = "http://127.0.0.1:8201{context}/resultdownload".format(**context)
    export_func = globals().get("export_{}".format(serviceid), unsupported_service)
    export_func(zipurl, serviceid, context)

