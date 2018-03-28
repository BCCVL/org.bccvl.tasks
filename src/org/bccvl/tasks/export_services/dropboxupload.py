import logging
from random import randint
import shutil
import time
import os.path
import re

import dropbox

from .util import get_files, get_oauth_tokens, get_metadata, get_datafiles, send_mail


LOG = logging.getLogger(__name__)


def export_dropbox(siteurl, fileurls, serviceid, context, conf):
    uploaded = []
    last_error = None
    tmpdir = get_files(fileurls, context['user']['id'], conf)
    metadata = get_metadata(os.path.join(tmpdir, 'mets.xml'))
    try:
        client_tokens, access_tokens = get_oauth_tokens(
            siteurl, serviceid, context['user']['id'], conf)
        access_token = access_tokens['access_token']
    except Exception as e:
        msg = "Error uploading experiment '{0}' - Access Token could not be refreshed: {1}".format(
            metadata['title'],
            str(e))
        LOG.error(msg, exc_info=True)
        send_mail(context, serviceid, metadata['title'], msg, success=False)
    foldername = metadata['title'].strip('/')
    foldername = '/' + re.sub('[\\/:*?"<>|]', '_', foldername)

    try:
        success = False
        for i in range(5):  # we'll give it 5 tries
            LOG.info(
                "Attempting to upload '{}' to dropbox.".format(
                    metadata['title']))
            if i != 0:
                t = randint(5, 20)
                LOG.info(
                    "Waiting for {} seconds before trying again".format(t))
                time.sleep(t)
            try:
                dbx = dropbox.Dropbox(access_token)
                # if dir exists, delete it first.
                try:
                    m = dbx.files_get_metadata(foldername, include_deleted=False)
                except dropbox.exceptions.ApiError as e:
                    if type(e.error).__name__ != 'GetMetadataError':
                        raise e
                else:
                    dbx.files_delete(foldername)

                dbx.files_create_folder(foldername)
                dbx.files_create_folder(os.path.join(foldername, 'data'))

                datafiles = get_datafiles(tmpdir, include_prov=False)

                for fn in datafiles:
                    with open(fn, 'rb') as f:
                        dbx.files_upload(
                            f.read(),
                            os.path.join(foldername, 'data', os.path.basename(fn)))
                    uploaded.append(fn)

                for fname in ('mets.xml', 'prov.ttl', 'expmetadata.txt'):
                    fn = os.path.join(tmpdir, fname)
                    with open(fn, 'rb') as f:
                        dbx.files_upload(
                            f.read(),
                            os.path.join(foldername, os.path.basename(fn)))
                    uploaded.append(fn)

                msg = "\n".join(uploaded)
                send_mail(
                    context,
                    serviceid,
                    metadata['title'],
                    msg,
                    success=True)
                success = True
                LOG.info(
                    "Upload of '{}' to dropbox complete.".format(
                        metadata['title']))
                break
            except dropbox.exceptions.ApiError as e:
                if type(e.error).__name__ != 'UploadError':
                    raise e
                else:
                    last_error = str(e)
                    LOG.warning(
                        "Unsuccessful attempt to upload to dropbox: " +
                        str(e))
        if not success:
            raise Exception("Too many retries. Last error: " + last_error)
    except Exception as e:
        msg = "Error uploading experiment '{0}': {1}".format(
            metadata['title'], str(e))
        LOG.error(msg, exc_info=True)
        send_mail(context, serviceid, metadata['title'], msg, success=False)
    finally:
        if os.path.exists(tmpdir):
            shutil.rmtree(tmpdir)
