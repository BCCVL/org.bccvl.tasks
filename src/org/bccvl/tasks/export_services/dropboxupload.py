import logging
from random import randint
import shutil
import time
import os.path

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
    foldername = metadata['title']
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
                client = dropbox.client.DropboxClient(access_token)
                # if dir exists, delete it first.
                try:
                    m = client.metadata(foldername, include_deleted=False)
                except dropbox.rest.ErrorResponse as e:
                    if not e.status == 404:
                        raise e
                    else:
                        pass  # no metadata means it does not exist
                else:
                    # is_deleted should not occur with include_deleted=False but the docs seemed a bit out of sync with the actual api
                    # so better save than sorry.
                    if not ('is_deleted' in m and m['is_deleted']):
                        client.file_delete(metadata['title'])

                client.file_create_folder(foldername)
                client.file_create_folder(os.path.join(foldername, 'data'))

                datafiles = get_datafiles(tmpdir, include_prov=False)

                for fn in datafiles:
                    client.put_file(
                        os.path.join(foldername, os.path.basename(fn)),
                        open(fn, 'rb'))
                    uploaded.append(fn)

                mets_fn = os.path.join(tmpdir, 'mets.xml')
                client.put_file(
                    os.path.join(foldername, os.path.basename(mets_fn)),
                    open(mets_fn, 'rb'))
                uploaded.append(mets_fn)

                prov_fn = os.path.join(tmpdir, 'prov.ttl')
                client.put_file(
                    os.path.join(foldername, os.path.basename(prov_fn)),
                    open(prov_fn, 'rb'))
                uploaded.append(prov_fn)

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
            except dropbox.rest.ErrorResponse as e:
                if not e.status == 503:
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
