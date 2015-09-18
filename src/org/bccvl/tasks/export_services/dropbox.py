import requests
import json
import tempfile
import zipfile
from os.path import split, exists, join
import logging
from datetime import datetime

import shutil

import dropbox
from time import sleep
from random import randint

from .util import get_zip, get_oauth_tokens, guess_mimetype, get_metadata, get_datafiles, send_mail

LOG = logging.getLogger(__name__)


def export_dropbox(zipurl, serviceid, context):
    try:
        client_tokens, access_tokens = get_oauth_tokens(
            serviceid, context['user']['id'])
        access_token = access_tokens['access_token']
    except Exception as e:
        msg = "Error uploading experiment '{0}' - Access Token could not be refreshed: {1}".format(
            metadata['title'],
            str(e))
        LOG.error(msg)
        send_mail(context, serviceid, metadata['title'], msg, success=False)

    uploaded = []
    client = dropbox.client.DropboxClient(access_token)
    print 'linked account: ', client.account_info()

    zf = get_zip(zipurl)
    metadata = get_metadata(zf)

    foldername = metadata['title']

    # if dir exists, delete it first.
    try:
        m = client.metadata(foldername, include_deleted=False)
    except dropbox.rest.ErrorResponse as e:
        pass  # no metadata means it does not exist
    else:
        # is_deleted should not occur with include_deleted=False but the docs seemed a bit out of sync with the actual api
        # so better save than sorry.
        if not ('is_deleted' in m and m['is_deleted']):
            client.file_delete(metadata['title'])

    try:
        client.file_create_folder(foldername)
        client.file_create_folder(join(foldername, 'data'))

        tmpdir = tempfile.mkdtemp(prefix='bccvl')
        zf.extractall(tmpdir)
        datafiles = get_datafiles(zf, include_prov=False)

        for fn in datafiles:
            client.put_file(join(foldername, 'data', split(fn)
                                 [-1]), open(join(tmpdir, fn), 'rb'))
            uploaded.append(fn)

        mets_fn = filter(lambda x: x.endswith('mets.xml'), zf.namelist())[0]
        client.put_file(
            join(
                foldername, 'mets.xml'), open(
                    join(
                        tmpdir, mets_fn), 'rb'))
        uploaded.append(mets_fn)

        prov_fns = filter(lambda x: x.endswith('prov.ttl'), zf.namelist())
        if len(prov_fns):
            prov_fn = prov_fns[0]
            client.put_file(
                join(
                    foldername, 'prov.ttl'), open(
                        join(
                            tmpdir, mets_fn), 'rb'))
            uploaded.append(prov_fn)

        msg = "\n".join(uploaded)
        send_mail(
            context,
            serviceid,
            metadata['title'],
            msg,
            success=True)

    except Exception as e:
        msg = "Error uploading experiment '{0}': {1}".format(
            metadata['title'], str(e))
        LOG.error(msg)
        send_mail(context, serviceid, metadata['title'], msg, success=False)
    finally:
        if exists(tmpdir):
            shutil.rmtree(tmpdir)
