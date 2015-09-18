import requests
import json
import tempfile
import zipfile
from os.path import split, exists, join
import logging
from datetime import datetime

# google api
import httplib2
from apiclient import discovery
from oauth2client import client
from apiclient import errors
from apiclient.http import MediaFileUpload, MediaIoBaseUpload
import mimetypes
import shutil

from time import sleep
from random import randint

from .util import get_zip, get_oauth_tokens, guess_mimetype, get_metadata, get_datafiles, send_mail

LOG = logging.getLogger(__name__)


def export_googledrive(zipurl, serviceid, context):
    zf = get_zip(zipurl)
    metadata = get_metadata(zf)
    uploaded = []
    try:
        client_tokens, access_tokens = get_oauth_tokens(
            serviceid, context['user']['id'])
        tokens = access_tokens
        tokens.update(client_tokens['auto_refresh_kwargs'])
        tokens['token_expiry'] = datetime.fromtimestamp(
            tokens['expires_at']).isoformat() + "Z"
        tokens['token_uri'] = "https://accounts.google.com/o/oauth2/token"
        tokens['user_agent'] = None
        tokens['invalid'] = False
    except Exception as e:
        msg = "Error uploading experiment '{0}' - Access Token could not be refreshed: {1}".format(
            metadata['title'],
            str(e))
        LOG.error(msg)
        send_mail(context, serviceid, metadata['title'], msg, success=False)

    credentials = client.OAuth2Credentials.from_json(
        json.dumps(tokens))
    if credentials.access_token_expired:
        try:
            credentials.refresh(httplib2.Http())
            assert(not credentials.access_token_expired)
        except Exception as e:
            msg = "Error uploading experiment - Access Token could not be refreshed: {1}".format(
                metadata['title'],
                str(e))
            print msg
            sys.exit(1)
            LOG.error(msg)
            send_mail(
                context,
                serviceid,
                metadata['title'],
                msg,
                success=False)
    try:
        http_auth = credentials.authorize(httplib2.Http())
        drive_service = discovery.build('drive', 'v2', http_auth)
    except Exception as e:
        msg = "Error uploading experiment '{0}' - Google Drive authentication failed: {1}".format(
            metadata['title'],
            str(e))
        LOG.error(msg)
        send_mail(context, serviceid, metadata['title'], msg, success=False)

    try:
        files = []
        page_token = None
        while True:
            try:
                param = {}
                if page_token:
                    param['pageToken'] = page_token
                result = drive_service.files().list(**param).execute()

                files.extend(result['items'])
                page_token = result.get('nextPageToken')
                if not page_token:
                    break
            except errors.HttpError as error:
                print 'An error occurred: %s' % error
                break
        folders = dict((f['title'], f['id']) for f in files if f[
                       'mimeType'] == "application/vnd.google-apps.folder" and f['labels']['trashed'] == False)
        bccvl_folder_id = folders.get("BCCVL", None)

        if bccvl_folder_id is None:
            body = {
                'title': "BCCVL",
                'description': 'BCCVL Experiments (http://www.bccvl.org.au)',
                'mimeType': 'application/vnd.google-apps.folder',
            }
            folder = drive_service.files().insert(
                body=body,
                media_body=None).execute()
            bccvl_folder_id = folder['id']

    except Exception as e:
        msg = "Error uploading experiment '{0}': {1}".format(
            metadata['title'], str(e))
        LOG.error(msg)
        send_mail(context, serviceid, metadata['title'], msg, success=False)

    try:
        body = {
            'title': metadata['title'],
            'description': metadata['description'],
            'mimeType': 'application/vnd.google-apps.folder',
            'parents': [{'id': bccvl_folder_id}, ]
        }
        folder = drive_service.files().insert(
            body=body,
            media_body=None).execute()
        exp_folder_id = folder['id']

        body = {
            'title': 'data',
            'description': 'Experiment data for "{0}"'.format(metadata['title']),
            'mimeType': 'application/vnd.google-apps.folder',
            'parents': [{'id': exp_folder_id}, ]
        }
        folder = drive_service.files().insert(
            body=body,
            media_body=None).execute()
        data_folder_id = folder['id']
        data_files = get_datafiles(zf, include_prov=False)
        tmpdir = tempfile.mkdtemp(prefix='bccvl')
        zf.extractall(tmpdir)
        for data_file in data_files:
            mimetype = guess_mimetype(data_file)
            media_body = MediaFileUpload(
                join(
                    tmpdir,
                    data_file),
                mimetype=mimetype,
                resumable=True)
            body = {
                'title': split(data_file)[-1],
                'parents': [{'id': data_folder_id}, ],
                'mimeType': mimetype,
            }
            file = drive_service.files().insert(
                body=body,
                media_body=media_body).execute()
            uploaded.append(data_file)

        mets_fn = filter(lambda x: x.endswith('mets.xml'), zf.namelist())[0]
        mimetype = guess_mimetype(mets_fn)
        media_body = MediaFileUpload(
            join(
                tmpdir,
                mets_fn),
            mimetype=mimetype,
            resumable=True)
        body = {
            'title': 'mets.xml',
            'description': 'METS Metadata information for "{0}". For more information, please see http://www.loc.gov/standards/mets/METSOverview.v2.html'.format(metadata['title']),
            'parents': [{'id': exp_folder_id}, ],
            'mimeType': mimetype,
        }
        file = drive_service.files().insert(
            body=body,
            media_body=media_body).execute()
        uploaded.append(mets_fn)

        prov_fn = filter(lambda x: x.endswith('prov.ttl'), zf.namelist())
        if len(prov_fn):
            prov_fn = prov_fn[0]
            mimetype = guess_mimetype(prov_fn)
            media_body = MediaFileUpload(
                join(
                    tmpdir,
                    prov_fn),
                mimetype=mimetype,
                resumable=True)
            body = {
                'title': 'prov.ttl',
                'description': 'PROV-O Provenance information for "{0}". For more information, please see http://www.w3.org/TR/prov-o/'.format(metadata['title']),
                'parents': [{'id': exp_folder_id}, ],
                'mimeType': mimetype,
            }
            file = drive_service.files().insert(
                body=body,
                media_body=media_body).execute()
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
