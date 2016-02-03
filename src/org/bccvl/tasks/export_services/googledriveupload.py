from datetime import datetime
import json
import logging
import os.path
import shutil

# google api
import httplib2
from apiclient import discovery
from oauth2client import client
from apiclient import errors
from apiclient.http import MediaFileUpload

from .util import get_files, get_oauth_tokens, guess_mimetype, get_metadata, get_datafiles, send_mail


LOG = logging.getLogger(__name__)


def export_googledrive(siteurl, fileurls, serviceid, context, conf):
    tmpdir = get_files(fileurls, context['user']['id'], conf)
    metadata = get_metadata(os.path.join(tmpdir, 'mets.xml'))
    uploaded = []
    try:
        client_tokens, access_tokens = get_oauth_tokens(
            siteurl, serviceid, context['user']['id'], conf)
        tokens = access_tokens
        tokens.update(client_tokens)
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
        bccvl_folder_id = folders.get("BCCVL Experiments", None)

        if bccvl_folder_id is None:
            body = {
                'title': "BCCVL Experiments",
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
        data_files = get_datafiles(tmpdir, include_prov=False)
        for data_file in data_files:
            mimetype = guess_mimetype(data_file)
            media_body = MediaFileUpload(
                data_file,
                mimetype=mimetype,
                resumable=True)
            body = {
                'title': os.path.split(data_file)[-1],
                'parents': [{'id': data_folder_id}, ],
                'mimeType': mimetype,
            }
            file = drive_service.files().insert(
                body=body,
                media_body=media_body).execute()
            uploaded.append(data_file)

        mets_fn = os.path.join(tmpdir, 'mets.xml')
        mimetype = guess_mimetype(mets_fn)
        media_body = MediaFileUpload(
            mets_fn,
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

        prov_fn = os.path.join(tmpdir, 'prov.ttl')
        if len(prov_fn):
            mimetype = guess_mimetype(prov_fn)
            media_body = MediaFileUpload(
                prov_fn,
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
        if os.path.exists(tmpdir):
            shutil.rmtree(tmpdir)
