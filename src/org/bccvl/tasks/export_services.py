import requests
from requests_oauthlib import OAuth1
import json
import tempfile
import zipfile
import xml.etree.ElementTree as ET
from os.path import split, exists, join
import smtplib
from email.mime.text import MIMEText
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

import dropbox

LOG = logging.getLogger(__name__)


TMPL_EMAIL_SUCCESS = """Hello {fullname},

Your experiment '{experiment_name}' has been successfully uploaded to {service_name}.

{message}

Kind regards,
The BCCVL
"""

TMPL_EMAIL_FAILURE = """Hello {fullname},

An error occured while uploading your experiment {experiment_name} to {service_name}.

Please find below the error message returned by the service. If the problem persists, please contact the BCCVL support team.

------------
{message}
------------

Kind regards,
The BCCVL
"""


def _get_zip(url):
    """
    Download experiment zip file, into a tempfile with autodelete, open as ZipFile and
    return ZipFile object.
    """
    r = requests.get(url, stream=True)
    f = tempfile.NamedTemporaryFile(
        prefix="bccvl_export", dir="/tmp", delete=True)
    for chunk in r.iter_content(chunk_size=1024):
        if chunk:  # filter out keep-alive new chunks
            f.write(chunk)
            f.flush()
    f.seek(0)
    return zipfile.ZipFile(f, 'r')


def _get_oauth_tokens(serviceid, user):
    """
    Acquire user and service related OAUTH tokens from the BCCVL
    """
    try:
        access_tokens = requests.get(
            "http://127.0.0.1:8201/bccvl/oauth/{0}/accesstoken?user={1}".format(serviceid, user)).json()
    except Exception as e:
        LOG.error('Error getting access token: {0}'.format(str(e)))
        raise e
    try:
        client_tokens = requests.get(
            "http://127.0.0.1:8201/bccvl/oauth/{0}/clienttoken".format(serviceid)).json()
    except Exception as e:
        LOG.error('Error getting client token: {0}'.format(str(e)))
        raise e
    return client_tokens, access_tokens


def _guess_mimetype(filename):
    special_types = {'Rout': 'text/plain', 'ttl': 'text/turtle'}
    return mimetypes.guess_type(filename)[0] or special_types.get(
        filename.split('.')[-1], 'application/octet-stream')


def _get_metadata(zf):
    """
    Extract title and description from the METS xml file.
    """
    try:
        fn = filter(lambda x: x.endswith('mets.xml'), zf.namelist())[0]
    except Exception as e:
        raise Exception('No METS data found.')
    root = ET.parse(zf.open(fn, 'r')).getroot()
    title = root.findall(
        './/{0}dmdSec/{0}mdWrap/{0}xmlData/{1}mods/{1}titleInfo[@displayLabel="Title"]/{1}title'.format(
            "{http://www.loc.gov/METS/}",
            "{http://www.loc.gov/mods/v3}"))[0].text
    abstract = root.findall(
        './/{0}dmdSec/{0}mdWrap/{0}xmlData/{1}mods/{1}abstract[@displayLabel="Description"]'.format(
            "{http://www.loc.gov/METS/}",
            "{http://www.loc.gov/mods/v3}"))[0].text
    return {'title': title, 'description': abstract}


def _get_datafiles(zf, include_prov=True):
    """
    return the full path within the zip file of all files that are in the data subfolder.
    """
    if include_prov:
        return filter(lambda x: (x.split(
            '/')[1] == 'data' and len(x.split('/')[-1])) or x.endswith('prov.ttl'), zf.namelist())
    else:
        return filter(lambda x: (x.split(
            '/')[1] == 'data' and len(x.split('/')[-1])), zf.namelist())


def _send_mail(
        context,
        serviceid,
        experiment_name,
        body_message,
        success=True):
    """
    Send email about success / failure to user.
    """
    fullname = context['user']['fullname'] if len(
        context['user']['fullname']) else context['user']['id']
    user_address = context['user']['email']
    if not len(user_address) > 0:
        return
    if success:
        TMPL = TMPL_EMAIL_SUCCESS
        body = TMPL.format(
            fullname=fullname,
            experiment_name=experiment_name,
            message=body_message.strip(),
            service_name=serviceid.title())
        msg = MIMEText(body)
        msg['Subject'] = "Your upload to {0} is complete".format(
            serviceid.title())
    else:
        TMPL = TMPL_EMAIL_FAILURE
        body = TMPL.format(
            fullname=fullname,
            experiment_name=experiment_name,
            message=body_message.strip(),
            service_name=serviceid.title())
        msg = MIMEText(body)
        msg['Subject'] = "Your upload to {0} failed".format(serviceid.title())
    msg['From'] = "Biodiversity & Climate Change Virtual Lab <bccvl@griffith.edu.au>"
    msg['To'] = user_address

    server = smtplib.SMTP("localhost")
    # server = smtplib.SMTP("smtp.griffith.edu.au")
    server.sendmail("bccvl@griffith.edu.au", user_address, msg.as_string())
    server.quit()


def export_figshare(zipurl, serviceid, context):
    """
    Export dataset to figshare via the figshare rest api (oauth1)
    """

    client_tokens, access_tokens = _get_oauth_tokens(
        serviceid, context['user']['id'])
    oauth_tokens = client_tokens
    oauth_tokens.update(access_tokens)
    oauth_tokens['resource_owner_key'] = oauth_tokens.get('oauth_token')
    oauth_tokens['resource_owner_secret'] = oauth_tokens.get(
        'oauth_token_secret')
    del(oauth_tokens['oauth_token'])
    del(oauth_tokens['oauth_token_secret'])
    oauth_tokens['signature_type'] = 'auth_header'
    oauth = OAuth1(**oauth_tokens)

    zf = _get_zip(zipurl)
    metadata = _get_metadata(zf)
    metadata.update({'defined_type': 'fileset'})

    client = requests.session()

    # create article
    body = metadata
    headers = {'content-type': 'application/json'}
    response = client.post(
        'http://api.figshare.com/v1/my_data/articles',
        auth=oauth,
        data=json.dumps(body),
        headers=headers)
    try:
        article_metadata = json.loads(response.content)
        if response.status_code != 200:
            raise Exception(
                "http status code {0}".format(
                    response.status_code))
    except Exception as e:
        msg = "Error creating article: {0} - response: {1}".format(
            str(e), str(response.content))
        LOG.error(msg)
        _send_mail(context, serviceid, metadata['title'], msg, success=False)
        raise e

    # add files
    data_files = _get_datafiles(zf)
    # upload one by one to avoid making one enourmous request
    for data_file in data_files:
        files = {'filedata': (split(data_file)[-1], zf.open(data_file, 'r'))}
        response = client.put(
            'http://api.figshare.com/v1/my_data/articles/{0}/files'.format(
                article_metadata['article_id']),
            auth=oauth,
            files=files,
            timeout=900)
        file_results = json.loads(response.content)
        try:
            file_results = json.loads(response.content)
            if response.status_code != 200:
                raise Exception(
                    "http status code {0}".format(
                        response.status_code))
        except Exception as e:
            msg = "Error uploading file '{0}': {1} - response: {2}".format(
                split(data_file)[-1], str(e), str(response.content))
            LOG.error(msg)
            _send_mail(
                context,
                serviceid,
                metadata['title'],
                msg,
                success=False)
            raise e

    # add link to the BCCVL
    bccvl_link = {'link': 'http://www.bccvl.org.au'}
    response = client.put(
        'http://api.figshare.com/v1/my_data/articles/{0}/links'.format(
            article_metadata['article_id']),
        auth=oauth,
        data=json.dumps(bccvl_link),
        headers=headers)
    try:
        link_results = json.loads(response.content)
    except Exception as e:
        # We can do without the link, so we'll just log this error but not fail
        # the entire upload.
        LOG.error(
            "Error adding link: {0} - response: {1}".format(str(e), str(response.content)))

    # get article info
    response = client.get(
        'http://api.figshare.com/v1/my_data/articles/{0}'.format(
            article_metadata['article_id']), auth=oauth)
    try:
        article_info = json.loads(response.content)
        if response.status_code != 200:
            raise Exception(
                "http status code {0}".format(
                    response.status_code))
    except Exception as e:
        msg = "Error getting article info: {0} - response: {1}".format(
            str(e), str(response.content))
        LOG.error(msg)
        _send_mail(context, serviceid, metadata['title'], msg, success=False)
        raise e

    try:
        article_info['items'][0][
            'preview_url'] = "http://figshare.com/preview/_preview/{0}".format(article_info['items'][0]['article_id'])
        msg = []
        for key in [
                'article_id',
                'title',
                'status',
                'published_date',
                'preview_url',
                'total_size']:
            msg.append(
                "{0}: {1}".format(
                    key.title().replace(
                        "_",
                        " "),
                    article_info['items'][0][key]))

        msg += ['', 'Files:']
        for file_item in article_info['items'][0]['files']:
            for key in ['name', 'mime_type', 'size']:
                msg.append(
                    "{0}: {1}".format(
                        key.title().replace(
                            "_", " "), file_item[key]))
            msg.append("")

        _send_mail(
            context,
            serviceid,
            metadata['title'],
            "\n".join(msg),
            success=True)
    except Exception as e:
        LOG.error("Error notifying user: {0}".format(str(e)))
        raise e


def export_googledrive(zipurl, serviceid, context):
    zf = _get_zip(zipurl)
    metadata = _get_metadata(zf)
    uploaded = []
    try:
        client_tokens, access_tokens = _get_oauth_tokens(
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
        _send_mail(context, serviceid, metadata['title'], msg, success=False)

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
            _send_mail(
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
        _send_mail(context, serviceid, metadata['title'], msg, success=False)

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
        _send_mail(context, serviceid, metadata['title'], msg, success=False)

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
        data_files = _get_datafiles(zf, include_prov=False)
        tmpdir = tempfile.mkdtemp(prefix='bccvl')
        zf.extractall(tmpdir)
        for data_file in data_files:
            mimetype = _guess_mimetype(data_file)
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
        mimetype = _guess_mimetype(mets_fn)
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
            mimetype = _guess_mimetype(prov_fn)
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
        _send_mail(
            context,
            serviceid,
            metadata['title'],
            msg,
            success=True)

    except Exception as e:
        msg = "Error uploading experiment '{0}': {1}".format(
            metadata['title'], str(e))
        LOG.error(msg)
        _send_mail(context, serviceid, metadata['title'], msg, success=False)
    finally:
        if exists(tmpdir):
            shutil.rmtree(tmpdir)


def export_dropbox(zipurl, serviceid, context):
    try:
        client_tokens, access_tokens = _get_oauth_tokens(
            serviceid, context['user']['id'])
        access_token = access_tokens['access_token']
    except Exception as e:
        msg = "Error uploading experiment '{0}' - Access Token could not be refreshed: {1}".format(
            metadata['title'],
            str(e))
        LOG.error(msg)
        _send_mail(context, serviceid, metadata['title'], msg, success=False)

    uploaded = []
    client = dropbox.client.DropboxClient(access_token)
    print 'linked account: ', client.account_info()

    zf = _get_zip(zipurl)
    metadata = _get_metadata(zf)

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
        datafiles = _get_datafiles(zf, include_prov=False)

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
        _send_mail(
            context,
            serviceid,
            metadata['title'],
            msg,
            success=True)

    except Exception as e:
        msg = "Error uploading experiment '{0}': {1}".format(
            metadata['title'], str(e))
        LOG.error(msg)
        _send_mail(context, serviceid, metadata['title'], msg, success=False)
    finally:
        if exists(tmpdir):
            shutil.rmtree(tmpdir)


def unsupported_service(zipurl, serviceid, context):
    raise NotImplementedError(
        "{} is currently not a supported service".format(serviceid))

# for local testing.


def main():
    with open('/tmp/job.json') as f:
        p = json.load(f)
    export_dropbox(p['zipurl'], p['serviceid'], p['context'])

if __name__ == "__main__":
    main()
