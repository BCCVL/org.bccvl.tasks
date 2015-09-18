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
from time import sleep
from random import randint

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


def get_zip(url):
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


def get_oauth_tokens(serviceid, user):
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


def guess_mimetype(filename):
    special_types = {'Rout': 'text/plain', 'ttl': 'text/turtle'}
    return mimetypes.guess_type(filename)[0] or special_types.get(
        filename.split('.')[-1], 'application/octet-stream')


def get_metadata(zf):
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


def get_datafiles(zf, include_prov=True):
    """
    return the full path within the zip file of all files that are in the data subfolder.
    """
    if include_prov:
        return filter(lambda x: (x.split(
            '/')[1] == 'data' and len(x.split('/')[-1])) or x.endswith('prov.ttl'), zf.namelist())
    else:
        return filter(lambda x: (x.split(
            '/')[1] == 'data' and len(x.split('/')[-1])), zf.namelist())


def send_mail(
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
