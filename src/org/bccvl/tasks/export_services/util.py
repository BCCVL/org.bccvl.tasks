import requests
import tempfile
import xml.etree.ElementTree as ET
import os.path
import smtplib
from email.mime.text import MIMEText
import logging

import mimetypes

from org.bccvl import movelib
from org.bccvl.movelib.utils import build_source, build_destination, get_cookies


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


def get_files(urllist, userid, conf):
    """
    Download all files fiven in urllist to local tempfile
    return temp folder location.
    """
    dest = tempfile.mkdtemp(prefix='bccvl_export')
    for url in urllist:
        src = build_source(url, userid, conf)
        dst = build_destination('file://{0}/{1}'.format(dest, os.path.basename(url)), conf)
        movelib.move(src, dst)
    return dest


def get_oauth_tokens(siteurl, serviceid, user, conf):
    """
    Acquire user and service related OAUTH tokens from the BCCVL
    """
    cookie = get_cookies(conf.get('cookie', {}), user)
    verify = conf.get('ssl', {}).get('verify', True)
    s = requests.Session()
    if cookie:
        s.cookies.set(**cookie)
    try:

        access_tokens = s.get(
            "{0}/oauth/{1}/accesstoken".format(siteurl, serviceid),
            verify=verify).json()
    except Exception as e:
        LOG.error('Error getting access token: {0}'.format(str(e)), exc_info=True)
        raise e
    # get client token from configuration
    client_tokens = conf['oauth'].get(serviceid, {})
    return client_tokens, access_tokens


def guess_mimetype(filename):
    special_types = {'Rout': 'text/plain', 'ttl': 'text/turtle'}
    return mimetypes.guess_type(filename)[0] or special_types.get(
        filename.split('.')[-1], 'application/octet-stream')


def get_metadata(metsfile):
    """
    Extract title and description from the METS xml file.
    """
    if not os.path.exists(metsfile):
        raise Exception('No METS data found.')
    root = ET.parse(open(metsfile, 'r')).getroot()
    title = root.findall(
        './/{0}dmdSec/{0}mdWrap/{0}xmlData/{1}mods/{1}titleInfo[@displayLabel="Title"]/{1}title'.format(
            "{http://www.loc.gov/METS/}",
            "{http://www.loc.gov/mods/v3}"))[0].text
    abstract = root.findall(
        './/{0}dmdSec/{0}mdWrap/{0}xmlData/{1}mods/{1}abstract[@displayLabel="Description"]'.format(
            "{http://www.loc.gov/METS/}",
            "{http://www.loc.gov/mods/v3}"))[0].text
    return {'title': title, 'description': abstract}


def get_datafiles(tmpdir, include_prov=True):
    """
    return the full path within the zip file of all files that are in the data subfolder.
    """
    if include_prov:
        return ('{0}/{1}'.format(tmpdir, fname) for fname in
                os.listdir(tmpdir) if fname != 'mets.xml')
    else:
        return ('{0}/{1}'.format(tmpdir, fname) for fname in
                os.listdir(tmpdir) if fname not in ('mets.xml', 'prov.ttl'))


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
