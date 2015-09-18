import requests
from requests_oauthlib import OAuth1
import json
import tempfile
import zipfile
from os.path import split, exists, join
import logging
from datetime import datetime

import shutil

from time import sleep
from random import randint

from .util import get_zip, get_oauth_tokens, guess_mimetype, get_metadata, get_datafiles, send_mail

LOG = logging.getLogger(__name__)


def export_figshare(zipurl, serviceid, context):
    """
    Export dataset to figshare via the figshare rest api (oauth1)
    """

    client_tokens, access_tokens = get_oauth_tokens(
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

    zf = get_zip(zipurl)
    metadata = get_metadata(zf)
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
        send_mail(context, serviceid, metadata['title'], msg, success=False)
        raise e

    # add files
    data_files = get_datafiles(
        zf) + filter(lambda x: x.endswith('mets.xml'), zf.namelist())
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
            send_mail(
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
        send_mail(context, serviceid, metadata['title'], msg, success=False)
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

        send_mail(
            context,
            serviceid,
            metadata['title'],
            "\n".join(msg),
            success=True)
    except Exception as e:
        LOG.error("Error notifying user: {0}".format(str(e)))
        raise e
