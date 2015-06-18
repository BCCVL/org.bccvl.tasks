import requests
from requests_oauthlib import OAuth1
import json
import tempfile
import zipfile
import xml.etree.ElementTree as ET
from os.path import split

def _get_zip(url):
    r = requests.get(url, stream=True)
    f = tempfile.NamedTemporaryFile(prefix="bccvl_export", dir="/tmp", delete=True)    
    for chunk in r.iter_content(chunk_size=1024): 
        if chunk: # filter out keep-alive new chunks
            f.write(chunk)
            f.flush()
    f.seek(0)
    return zipfile.ZipFile(f, 'r')

def _get_tokens(serviceid, user):
    tokens = {}
    tokens.update( requests.get("http://127.0.0.1:8201/bccvl/oauth/figshare/accesstoken?user={}".format(user)).json() )
    tokens.update( requests.get("http://127.0.0.1:8201/bccvl/oauth/figshare/clienttoken".format(user)).json() )
    return tokens

def _get_metadata(zf):
    try:
        fn = filter(lambda x: x.endswith('mets.xml'), zf.namelist())[0]
    except Exception as e:
        raise Exception('No METS data found.')
    root = ET.parse(zf.open(fn,'r')).getroot()
    title = root.findall('.//{0}dmdSec/{0}mdWrap/{0}xmlData/{1}mods/{1}titleInfo[@displayLabel="Title"]/{1}title'.format("{http://www.loc.gov/METS/}","{http://www.loc.gov/mods/v3}"))[0].text
    abstract = root.findall('.//{0}dmdSec/{0}mdWrap/{0}xmlData/{1}mods/{1}abstract[@displayLabel="Description"]'.format("{http://www.loc.gov/METS/}","{http://www.loc.gov/mods/v3}"))[0].text
    return {'title':title, 'description':abstract}

def _get_datafiles(zf):
    return filter( lambda x: x.split('/')[1] == 'data', zf.namelist() )

def export_figshare(zipurl, serviceid, context):
    oauth_tokens = _get_tokens(serviceid, context['user']['id'])
    oauth_tokens['resource_owner_key'] = oauth_tokens.get('oauth_token')
    oauth_tokens['resource_owner_secret'] = oauth_tokens.get('oauth_token_secret')
    del(oauth_tokens['oauth_token'])
    del(oauth_tokens['oauth_token_secret'])
    oauth_tokens['signature_type'] = 'auth_header'
    oauth = OAuth1(**oauth_tokens)

    zf = _get_zip(zipurl)
    metadata = _get_metadata(zf)
    metadata.update({'defined_type':'fileset'})

    client = requests.session()

    # create article
    body = metadata
    headers = {'content-type':'application/json'}
    response = client.post('http://api.figshare.com/v1/my_data/articles', auth=oauth, data=json.dumps(body), headers=headers)
    article_metadata = json.loads(response.content)

    # add files
    data_files = _get_datafiles(zf)
    # upload one by one to avoid making one enourmous request
    for data_file in data_files:
        files = {'filedata':(split(data_file)[-1], zf.open(data_file,'r'))}
        response = client.put('http://api.figshare.com/v1/my_data/articles/{0}/files'.format(article_metadata['article_id']), auth=oauth, files=files)
        file_results = json.loads(response.content)

    # add link to the BCCVL
    bccvl_link = {'link':'http://www.bccvl.org.au'}
    response = client.put('http://api.figshare.com/v1/my_data/articles/{0}/links'.format(article_metadata['article_id']), auth=oauth, data=json.dumps(bccvl_link), headers=headers)
    link_results = json.loads(response.content)

    ## get article info
    # response = client.get('http://api.figshare.com/v1/my_data/articles/{0}'.format(article_metadata['article_id']), auth=oauth)
    # article_info = json.loads(response.content)


def export_dropbox(zipurl, serviceid, context):
    raise NotImplementedError("{} is currently not a supported service".format(serviceid))


def export_googledrive(zipurl, serviceid, context):
    raise NotImplementedError("{} is currently not a supported service".format(serviceid))


def unsupported_service(zipurl, serviceid, context):
    raise NotImplementedError("{} is currently not a supported service".format(serviceid))
