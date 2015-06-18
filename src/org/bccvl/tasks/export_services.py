from requests import get, post

def download_file(url):
    local_filename = url.split('/')[-1]
    r = requests.get(url, stream=True)
    with open(local_filename, 'wb') as f:
        for chunk in r.iter_content(chunk_size=1024): 
            if chunk: # filter out keep-alive new chunks
                f.write(chunk)
                f.flush()
    return local_filename


def export_figshare(zipurl, serviceid, context):
    pass


def export_dropbox(zipurl, serviceid, context):
    pass


def export_googledrive(zipurl, serviceid, context):
    pass


def unsupported_service(zipurl, serviceid, context):
    raise NotImplementedError("{} is currently not a supported service".format(serviceid))
