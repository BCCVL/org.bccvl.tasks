import Cookie
import logging
from time import time
import socket
import struct
import hashlib
from urlparse import urlsplit

from org.bccvl.tasks.celery import app
from org.bccvl.tasks.mdextractor import MetadataExtractor


LOG = logging.getLogger(__name__)


def set_progress(state, statusmsg, context):
    app.signature("org.bccvl.tasks.plone.set_progress",
                  args=(state, statusmsg, context)).delay()


def import_cleanup(results_dir, context):
    app.signature("org.bccvl.tasks.plone.import_cleanup",
                  args=(results_dir, context)).delay()


def set_progress_job(state, statusmsg, context):
    return app.signature("org.bccvl.tasks.plone.set_progress",
                         args=(state, statusmsg, context),
                         immutable=True)


def import_result_job(items, params, context):
    return app.signature("org.bccvl.tasks.plone.import_result",
                         args=(items, params, context),
                         immutable=True)


def import_file_metadata_job(items, params, context):
    return app.signature("org.bccvl.tasks.plone.import_file_metadata",
                         args=(items, params, context),
                         immutable=True)


def import_ala_job(items, params, context):
    return app.signature("org.bccvl.tasks.plone.import_ala",
                         args=(items, params, context),
                         immutable=True)


def import_cleanup_job(results_dir, context):
    return app.signature("org.bccvl.tasks.plone.import_cleanup",
                         args=(results_dir, context),
                         immutable=True)


def extract_metadata(filepath, filect):
    mdextractor = MetadataExtractor()
    try:
        return mdextractor.from_file(filepath, filect)
    except Exception as ex:
        LOG.warn("Couldn't extract metadata from file: %s : %s", filepath, repr(ex))
        raise


def traverse_dict(source, path):
    current = source
    try:
        for el in path.split('/'):
            if isinstance(current, list):
                el = int(el)
            current = current[el]
    except:
        # TODO: at least log error?
        current = None
    return current


class AuthTkt(object):
    def __init__(self, secret, uid, data='', ip='0.0.0.0', tokens=(),
                 base64=True, ts=None):
        self.secret = str(secret)
        self.uid = str(uid)
        self.data = data
        self.ip = ip
        self.tokens = ','.join(tok.strip() for tok in tokens)
        self.base64 = base64
        self.ts = int(time() if ts is None else ts)

    def ticket(self):
        v = self.cookie_value()
        if self.base64:
            return v.encode('base64').strip().replace('\n', '')
        return v

    def cookie(self, name, **kwargs):
        name = str(name)
        c = Cookie.SimpleCookie()
        c[name] = self.ticket()

        kwargs.setdefault('path', '/')
        c[name].update(kwargs)

        return c

    def cookie_value(self):
        parts = ['%s%08x%s' % (self._digest(), self.ts, self.uid)]
        if self.tokens:
            parts.append(self.tokens)
        parts.append(self.data)
        return '!'.join(parts)

    def _digest(self):
        return hashlib.md5(self._digest0() + self.secret).hexdigest()

    def _digest0(self):
        parts = (self._encode_ip(self.ip), self._encode_ts(self.ts),
                 self.secret, self.uid, '\0', self.tokens, '\0', self.data)
        return hashlib.md5(''.join(parts)).hexdigest()

    def _encode_ip(self, ip):
        return socket.inet_aton(ip)

    def _encode_ts(self, ts):
        return struct.pack('!I', ts)


def get_cookies(settings, userid):
    if not settings.get('secret'):
        return {}
    ticket = AuthTkt(settings['secret'], userid)
    return {
        'name': settings['name'],
        'value': ticket.ticket(),
        'domain': settings['domain'],
        'path': settings.get('path', '/'),
        'secure': settings.get('secure', True),
    }


def build_source(src, userid=None, settings=None):
    source = {'url': src}
    # Create a cookies for http download from the plone server
    url = urlsplit(src)
    if settings is None:
        settings = {}
    if url.scheme in ('http', 'https'):
        cookie_settings = settings.get('cookie', {})
        if url.hostname == cookie_settings.get('domain'):
            source['cookies'] = get_cookies(cookie_settings,
                                            userid)
        source['verify'] = settings.get('ssl', {}).get('verify', True)
    elif url.scheme in ('swift+http', 'swift+https'):
        # TODO: should check swift host name as well
        swift_settings = settings.get('swift', {})
        for key in ('os_auth_url', 'os_username', 'os_password', 'os_tenant_name'):
            if key in swift_settings:
                source[key] = swift_settings[key]
    return source


def build_destination(dest, settings=None):
    destination = {'url': dest}

    # Create a cookies for http download from the plone server
    url = urlsplit(dest)
    if url.scheme in ('swift+http', 'swift+https'):
        # TODO: should check swift host name as well
        # FIXME: assumes settings is not None
        swift_settings = settings and settings.get('swift', {}) or {}
        for key in ('os_auth_url', 'os_username', 'os_password', 'os_tenant_name'):
            if key not in swift_settings:
                continue
            destination[key] = swift_settings[key]
    return destination
