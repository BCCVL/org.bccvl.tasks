from time import time
import socket
import struct
import hashlib
from urlparse import urlsplit


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
    if url.scheme in ('http', 'https'):
        source['cookies'] = get_cookies(settings.get('cookie', {}),
                                        userid)
        source['verify'] = settings.get('ssl', {}).get('verify', True)
    # elif url.scheme == 'swift':
    #     source['auth'] = 'https://keystone.rc.nectar.org.au:5000/v2.0/'
    #     source['user'] = 'username@griffith.edu.au'
    #     source['key'] = 'password'
    #     source['os_tenant_name'] = 'pt-12345'
    #     source['auth_version'] = '2'
    return source


def build_destination(dest, filename=None):
    destination = {'url': dest}
    if filename:
        destination['filename'] = filename

    # Create a cookies for http download from the plone server
    url = urlsplit(dest)
    if url.scheme == 'swift':
        destination['auth'] = 'https://keystone.rc.nectar.org.au:5000/v2.0/'
        destination['user'] = 'username@griffith.edu.au'
        destination['key'] = 'password'
        destination['os_tenant_name'] = 'pt-12345'
        destination['auth_version'] = '2'
    return destination
