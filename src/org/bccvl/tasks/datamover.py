from __future__ import absolute_import

import logging
import time
import os
from httplib import HTTPSConnection
import socket
import xmlrpclib
import ssl
from urlparse import urlsplit
from backports.ssl_match_hostname import match_hostname, CertificateError
from org.bccvl.tasks.celery import app

LOG = logging.getLogger('__name__')


class VerifyingHTTPSConnection(HTTPSConnection):
    """A httplib HTTPSConnection that sends a client cert and verifies the
    host certificate

    """

    def __init__(self, host, port=None, key_file=None, cert_file=None,
                 strict=None, timeout=socket._GLOBAL_DEFAULT_TIMEOUT,
                 source_address=None, ca_certs=None):
        HTTPSConnection.__init__(self, host, port,
                                 key_file, cert_file, strict,
                                 timeout, source_address)
        self.ca_certs = ca_certs

    def connect(self):
        "Connect to a host on a given (SSL) port."

        sock = socket.create_connection((self.host, self.port),
                                        self.timeout, self.source_address)
        if self._tunnel_host:
            self.sock = sock
            self._tunnel()
        # verify server cert against ca_certs
        self.sock = ssl.wrap_socket(sock,
                                    cert_reqs=ssl.CERT_REQUIRED,
                                    ssl_version=ssl.PROTOCOL_TLSv1,
                                    ca_certs=self.ca_certs,
                                    keyfile=self.key_file,
                                    certfile=self.cert_file)
        # match hostnome
        match_hostname(self.sock.getpeercert(), self.host)


class SSLSafeTransport(xmlrpclib.SafeTransport):
    """A xmlrpclib Transport which uses VerifyingHTTSPConnection ta talk
    to a xmlpc server

    """

    # host can be a tuple .... ('url', {sslparams})
    # sslparams: see abov what VerifyingHTTPSConnection accets and wrap_socket
    def __init__(self, ca_certs, keyfile, certfile):
        xmlrpclib.SafeTransport.__init__(self)
        self.ca_certs = ca_certs
        self.keyfile = keyfile
        self.certfile = certfile

    def make_connection(self, host):
        # FIXME: should support non ssl connections as well here
        if self._connection and host == self._connection[0]:
            return self._connection[1]
        # create a HTTPS connection object from a host descriptor
        # host may be a string, or a (host, x509-dict) tuple
        chost, self._extra_headers, _ = self.get_host_info(host)
        # TODO: inconsistent naming,...
        #       HTTPSConnection uses key_file, but sslwrap uses
        #       keyfile
        x509 = {'ca_certs': self.ca_certs,
                'key_file': self.keyfile,
                'cert_file': self.certfile}
        self._connection = (host,
                            VerifyingHTTPSConnection(chost, None,
                                                     **(x509 or {})))
        return self._connection[1]


class VerifyingServerProxy(xmlrpclib.ServerProxy):

    def __init__(self, uri, transport=None, encoding=None, verbose=0,
                 allow_none=0, use_datetime=0, x509=None):
        # in case we have a ssl url and x509 config, we use SSLSafeTransport
        scheme = urlsplit(uri).scheme
        if transport is None and scheme.lower() == 'https' and x509:
            transport = SSLSafeTransport(**x509)
        xmlrpclib.ServerProxy.__init__(self, uri, transport, encoding, verbose,
                                       allow_none, use_datetime)


class DataMover(object):
    """A little helper class to talk to the data mover.
    """

    active_states = ('PENDING', 'IN_PROGRESS')
    failed_states = ('FAILED', 'REJECTED')
    success_states = ('COMPLETED', )

    def __init__(self):
        self._proxy = None
        # TODO: get data_mover location from config file
        #       or some other discovery mechanism
        # FIXME: make cerfiles configurable
        self.url = os.environ.get('DATA_MOVER',
                                  u'http://127.0.0.1:10700/data_mover')
        self.ca_certs = os.environ.get('DATA_MOVER_CA',
                                       '/etc/pki/tls/certs/bccvlca.crt.pem')
        self.keyfile = os.environ.get('DATA_MOVER_CERT',
                                      '/home/bccvl/worker/worker.key.pem')
        self.certfile = os.environ.get('DATA_MOVER_KEY',
                                       '/home/bccvl/worker/worker.crt.pem')

    @property
    def proxy(self):
        if self._proxy is None:
            x509 = {
                'ca_certs': self.ca_certs,
                'keyfile': self.keyfile,
                'certfile': self.certfile
            }
            self._proxy = VerifyingServerProxy(self.url, x509=x509)
        return self._proxy

    def move(self, move_args):
        states = []
        for src, dest in move_args:
            states.append(self.proxy.move(src, dest))
        return states

    def wait(self, states, sleep=10):
        waiting = True
        while waiting:
            waiting = False
            newstates = []
            time.sleep(sleep)
            for state in states:
                if state['status'] in self.active_states:
                    state = self.check_move_status(state['id'])
                    waiting = True
                newstates.append(state)
            states = newstates
        return states

    def check_move_status(self, job_id):
        ret = self.proxy.check_move_status(job_id)
        return ret


@app.task()
def move(arglist, context):
    dm = DataMover()
    states = dm.move(arglist)
    states = dm.wait(states)
    errmsgs = []
    for state in states:
        if state['status'] in dm.failed_states:
            errmsgs.append('Move job {0} failed: {1}: {2}'.format(
                state.get('id', -1), state['status'], state['reason']))
        elif state['status'] not in dm.success_states:
            # TODO: something weird happened, either there is an unknown state,
            #       or something went terribly wrong.
            LOG.warn("Unknown or incomplete state after data move: [%s] %s: %s",
                     state.get('id', -1), state['status'], state.get('reason', ''))
    if errmsgs:
        raise Exception('One or more move jobs failed', errmsgs)


@app.task()
def export_result(zipurl, serviceid, context):
    # TODO: make sure to clean all temporary up

    # 1. download zip file from zip url

    # 2. fetch oauth token 

    # 3. fetch oauth config for serviceid ?

    # 4. upload to service

    # 5. raise on error, or nothing
    
    pass
