from __future__ import absolute_import

from xmlrpclib import ServerProxy
import logging
import time
import os

from org.bccvl.tasks.celery import app

LOG = logging.getLogger('__name__')


class DataMover(object):
    # TODO: depending on url discovery it wolud be possible
    #       to register this as utility factory and keep a
    #       serverproxy instance for the lifetime of this utility
    # TODO: call to xmlrpc server might also throw socket
    #       errors. (e.g. socket.error: [Errno 61] Connection refused)

    url = os.environ.get('DATA_MOVER',
                         u'http://127.0.0.1:10700/data_mover')

    active_states = ('PENDING', 'IN_PROGRESS')
    failed_states = ('FAILED', 'REJECTED')
    success_states = ('COMPLETED', )

    def __init__(self):
        # TODO: get data_mover location from config file
        #       or some other discovery mechanism
        self.proxy = ServerProxy(self.url)

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


# TODO: these jobs need access to a datamover instance. they way be long running,
#       current job chaining requires that a move tasks runs as long as the move job itself takes
#       also the datamover doesn't support job finished notification
# TODO: look at using routing keys to utilise multiple data movers
@app.task(throws=(Exception, ), bind=True)
def move(self, arglist, context):
    # TODO: call datamover, wait for result, return state (success, error)
    app.send_task("org.bccvl.tasks.plone.set_progress",
                  args=('RUNNING', 'DOWNLOAD', context))
    dm = DataMover()
    states = dm.move(arglist)
    states = dm.wait(states)
    errmsgs = []
    for state in states:
        if state['status'] in dm.failed_states:
            errmsgs.append('Move job {0} failed: {1}: {2}'.format(
                state.get('id', -1), state['status'], state['reason']))
    if errmsgs:
        # FIXME we rely here on failed state, but if this code is skipped for some reason, the ui will never see the failed state.-> do this in task_chain
        app.send_task("org.bccvl.tasks.plone.set_progress",
                      args=('FAILED', '\n'.join(errmsgs), context))
        raise Exception('One or more move jobs failed')
