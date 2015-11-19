import logging
import os
from org.bccvl.movelib import move
from org.bccvl.tasks.celery import app
from org.bccvl.tasks.utils import build_source, build_destination

LOG = logging.getLogger('__name__')

@app.task()
def move(arglist, context):
    errmsgs = []
    for src, dest in move_args:
        try:
            source = build_source(src)
            destination = build_destination(dest)
            move(source, destination)

        except Exception as e:
            msg = 'Download from %s to %s failed: %s', src, dest, str(e)
            errmsgs.append(msg)
            LOG.warn(msg)
    if errmsgs:
        raise Exception('Move data failed', errmsgs)