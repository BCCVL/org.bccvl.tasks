from __future__ import absolute_import

import json
from decimal import Decimal
import subprocess
import os
import shutil
import tempfile
from zipfile import ZipFile

from org.bccvl.tasks.celery import app

from org.bccvl.tasks import datamover
from celery.utils.log import get_task_logger


LOG = get_task_logger(__name__)

# FIXME: in case of any errors, we still have to capture the log output and clean up import temp folder


# ALA import is a chain of tasks
# 1. start download
# 2. initate import

# TODO:  this is a very short lived task and can basically run anywhere
#        maybe put in default queue?
# TODO: could add callbacks with each task. It would be possible to modify
#       list of callbacks and errbacks within each task

# submit after transaction commit, otherwise we may submit it
# multiple times in case of conflicterrors, or kick off task for
# non existent content, in case task starts before this transaction
# commits
#@app.task(base=plone.AfterCommitTask, bind = True)
@app.task(bind=True)
def sdm_task(self, params, context):
    """
    lsid .. species id
    path ... destination path for ala import files
    context ... a dictionary with keys:
      - context: path to context object
      - userid: zope userid
    """
    try:

        app.send_task("org.bccvl.tasks.plone.set_progress",
                      args=('SUBMITTED', 'SUBMITTED', context))

        app.send_task("org.bccvl.tasks.plone.set_progress",
                      args=('RUNNING', 'Transferring data', context))
        # create initial folder structure
        create_workenv(params)
        # transfer input files
        transfer_inputs(params, context)
        # create script
        scriptname = os.path.join(params['env']['scriptdir'],
                                  params['worker']['script']['name'])
        scriptfile = open(scriptname, 'w')
        scriptfile.write(params['worker']['script']['script'])
        scriptfile.close()
        # write params.json
        jsonfile = open(os.path.join(params['env']['scriptdir'],
                                     'params.json'),
                        'w')
        json.dump(params, jsonfile, default=decimal_encoder,
                  sort_keys=True, indent=4)
        jsonfile.close()

        # run the script
        app.send_task("org.bccvl.tasks.plone.set_progress",
                      args=('RUNNING', 'Executing job', context))
        scriptout = os.path.join(params['env']['outputdir'],
                                 scriptname + 'out')
        #cmd = ["R", "CMD", "BATCH", "--vanilla", scriptname, scriptout]
        # Don't use--vanilla as it prohibits loading .Renviron which we use to find pre-installed rlibs .... may pre install them in some location as root to avoid modification?
        #cmd = ["R", "CMD", "BATCH", "--no-save", "--no-restore", scriptname, scriptout]
        outfile = open(scriptout, 'w')
        cmd = ["Rscript", "--no-save", "--no-restore", scriptname]
        LOG.info("Executing: %s", ' '.join(cmd))
        proc = subprocess.Popen(cmd, cwd=params['env']['scriptdir'],
                                stdout=outfile, stderr=outfile)
        # capture process statistics here
        ret = proc.wait()
        outfile.close()
        if ret != 0:
            errmsg = 'Script execution faild with exit code {0}'.format(ret)
            app.send_task("org.bccvl.tasks.plone.set_progress",
                          args=('FAILED', errmsg, context))
            raise Exception(errmsg)

        # move results back
        app.send_task("org.bccvl.tasks.plone.set_progress",
                      args=('RUNNING', 'Transferring outputs', context))
        transfer_outputs(params, context)

        # we are done here, hand over to result importer
        app.send_task("org.bccvl.tasks.plone.import_result",
                      args=(params, context))
    except Exception as e:
        # TODO: capture stacktrace
        app.send_task("org.bccvl.tasks.plone.set_progress",
                      args=('FAILED', 'SDM failed {}'.format(repr(e)),
                            context))
        raise e
    finally:
        # TODO:  check if dir exists
        path = params['env'].get('workdir', None)
        if path and os.path.exists(path):
            shutil.rmtree(path)


def create_workenv(params):
    ### create worker directories and update env section in params
    root = os.environ.get('WORKDIR') or os.environ.get('HOME', None)
    workdir = tempfile.mkdtemp(dir=root)
    params['env'].update({
        'workdir': workdir,
        'inputdir': os.path.join(workdir, 'input'),
        'scriptdir': os.path.join(workdir, 'script'),
        'outputdir': os.path.join(workdir, 'output')
    })
    os.mkdir(params['env']['inputdir'])
    os.mkdir(params['env']['scriptdir'])
    os.mkdir(params['env']['outputdir'])


def transfer_inputs(params, context):
    move_tasks = {}  # list af move job arguments
    # go through params['worker']['files'] and place files in local location
    for input in params['worker']['files']:
        input_param = params['params'][input]

        if not isinstance(input_param, list):
            # if it's not a list of files make it so
            input_param = [input_param]
        # we have a list of files
        for ip in input_param:
            # the paramater may actually be None
            if ip is None:
                continue
            # is the file already scheduled?
            if ip['uuid'] in move_tasks:
                # we have this file already, so copy it's destination
                ip['filename'] = move_tasks[ip['uuid']]['filename']
                continue
            move_tasks[ip['uuid']] = get_move_args(ip, params, context)

    # all move_tasks collected; kick of move
    dm = datamover.DataMover()
    states = dm.move(
        [task['args'] for task in move_tasks.values()])
    states = dm.wait(states)
    errmsgs = []
    for state in states:
        if state['status'] in dm.failed_states:
            errmsgs.append('Move job {0} failed: {1}: {2}'.format(
                state.get('id', '0'), state['status'], state['reason']))
    if errmsgs:
        app.send_task("org.bccvl.tasks.plone.set_progress",
                      args=('FAILED', '\n'.join(errmsgs), context))
        raise Exception('One or more move jobs failed')

    # all data successfully transferred
    # unpack all zip files and update filenames to local files
    for input in params['worker']['files']:
        input_param = params['params'][input]

        if not isinstance(input_param, list):
            # if it's not a list of files make it so
            input_param = [input_param]
        for ip in input_param:
            if ip is None:
                continue
            # extract file from zip
            if 'zippath' in ip:
                zipf = ZipFile(ip['filename'])
                # extract next to zipfile
                extractpath = os.path.dirname(ip['filename'])
                zipf.extract(ip['zippath'], extractpath)
                # replace filename with extracted filename
                ip['filename'] = os.path.join(extractpath, ip['zippath'])
            # if it's not a zip, then there is nothing to do


def transfer_outputs(params, context):
    move_tasks = []
    for fpath in os.listdir(params['env']['outputdir']):
        srcpath = os.path.join(params['env']['outputdir'],
                               fpath)
        destpath = os.path.join(params['result']['results_dir'],
                                fpath)
        move_tasks.append((
            {'type': 'scp',
             'host': get_public_ip(),
             'path': srcpath},
            {'type': 'scp',
             'host': 'plone',
             'path': destpath})
        )
    dm = datamover.DataMover()
    states = dm.move(move_tasks)
    states = dm.wait(states)
    errmsgs = []
    for state in states:
        if state['status'] in dm.failed_states:
            errmsgs.append('Move job {0} failed: {1}: {2}'.format(
                state.get('id', '0'), state['status'], state['reason']))
    if errmsgs:
        app.send_task("org.bccvl.tasks.plone.set_progress",
                      args=('FAILED', '\n'.join(errmsgs), context))
        raise Exception('One or more move jobs failed')


def get_move_args(file_descr, params, context):
    #
    # make sure we have a unique place for each file (filenames
    # may be identical for different downloads)
    inputdir = os.path.join(params['env']['inputdir'], file_descr['uuid'])
    os.mkdir(inputdir)
    src = {
        'type': 'url',
        'url': file_descr['internalurl']}
    destfile = os.path.join(inputdir, file_descr['filename'])
    dest = {
        'type': 'scp',
        'host': get_public_ip(),
        'path': destfile}
    # update params with local filename
    file_descr['filename'] = destfile
    return {'args': (src, dest),
            'filename': destfile}


def get_public_ip():
    import socket
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(('google.com', 80))
    return s.getsockname()[0]


def decimal_encoder(o):
    if isinstance(o, Decimal):
        return float(o)
    raise TypeError(repr(o) + " is not JSON serializable")


# FIXME: enormous amount of duplicate code here (see sdm_task)
@app.task(bind=True)
def biodiverse_task(self, params, context):
    """
    lsid .. species id
    path ... destination path for ala import files
    context ... a dictionary with keys:
      - context: path to context object
      - userid: zope userid
    """
    try:

        app.send_task("org.bccvl.tasks.plone.set_progress",
                      args=('SUBMITTED', 'SUBMITTED', context))

        app.send_task("org.bccvl.tasks.plone.set_progress",
                      args=('RUNNING', 'Transferring data', context))
        # create initial folder structure
        create_workenv(params)
        # transfer input files
        transfer_inputs(params, context)
        # create script
        scriptname = os.path.join(params['env']['scriptdir'],
                                  params['worker']['script']['name'])
        scriptfile = open(scriptname, 'w')
        scriptfile.write(params['worker']['script']['script'])
        scriptfile.close()
        # write params.json
        jsonfile = open(os.path.join(params['env']['scriptdir'],
                                     'params.json'),
                        'w')
        json.dump(params, jsonfile, default=decimal_encoder,
                  sort_keys=True, indent=4)
        jsonfile.close()

        # run the script
        app.send_task("org.bccvl.tasks.plone.set_progress",
                      args=('RUNNING', 'Executing job', context))
        scriptout = os.path.join(params['env']['outputdir'],
                                 scriptname + 'out')
        outfile = open(scriptout, 'w')
        cmd = ["perl", scriptname]
        LOG.info("Executing: %s", ' '.join(cmd))
        proc = subprocess.Popen(cmd, cwd=params['env']['scriptdir'],
                                stdout=outfile, stderr=outfile)
        # capture process statistics here
        ret = proc.wait()
        outfile.close()
        if ret != 0:
            errmsg = 'Script execution faild with exit code {0}'.format(ret)
            app.send_task("org.bccvl.tasks.plone.set_progress",
                          args=('FAILED', errmsg, context))
            raise Exception(errmsg)

        # move results back
        app.send_task("org.bccvl.tasks.plone.set_progress",
                      args=('RUNNING', 'Transferring outputs', context))
        transfer_outputs(params, context)

        # we are done here, hand over to result importer
        app.send_task("org.bccvl.tasks.plone.import_result",
                      args=(params, context))
    except Exception as e:
        # TODO: capture stacktrace
        app.send_task("org.bccvl.tasks.plone.set_progress",
                      args=('FAILED', 'Biodiverse failed {}'.format(repr(e)),
                            context))
        raise e
    finally:
        # TODO:  check if dir exists
        path = params['env'].get('workdir', None)
        if path and os.path.exists(path):
            shutil.rmtree(path)
