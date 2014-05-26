from __future__ import absolute_import

from pkg_resources import resource_string
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


@app.task(bind=True)
def r_task(self, params, context):
    # 1. get R wrapper
    wrapper = resource_string('org.bccvl.tasks', 'r_wrapper.sh')
    # 2. run task
    run_script(wrapper, params, context)


@app.task(bind=True)
def perl_task(params, context):
    # 1. get perl wrapper
    wrapper = resource_string('org.bccvl.tasks', 'perl_wrapper.sh')
    # 2. run task
    run_script(wrapper, params, context)


def run_script(wrapper, params, context):
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
        scriptname = create_scripts(params, context)
        # run the script
        app.send_task("org.bccvl.tasks.plone.set_progress",
                      args=('RUNNING', 'Executing job', context))
        scriptout = os.path.join(params['env']['outputdir'],
                                 params['worker']['script']['name'] + 'out')
        outfile = open(scriptout, 'w')
        wrapsh = os.path.join(params['env']['scriptdir'], 'wrap.sh')
        open(wrapsh, 'w').write(wrapper)
        cmd = ["/bin/bash", "-l", "wrap.sh", scriptname]
        LOG.info("Executing: %s", ' '.join(cmd))
        proc = subprocess.Popen(cmd, cwd=params['env']['scriptdir'],
                                stdout=outfile, stderr=subprocess.STDOUT)
        rpid, ret, rusage = os.wait4(proc.pid, 0)
        writerusage(rusage, params)
        # TODO: check whether ret and proc.returncode are the same

        # move results back
        app.send_task("org.bccvl.tasks.plone.set_progress",
                      args=('RUNNING', 'Transferring outputs', context))
        transfer_outputs(params, context)

        # we are done here, hand over to result importer
        app.send_task("org.bccvl.tasks.plone.import_result",
                      args=(params, context))

        if ret != 0:
            errmsg = 'Script execution faild with exit code {0}'.format(ret)
            app.send_task("org.bccvl.tasks.plone.set_progress",
                          args=('FAILED', errmsg, context))
            raise Exception(errmsg)
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

    # all move_tasks collected; call move task directly (don't send to queue)
    datamover.move([task['args'] for task in move_tasks.values()],
                   context)

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


def create_scripts(params, context):
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
        return scriptname


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
    # call move task directly, to run it in process
    datamover.move(move_tasks, context)


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


def writerusage(rusage, params):
    names = ('ru_utime', 'ru_stime',
             'ru_maxrss', 'ru_ixrss', 'ru_idrss', 'ru_isrss',
             'ru_minflt', 'ru_majflt', 'ru_nswap', 'ru_inblock', 'ru_oublock',
             'ru_msgsnd', 'ru_msgrcv', 'ru_nsignals', 'ru_nvcsw', 'ru_nicsw')
    procstats = {'rusage': dict(zip(names, rusage))}
    statsfile = open(os.path.join(params['env']['outputdir'],
                                  'pstats.json'),
                     'w')
    json.dump(procstats, statsfile, default=decimal_encoder,
              sort_keys=True, indent=4)
    statsfile.close()
    # cputime= utime+ stime (virtual cpu time)
    # average unshared data size: idrss/cputime+ isrss/cputime (wall could be 0)
    # Wall: separate elapsed timer?
    # average mem usage idrss/cputime+isrss/cputime+ixrss+cputime
    # %cpu= (cputime*100/elapsed time)
    # average shared text: ixrss/cputime
    # average stack segments: isrss/cputime
    # average resident set size: idrss/cputime
    # maybe I can get start time from subprocess object?
    # elapsed=end (gettimeofday after wait) - start (gettimeofday call before fork)
