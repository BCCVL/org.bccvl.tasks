from __future__ import absolute_import

from pkg_resources import resource_string
import json
from decimal import Decimal
import subprocess
import os
import shutil
import tempfile
import socket
import resource
from zipfile import ZipFile

from org.bccvl.tasks.celery import app

from org.bccvl.tasks import datamover
from celery.utils.log import get_task_logger
from multiprocessing.pool import ThreadPool


LOG = get_task_logger(__name__)


@app.task()
def r_task(params, context):
    # 1. get R wrapper
    wrapper = resource_string('org.bccvl.tasks', 'r_wrapper.sh')
    # 2. run task
    run_script(wrapper, params, context)


@app.task()
def perl_task(params, context):
    # 1. get perl wrapper
    wrapper = resource_string('org.bccvl.tasks', 'perl_wrapper.sh')
    # 2. run task
    run_script(wrapper, params, context)


def run_script(wrapper, params, context):
    try:
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
                                close_fds=True,
                                stdout=outfile, stderr=subprocess.STDOUT)
        rpid, ret, rusage = os.wait4(proc.pid, 0)
        writerusage(rusage, params)
        # TODO: check whether ret and proc.returncode are the same

        # move results back
        app.send_task("org.bccvl.tasks.plone.set_progress",
                      args=('RUNNING', 'Transferring outputs', context))
        transfer_outputs(params, context)

        # we are done here, hand over to result importer
        # build a chain of the remaining tasks
        start_import = app.signature(
            "org.bccvl.tasks.plone.set_progress",
            args=('RUNNING', 'Import results', context),
            immutable=True)

        import_job = app.signature("org.bccvl.tasks.plone.import_result",
                                   args=(params, context),
                                   immutable=True)
        import_job.link_error(
            # TODO: allow passing in result/exception of previous job
            app.signature("org.bccvl.tasks.plone.set_progress",
                          args=('FAILED', 'Result import failed',
                                context),
                          immutable=True))

        if ret != 0:
            errmsg = 'Script execution faild with exit code {0}'.format(ret)
            finish_job = app.signature(
                "org.bccvl.tasks.plone.set_progress",
                args=('FAILED', errmsg, context),
                immutable=True)
        else:
            finish_job = app.signature(
                "org.bccvl.tasks.plone.set_progress",
                args=('COMPLETED', 'Task succedded', context),
                immutable=True)

        (start_import | import_job | finish_job).delay()

    except Exception as e:
        # TODO: capture stacktrace
        # need to start import to get import cleaned up
        start_import = app.signature(
            "org.bccvl.tasks.plone.set_progress",
            args=('RUNNING', 'Import results', context),
            immutable=True)

        import_job = app.signature("org.bccvl.tasks.plone.import_result",
                                   args=(params, context),
                                   immutable=True)
        import_job.link_error(
            # TODO: allow passing in result/exception of previous job
            app.signature("org.bccvl.tasks.plone.set_progress",
                          args=('FAILED', 'Result import failed',
                                context),
                          immutable=True))
        finish_job = app.signature(
            "org.bccvl.tasks.plone.set_progress",
            args=('FAILED', 'Task failed {}'.format(repr(e)), context),
            immutable=True)
        (start_import | import_job | finish_job).delay()
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

    tp = ThreadPool(3)
    result = tp.map(download_input, move_tasks.items())
    tp.close()
    tp.join()

    for key in (k for k, success in result if success):
        # iterate over all successful downloads
        # and remove from job list for data_mover
        del move_tasks[key]
    if move_tasks:
        # still some jobs left?
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
            elif ip.get('filename', '').endswith('.zip'):
                # TODO: this comparison is suboptimal
                # if it's a zip and there is no zippath entry, we unpack the whole zipfile
                zipf = ZipFile(ip['filename'])
                extractpath = os.path.dirname(ip['filename'])
                zipf.extractall(extractpath)
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
            'scp://bccvl@' + get_public_ip() + srcpath,
            'scp://plone@127.0.0.1' + destpath)
        )
    # call move task directly, to run it in process
    datamover.move(move_tasks, context)


def get_move_args(file_descr, params, context):
    #
    # make sure we have a unique place for each file (filenames
    # may be identical for different downloads)
    inputdir = os.path.join(params['env']['inputdir'], file_descr['uuid'])
    os.mkdir(inputdir)
    src = file_descr['internalurl']
    destfile = os.path.join(inputdir, file_descr['filename'])
    dest = 'scp://bccvl@' + get_public_ip() + destfile
    # update params with local filename
    file_descr['filename'] = destfile
    return {'args': (src, dest),
            'filename': destfile}


def get_public_ip():
    # check if the environment variable EXT_IP has some useful value
    ip = os.environ.get('EXT_IP', None)
    if ip:
        return ip
    # otherwise we connect to some host, and check which local ip the socket uses
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(('google.com', 80))
        # TODO: could do name lookup with socket.gethostbyaddr('ip')[0]
        #       or socket.getnameinfo(s.getsockname())[0]
        #       namelookup may throw another exception?
        return s.getsockname()[0]
    except Exception as e:
        LOG.warn("couldn't connect to google.com: %s", repr(e))
    # we still have no clue, let's try it via hostname
    try:
        return socket.gethostbyname(socket.gethostname())
    except Exception as e:
        LOG.warn("couldn't resolve '%s': %s", socket.gethostname(), repr(e))
    # last chance
    return socket.getfqdn()


def decimal_encoder(o):
    if isinstance(o, Decimal):
        return float(o)
    raise TypeError(repr(o) + " is not JSON serializable")


def writerusage(rusage, params):
    names = ('ru_utime', 'ru_stime',
             'ru_maxrss', 'ru_ixrss', 'ru_idrss', 'ru_isrss',
             'ru_minflt', 'ru_majflt', 'ru_nswap', 'ru_inblock', 'ru_oublock',
             'ru_msgsnd', 'ru_msgrcv', 'ru_nsignals', 'ru_nvcsw', 'ru_nivcsw')
    procstats = {'rusage': dict(zip(names, rusage))}
    # correct ru_maxrss which is in pages and we want it in bytes
    # Note: to get correct pagesize this needs to run on the same
    #       machine where rusage stats came from
    procstats['rusage']['ru_maxrss'] *= resource.getpagesize()
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


def download_input(args):
    from urllib import urlretrieve
    key, args = args
    src = args['args'][0]
    destpath = args['filename']
    try:
        filename, info = urlretrieve(src, destpath)
    except Exception:
        LOG.info('Download from %s to %s failed - trying data_mover next',
                 src, destpath)
        return (key, False)
    LOG.info('Download from %s to %s succeeded.', src, destpath)
    return (key, True)
