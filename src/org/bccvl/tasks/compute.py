from __future__ import absolute_import

from pkg_resources import resource_string
import json
from decimal import Decimal
import subprocess
import os
import os.path
import glob
import shutil
import tempfile
import socket
import resource
from zipfile import ZipFile, ZIP_DEFLATED
from urlparse import urlparse

from org.bccvl.tasks.celery import app
from org.bccvl.movelib import move
from org.bccvl.tasks.utils import build_source, build_destination
from org.bccvl.tasks import datamover
from celery.utils.log import get_task_logger
from multiprocessing.pool import Pool
from csv import DictReader
from decimal import Decimal, InvalidOperation



LOG = get_task_logger(__name__)


def set_progress(state, statusmsg, context):
    app.send_task("org.bccvl.tasks.plone.set_progress",
                      args=(state, statusmsg, context))


def set_progress_job(state, statusmsg, context):
    return app.signature("org.bccvl.tasks.plone.set_progress",
                         args=(state, statusmsg, context),
                         immutable=True)


def import_result_job(params, context):
    return app.signature("org.bccvl.tasks.plone.import_result",
                         args=(params, context),
                         immutable=True)


def import_cleanup_job(params, context):
    return app.signature("org.bccvl.tasks.plone.import_cleanup",
                         args=(params['result']['results_dir'], context),
                         immutable=True)


def zip_folder(archive, folder):
    # We'll keep folder as root in the zip file
    # and rootdir will be the folder that contains folder
    archivename = os.path.basename(archive)
    rootdir = os.path.dirname(folder)
    rootlen = len(rootdir) + 1
    with ZipFile(archive, "w", ZIP_DEFLATED) as zipf:
        for root, dirs, files in os.walk(folder):
            # remove our zip file from files list
            if archivename in files:
                if root == os.path.dirname(archive):
                    files.remove(archivename)
            for fn in files:
                absfn = os.path.join(root, fn)
                zfn = absfn[rootlen:]  # relative path to store in zip
                zipf.write(absfn, zfn)
            if not files:
                #NOTE: don't ignore empty directories
                absdn = root
                zdn = root[rootlen:]
                zipf.write(absdn, zdn)


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

@app.task()
def demo_task(params, context):
    # 1. Get R wrapper
    wrapper = resource_string('org.bccvl.tasks', 'r_wrapper.sh')
    # 2. Run task
    run_script_SDM(wrapper, params, context)


def run_script_SDM(wrapper, params, context):
    # TODO: there are many little things that can fail here, and we
    #       need to communicate it properly back to the user.
    # TODO: however, we can't really do anything in case sending
    #       messages doesn't work.
    try:

        errmsg = 'Fail to transfer/import data'
        set_progress('RUNNING', 'Transferring data', context)

        # create initial folder structure
        create_workenv(params)
        # FIXME: remove me
        write_status_to_nectar(params, context, u'FETCHING')

        # transfer input files
        transfer_inputs(params, context)

        # Determine the number of pseudoabsence points
        pseudoabs = len(open(params['params']['species_occurrence_dataset']['filename']).readlines()) - 1
        params['params'].update({'species_number_pseudo_absence_points': pseudoabs,
                                 'species_pseudo_absence_points': True})

        # create script
        scriptname = create_scripts(params, context)

        # run the script
        errmsg = 'Fail to run experiement'
        set_progress('RUNNING', 'Executing job', context)
        # FIXME: remove me
        write_status_to_nectar(params, context, u'RUNNING')

        scriptout = os.path.join(params['env']['outputdir'],
                                 params['worker']['script']['name'] + 'out')
        outfile = open(scriptout, 'w')
        wrapsh = os.path.join(params['env']['scriptdir'], 'wrap.sh')
        open(wrapsh, 'w').write(wrapper)
        # zip up workenv if requested
        if params['worker'].get('zipworkenv', False):
            # make sure tmp is big enough
            # TODO: add toolkit name to zip name ... workenv_bioclim.zip
            zip_folder(os.path.join(params['env']['outputdir'], 'workenv.zip'),
                       params['env']['workdir'])
        cmd = ["/bin/bash", "-l", "wrap.sh", scriptname]
        LOG.info("Executing: %s", ' '.join(cmd))

        proc = subprocess.Popen(cmd, cwd=params['env']['scriptdir'],
                                close_fds=True,
                                stdout=outfile, stderr=subprocess.STDOUT)
        rpid, ret, rusage = os.wait4(proc.pid, 0)
        writerusage(rusage, params)
        # TODO: check whether ret and proc.returncode are the same

        # Reproject to Web Mercator
        reproject_to_webmercator(params, context)
        # move results back
        errmsg = 'Fail to transfer results back'
        set_progress('RUNNING', 'Transferring outputs', context)
        # FIXME: remove me
        write_status_to_nectar(params, context, u'TRANSFERRING')

        # Push the projection to nectar, for the wordpress site to fetch
        transfer_afileout(params, context)

        set_progress('COMPLETED', 'Task succeeded', context)
        # FIXME: remove me
        write_status_to_nectar(params, context, u'COMPLETE')

    except Exception as e:
        # TODO: capture stacktrace
        # need to start import to get import cleaned up

        # Log error message with stacktrace.
        #:( exposes internals, ugly hash, complicated with admin only access
        #-> certainly need to get rid of exception in message.
        # test exceptions:
        #  ... upload file, replace with something else (unzip error)
        #  ... delete file and rerun experiment (donwload error)
        #  ... create file/folder error? (can't write log)
        #  ... how to simulate fault? (download error)

        # log error message with exception and traceback
        LOG.exception(errmsg)

        set_progress('FAILED', errmsg, context)
        # FIXME: remove me
        write_status_to_nectar(params, context, u'FAILED')

        raise
    finally:
        # TODO:  check if dir exists
        path = params['env'].get('workdir', None)
        if path and os.path.exists(path):
            shutil.rmtree(path)


def run_script(wrapper, params, context):
    # TODO: there are many little things that can fail here, and we
    #       need to communicate it properly back to the user.
    # TODO: however, we can't really do anything in case sending
    #       messages doesn't work.
    try:

        errmsg = 'Fail to transfer/import data'
        set_progress('RUNNING', 'Transferring data', context)

        # create initial folder structure
        create_workenv(params)

        # transfer input files
        transfer_inputs(params, context)
        # create script
        scriptname = create_scripts(params, context)

        # run the script
        errmsg = 'Fail to run experiement'
        set_progress('RUNNING', 'Executing job', context)

        scriptout = os.path.join(params['env']['outputdir'],
                                 params['worker']['script']['name'] + 'out')
        outfile = open(scriptout, 'w')
        wrapsh = os.path.join(params['env']['scriptdir'], 'wrap.sh')
        open(wrapsh, 'w').write(wrapper)
        # zip up workenv if requested
        if params['worker'].get('zipworkenv', False):
            # make sure tmp is big enough
            # TODO: add toolkit name to zip name ... workenv_bioclim.zip
            zip_folder(os.path.join(params['env']['outputdir'], 'workenv.zip'),
                       params['env']['workdir'])
        cmd = ["/bin/bash", "-l", "wrap.sh", scriptname]
        LOG.info("Executing: %s", ' '.join(cmd))
        proc = subprocess.Popen(cmd, cwd=params['env']['scriptdir'],
                                close_fds=True,
                                stdout=outfile, stderr=subprocess.STDOUT)
        rpid, ret, rusage = os.wait4(proc.pid, 0)
        writerusage(rusage, params)
        # TODO: check whether ret and proc.returncode are the same

        # move results back
        errmsg = 'Fail to transfer results back'
        set_progress('RUNNING', 'Transferring outputs', context)
        transfer_outputs(params, context)

        # we are done here, hand over to result importer
        # build a chain of the remaining tasks
        start_import = set_progress_job('RUNNING', 'Import results', context)

        cleanup_job = import_cleanup_job(params, context)

        import_job = import_result_job(params, context)
        import_job.link_error(set_progress_job('FAILED', 'Result import failed', context))
        import_job.link_error(cleanup_job)

        if ret != 0:
            errmsg = 'Script execution failed with exit code {0}'.format(ret)
            finish_job = set_progress_job('FAILED', errmsg, context)
        else:
            finish_job = set_progress_job('COMPLETED', 'Task succeeded', context)

        (start_import | import_job | cleanup_job | finish_job).delay()

    except Exception as e:
        # TODO: capture stacktrace
        # need to start import to get import cleaned up

        # Log error message with stacktrace.
        #:( exposes internals, ugly hash, complicated with admin only access
        #-> certainly need to get rid of exception in message.
        # test exceptions:
        #  ... upload file, replace with something else (unzip error)
        #  ... delete file and rerun experiment (donwload error)
        #  ... create file/folder error? (can't write log)
        #  ... how to simulate fault? (download error)

        # log error message with exception and traceback
        LOG.exception(errmsg)

        start_import = set_progress_job('RUNNING', 'Import results', context)

        import_job = import_result_job(params, context)
        import_job.link_error(set_progress_job('FAILED', 'Result import failed', context))

        finish_job = set_progress_job('FAILED', errmsg, context)

        (start_import | import_job | finish_job).delay()
        raise
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
    move_tasks = {}  # list of move job arguments
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

    tp = Pool(3)
    result = tp.map(download_input, move_tasks.items())
    tp.close()
    tp.join()

    for key in (k for k, success in result if success):
        # iterate over all successful downloads
        # and remove from job list for data_mover
        del move_tasks[key]

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

def reproject_to_webmercator(params, context):
    # from celery.contrib import rdb; rdb.set_trace()
    # TO-DO: Catch an exception if there isn't a .tif output file
    srcpath = os.path.join(params['env']['outputdir'], 'demoSDM')
    # Fetch the original projection

    srcfile = [x for x in glob.iglob(os.path.join(srcpath,
                                                  'proj_current', '*.tif'))
               if 'Clamping' not in x][0]
    wmcfile = os.path.join(srcpath, 'webmcproj.tif')
    destfile = '.'.join((os.path.splitext(srcfile)[0], '.png'))

    # Create a color file
    coltxt = ['1000 216 7 7 255', '900 232 16 16 255', '800 234 39 39 255',
              '700 236 66 66 255', '600 239 96 96 255', '500 242 128 128 255',
              '400 246 159 159 255', '300 249 189 189 255', '200 251 216 216 255',
              '100 253 239 239 255', '0% 255 255 255 255', 'nv 255 255 255 0']
    colsrc = os.path.join(params['env']['outputdir'], 'col.txt')
    with open(colsrc, 'w') as f:
        for color in coltxt:
            f.write('%s\n' % color)

    commreproj = ['/usr/bin/gdalwarp', '-s_srs', 'epsg:4326', '-t_srs', 'epsg:3857', srcfile, wmcfile]
    commrelief = ['/usr/bin/gdaldem', 'color-relief', '-of', 'PNG', wmcfile, colsrc, destfile, '-alpha']

    scriptout = os.path.join(params['env']['outputdir'], params['worker']['script']['name'] + 'out')
    outfile = open(scriptout, 'w')

    try:
        proc = subprocess.Popen(commreproj, close_fds=True,
                                stdout=outfile, stderr=subprocess.STDOUT)
        rpid, ret, rusage = os.wait4(proc.pid, 0)
        proc = subprocess.Popen(commrelief, close_fds=True,
                                stdout=outfile, stderr=subprocess.STDOUT)
        rpid, ret, rusage = os.wait4(proc.pid, 0)
    except Exception as e:
        raise


# FIXME: Remove Me
def write_status_to_nectar(params, context, status):

    move_tasks = []
    # TODO: Figure out where the status file lives
    srcpath = os.path.join(params['env']['outputdir'], 'state.json')
    with open(srcpath, 'w') as f_json:
        f_json.write(json.dumps({u'status': status,
                                 'jobid': context['jobid']}, indent=4))

    destpath = os.path.join(params['result']['results_dir'], 'state.json')
    move_tasks.append(('file://' + srcpath, destpath))
    datamover.move(move_tasks, context)


def transfer_afileout(params, context):
    # TO-DO: Catch an exception if there isn't a .tif output file
    # Fetch a tiff file that isn't a clamping mask
    move_tasks = []
    srcpath = [x for x in glob.iglob(os.path.join(params['env']['outputdir'],
                                                  'demoSDM',
                                                  'proj_current', '*.png'))
               if 'Clamping' not in x][0]
    destpath = os.path.join(params['result']['results_dir'], 'projection.png')
    move_tasks.append(('file://' + srcpath, destpath))
    datamover.move(move_tasks, context)

def transfer_outputs(params, context):
    move_tasks = []

    # Add result files
    file_outputmap = params['result']['outputs'].get('files', {})
    for fpath in os.listdir(params['env']['outputdir']):
        srcpath = os.path.join(params['env']['outputdir'], fpath)
        destpath = os.path.join(params['result']['results_dir'], fpath)
        includefile, fileinfo = get_file_info(srcpath, file_outputmap)
        if includefile:
            move_tasks.append(('file://' + srcpath, destpath, fileinfo))


    # add job script to outputs
    srcpath = os.path.join(params['env']['scriptdir'],
                          params['worker']['script']['name'])
    destpath = os.path.join(params['result']['results_dir'],
                            params['worker']['script']['name'])
    includefile, fileinfo = get_file_info(srcpath, file_outputmap)
    if includefile:
        move_tasks.append(('file://' + srcpath, destpath, fileinfo))

    # create archives
    path = params['env']['outputdir']
    archive_outputmap = params['result']['outputs'].get('archives', {})
    for archname, archdef in archive_outputmap.items():
        farchname = os.path.join(path, archname)
        empty = True
        with ZipFile(farchname, 'w', ZIP_DEFLATED) as zipf:
            for fileglob in archdef['files']:
                absglob = os.path.join(path, fileglob)
                for fname in glob.glob(absglob):
                    empty = False
                    zipf.write(fname, os.path.relpath(fname, path))
        # Only include non-empty achieve
        if not empty:
            includefile, fileinfo = get_file_info(srcpath, archive_outputmap)
            if includefile:
                move_tasks.append(('file://' + srcpath, destpath, fileinfo))


    # Upload output out to destination specified i.e. swift store
    tp = Pool(3)
    result = tp.map(upload_outputs, move_tasks.items())
    tp.close()
    tp.join()

    # Store metadata for suceessful upload file
    params['results_metadata'] = dict([mdinfo for mdinfo, success in result if success])

def get_move_args(file_descr, params, context):
    #
    # make sure we have a unique place for each file (filenames
    # may be identical for different downloads)
    inputdir = os.path.join(params['env']['inputdir'], file_descr['uuid'])
    os.mkdir(inputdir)
    src = file_descr['downloadurl']
    parsedurl = urlparse(src)
    # If it's an ala download for DemoSDM, pass ala url with no filename
    if parsedurl.scheme == 'ala':
        destfile = inputdir
        dest = 'file://' + destfile
        file_descr['filename'] = os.path.join(inputdir, 'ala_occurrence.csv')
    else:
        # update params with local filename
        destfile = os.path.join(inputdir, file_descr['filename'])
        dest = 'file://' + destfile
        file_descr['filename'] = 'file://' + destfile
    return {'args': (src, dest),
            'filename': destfile,
            'userid': context['user'].get('id')}


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
    key, args = args
    src = args['args'][0]
    destpath = args['filename']

    try:
        # set up the source and destination
        source = build_source(src, args['secret'], args['userid'])
        destination = build_destination(dest)

        move(source, destination)
    except Exception:
        LOG.info('Download from %s to %s failed', src, destpath)
        return (key, False)
    LOG.info('Download from %s to %s succeeded.', src, destpath)
    return (key, True)

def upload_outputs(args):
    src, dest, fileinfo = args[0]

    try:
        # set up the source and destination
        source = build_source(src)
        destination = build_destination(dest)

        # Upload the file and then generate metadata
        move(source, destination)
        md = extract_metadata(src, fileinfo.get('mimetype'))

        thresholds = None
        if fileinfo.get('genre') == 'DataGenreSDMEval' and fileinfo.get('mimetype') == 'text/csv':
            thresholds = extractThresholdValues(urlparse(src).path)

        LOG.info('Upload from %s to %s succeeded.', src, dest)
        return ((dest, {'metadata': md, 'fileinfo': fileinfo, 'thresholds': thresholds}), True)
    except Exception:
        LOG.info('Upload from %s to %s failed', src, dest)
        return (dest, {'metadata': {}, 'fileinfo': fileinfo, 'thresholds': None}, False)

def extract_metadata(filepath, filect):
    from .mdextractor import MetadataExtractor
    mdextractor = MetadataExtractor()

    try:
        return mdextractor.from_file(filepath, filect)
    except Exception as ex:
        LOG.warn("Couldn't extract metadata from file: %s : %s", filepath, repr(ex))
        raise

def get_file_info(filepath, outputmap):
    # Get the file info
    path = os.path.dirname(filepath)

    # sort list of globs with longest first:
    globlist = sorted(outputmap.items(),
                      key=lambda item: (-len(item[0]), item[0]))
    for fileglob, filedef in globlist:
        if filepath in glob.glob(os.path.join(path, fileglob)):
            if not filedef.get('skip', False):
                # Include file only if it is not marked 'skip' = True
                return True, {'mimetype': filedef.get('mimetype'), 'genre': filedef.get('genre'), 'title': filedef.get('title')}
            return False, {}  # Ignore this file
    return False, {}  # File not found

def extractThresholdValues(fname):
    # parse csv file and add threshold values as dict
    # this method might be called multiple times for one item

    # There are various formats:
    #   combined.modelEvaluation: Threshold Name, Testing.data, Cutoff,
    #                             Sensitivity, Specificity
    #   biomod2.modelEvaluation: Threshold Name, Testing.data, Cutoff.*,
    #                            Sensitivity.*, Specificity.*
    #   maxentResults.csv: Species,<various columns with interesting values>
    #                <threshold name><space><cumulative threshold,
    #                              logistic threshold,area,training omission>
    # FIXME: this is really ugly and csv format detection should be done
    #        differently
    thresholds = {}
    if fname.endswith('maxentResults.csv'):
        csvfile = open(fname, 'r')
        dictreader = DictReader(csvfile)
        row = dictreader.next()
        # There is only one row in maxentResults
        namelist = (
            'Fixed cumulative value 1', 'Fixed cumulative value 5',
            'Fixed cumulative value 10', 'Minimum training presence',
            '10 percentile training presence',
            '10 percentile training presence',
            'Equal training sensitivity and specificity',
            'Maximum training sensitivity plus specificity',
            'Balance training omission, predicted area and threshold value',
            'Equate entropy of thresholded and original distributions')
        for name in namelist:
            # We extract only 'cumulative threshold'' values
            threshold = '{} cumulative threshold'.format(name)
            thresholds[threshold] = Decimal(row[threshold])
    else:
        # assume it's one of our biomod/dismo results
        csvfile = open(fname, 'r')
        dictreader = DictReader(csvfile)
        # search the field with Cutoff
        name = 'Cutoff'
        for fieldname in dictreader.fieldnames:
            if fieldname.startswith('Cutoff.'):
                name = fieldname
                break
        try:
            for row in dictreader:
                try:
                    thresholds[row['']] = Decimal(row[name])
                except (TypeError, InvalidOperation) as e:
                    LOG.warn("Couldn't parse threshold value '%s' (%s) from"
                             "file '%s': %s",
                             name, row[name], fname, repr(e))
        except KeyError:
            LOG.warn("Couldn't extract Threshold '%s' from file '%s'",
                     name, fname)
    return thresholds
