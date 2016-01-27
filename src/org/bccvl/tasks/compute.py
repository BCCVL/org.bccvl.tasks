from __future__ import absolute_import

from csv import DictReader
from decimal import Decimal, InvalidOperation
import glob
import json
import mimetypes
#from multiprocessing.pool import Pool
from multiprocessing.pool import ThreadPool as Pool
import os
import os.path
from pkg_resources import resource_string
import resource
import shutil
import subprocess
import tempfile
from urlparse import urlsplit
from zipfile import ZipFile, ZIP_DEFLATED

from celery.utils.log import get_task_logger

from org.bccvl.movelib import move
from org.bccvl.movelib.utils import build_source, build_destination
from org.bccvl.tasks import datamover
from org.bccvl.tasks.celery import app
from org.bccvl.tasks.utils import extract_metadata
from org.bccvl.tasks.utils import set_progress, set_progress_job
from org.bccvl.tasks.utils import import_result_job, import_cleanup_job


LOG = get_task_logger(__name__)


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
        set_progress('RUNNING', 'Transferring data', None, context)

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
        errmsg = 'Fail to run experiment'
        set_progress('RUNNING', 'Executing job', None, context)
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
        usage = get_rusage(rusage)
        # TODO: check whether ret and proc.returncode are the same

        # Reproject to Web Mercator
        reproject_to_webmercator(params, context)
        # move results back
        errmsg = 'Fail to transfer results back'
        set_progress('RUNNING', 'Transferring outputs', usage, context)
        # FIXME: remove me
        write_status_to_nectar(params, context, u'TRANSFERRING')

        # Push the projection to nectar, for the wordpress site to fetch
        transfer_afileout(params, context)

        set_progress('COMPLETED', 'Task succeeded', None, context)
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

        set_progress('FAILED', errmsg, None, context)
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
    items = []
    try:
        errmsg = 'Fail to transfer/import data'
        set_progress('RUNNING', 'Transferring data', None, context)

        # create initial folder structure
        create_workenv(params)

        # transfer input files
        transfer_inputs(params, context)
        # create script
        scriptname = create_scripts(params, context)

        # run the script
        errmsg = 'Fail to run experiement'
        set_progress('RUNNING', 'Executing job', None, context)

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
        # TODO: should we write this as json file and send as result back
        #       or just send rusage with finished message?
        usage = get_rusage(rusage)
        # TODO: check whether ret and proc.returncode are the same

        # move results back
        errmsg = 'Fail to transfer results back'
        set_progress('RUNNING', 'Transferring outputs', usage, context)
        # TODO: maybe redesign this?
        #       transfer only uploads to destination and stores new url somewhere
        #       and we do metadata extraction and item creation afterwards (here)?
        items = transfer_outputs(params, context)

        # we are done here, hand over to result importer
        # build a chain of the remaining tasks
        start_import = set_progress_job('RUNNING', 'Import results', None, context)

        cleanup_job = import_cleanup_job(params['result']['results_dir'], context)
        import_job = import_result_job(items, params['result']['results_dir'], context)
        import_job.link_error(set_progress_job('FAILED', 'Result import failed', None, context))
        import_job.link_error(cleanup_job)

        if ret != 0:
            errmsg = 'Script execution failed with exit code {0}'.format(ret)
            finish_job = set_progress_job('FAILED', errmsg, None, context)
        else:
            finish_job = set_progress_job('COMPLETED', 'Task succeeded', None, context)

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

        start_import = set_progress_job('RUNNING', 'Import results', None, context)

        import_job = import_result_job(items, params['result']['results_dir'], context)
        import_job.link_error(set_progress_job('FAILED', 'Result import failed', None, context))

        finish_job = set_progress_job('FAILED', errmsg, None, context)

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
    tp.map(download_input, move_tasks.values())
    tp.close()
    tp.join()

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
    # TODO: remove no longer needed zip files?


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
    # TODO: [0] may raise index error if there is no result?
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
    # items to import
    items = []
    # add job script to outputs
    shutil.copyfile(os.path.join(params['env']['scriptdir'],
                                 params['worker']['script']['name']),
                    os.path.join(params['env']['outputdir'],
                                 params['worker']['script']['name']))
    # build collection of all output files
    filelist = set()
    out_dir = params['env']['outputdir']
    for root, dirs, files in os.walk(out_dir):
        for name in files:
            filelist.add(os.path.join(out_dir,
                                      root, name))
    # match files in output map (sorted by length of glob)
    globlist = sorted(params['result']['outputs'].get('files', {}).items(),
                      key=lambda item: (-len(item[0]), item[0]))
    # go through list of globs from outputmap
    for fileglob, filedef in globlist:
        # match globs
        for fname in glob.glob(os.path.join(out_dir, fileglob)):
            if fname in filelist:
                # generate metadat, upload file and collect import infos
                # we import only if not marked as 'skip' and and not done already
                if not filedef.get('skip', False):
                    items.append(createItem(fname, filedef, params['params']))
            filelist.discard(fname)
    # check archives in outputmap
    for archname, archdef in params['result']['outputs'].get('archives', {}).items():
        # create archive
        farchname = os.path.join(out_dir, archname)
        # check if we have added any files
        empty = True
        with ZipFile(farchname, 'w', ZIP_DEFLATED) as zipf:
            # files to add
            for fileglob in archdef['files']:
                absglob = os.path.join(out_dir, fileglob)
                for fname in glob.glob(absglob):
                    empty = False
                    zipf.write(fname, os.path.relpath(fname, out_dir))
                    # discrad all archived files from filelist
                    filelist.discard(fname)
        # create item to import for archive
        if not empty:
            items.append(createItem(farchname, archdef, params['params']))
    # still some files left in out_dir?
    for fname in filelist:
        LOG.info('Importing undefined item %s', fname)
        items.append(createItem(fname, {}, params['params']))
    # TODO: upload each item and send import job for each item
    move_tasks = []
    # build move_tasks
    for item in items:
        src = item['file']['url']
        dst = os.path.join(params['result']['results_dir'],
                           item['file']['filename'])
        item['file']['url'] = dst  # update destination with filename
        move_tasks.append((src, dst, item))

    # Upload output out to destination specified i.e. swift store
    tp = Pool(3)
    tp.map(upload_outputs, move_tasks)
    tp.close()
    tp.join()
    # Store metadata for suceessful upload file
    return items

def get_move_args(file_descr, params, context):
    #
    # make sure we have a unique place for each file (filenames
    # may be identical for different downloads)
    inputdir = os.path.join(params['env']['inputdir'], file_descr['uuid'])
    os.mkdir(inputdir)
    src = file_descr['downloadurl']
    parsedurl = urlsplit(src)
    # If it's an ala download for DemoSDM, pass ala url with no filename
    if parsedurl.scheme == 'ala':
        # FIXME: does this really work?, do we have a uuid for direkt ala downloads?
        destfile = inputdir
        dest = 'file://' + destfile
        file_descr['filename'] = os.path.join(inputdir, 'ala_occurrence.csv')
    else:
        # update params with local filename
        destfile = os.path.join(inputdir, file_descr['filename'])
        dest = 'file://' + destfile
        file_descr['filename'] = destfile
    return {'args': (src, dest),
            'filename': destfile,
            'userid': context['user'].get('id')}


def decimal_encoder(o):
    if isinstance(o, Decimal):
        return float(o)
    raise TypeError(repr(o) + " is not JSON serializable")


def get_rusage(rusage):
    names = ('ru_utime', 'ru_stime',
             'ru_maxrss', 'ru_ixrss', 'ru_idrss', 'ru_isrss',
             'ru_minflt', 'ru_majflt', 'ru_nswap', 'ru_inblock', 'ru_oublock',
             'ru_msgsnd', 'ru_msgrcv', 'ru_nsignals', 'ru_nvcsw', 'ru_nivcsw')
    procstats = {'rusage': dict(zip(names, rusage))}
    # correct ru_maxrss which is in pages and we want it in bytes
    # Note: to get correct pagesize this needs to run on the same
    #       machine where rusage stats came from
    procstats['rusage']['ru_maxrss'] *= resource.getpagesize()
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
    return procstats


def download_input(move_args):
    src, dst = move_args['args']
    try:
        # set up the source and destination
        source = build_source(src, move_args['userid'], app.conf.get('bccvl', {}))
        destination = build_destination(dst)
        move(source, destination)
    except Exception as e:
        LOG.info('Download from %s to %s failed: %s', src, dst, e)
        raise
    LOG.info('Download from %s to %s succeeded.', src, dst)


def upload_outputs(args):
    src, dest, item = args

    try:
        # set up the source and destination (src is a local file)
        source = build_source(src)
        # TODO: add content_type to destination? (move_lib supports it)
        destination = build_destination(dest, app.conf.get('bccvl', {}))

        # Upload the file and then generate metadata
        move(source, destination)
        LOG.info('Upload from %s to %s succeeded.', src, dest)
        item['file']['failed'] = False
    except Exception:
        LOG.info('Upload from %s to %s failed', src, dest)
        item['file']['failed'] = True


# TODO: fname -> dsturl? could use both
def createItem(fname, info, params):
    # fname: full path to file
    # info: ... from outputmap
    name = os.path.basename(fname)
    # layermd ... metadata about raster layer
    layermd = {}
    # bccvlmd ... bccvl specific metadata
    bccvlmd = {}
    genre = info.get('genre', None)
    if genre:
        bccvlmd['genre'] = genre
        if genre in ('DataGenreSDMModel', 'DataGenreCP', 'DataGenreClampingMask'):
            if genre == 'DataGenreClampingMask':
                layermd = {'files': {name: {'layer': 'clamping_mask', 'data_type': 'Discrete'}}}
            elif genre == 'DataGenreCP':
                if params['function'] in ('circles', 'convhull', 'voronoihull'):
                    layermd = {'files': {name: {'layer': 'projection_binary', 'data_type': 'Continuous'}}}
                elif params['function'] in ('maxent',):
                    layermd = {'files': {name: {'layer': 'projection_suitablity', 'data_type': 'Continuous'}}}
                else:
                    layermd = {'files': {name: {'layer': 'projection_probability', 'data_type': 'Continuous'}}}
            # FIXME: find a cleaner way to attach metadata
            for key in ('year', 'month', 'emsc', 'gcm'):
                if key in params:
                    bccvlmd[key] = params[key]
        elif genre == 'DataGenreSDMEval' and info.get('mimetype') == 'text/csv':
            thresholds = extractThresholdValues(fname)
            # FIXME: merge thresholds?
            bccvlmd['thresholds'] = thresholds
    # make sure we have a mimetype
    mimetype = info.get('mimetype', None)
    if mimetype is None:
        mimetype = guess_mimetype(fname)
    # extract file metadata
    filemd = extract_metadata(fname, mimetype)

    # FIXME: check keys to make sense
    #        -> merge layermd and filemetadata?
    #        -> merge bccvlmd and filemetadata?
    return {
        'file': {
            'url': 'file://{}'.format(fname),  # local file url
            'contenttype': mimetype,
            'filename': name
        },
        'title': name,
        'description': info.get('title', u''),
        'bccvlmetadata': bccvlmd,
        'filemetadata': filemd,
        'layermd': layermd
    }


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
            #thresholds[threshold] = Decimal(row[threshold])
            thresholds[threshold] = row[threshold]
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
                    thresholds[row['']] = row[name]
                except (TypeError, InvalidOperation) as e:
                    LOG.warn("Couldn't parse threshold value '%s' (%s) from"
                             "file '%s': %s",
                             name, row[name], fname, repr(e))
        except KeyError:
            LOG.warn("Couldn't extract Threshold '%s' from file '%s'",
                     name, fname)
    return thresholds


def guess_mimetype(name):
    # 1. try mimetype registry
    name = os.path.basename(name)
    mtype = None
    if mtype is None:
        mtype = mimetypes.guess_type(name)
        # TODO: add mime magic here
        # https://github.com/ahupp/python-magic/blob/master/magic.py
        if mtype is not (None, None):
            return mtype[0]
    return 'application/octet-stream'
