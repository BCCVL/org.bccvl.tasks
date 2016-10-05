from __future__ import absolute_import
import csv
import io
import logging
import os.path
import shutil
import tempfile

from org.bccvl import movelib
from org.bccvl.movelib.utils import build_source, build_destination
from org.bccvl.tasks.celery import app
from org.bccvl.tasks.utils import extract_metadata
from org.bccvl.tasks.utils import set_progress
from org.bccvl.tasks.utils import set_progress_job, import_cleanup_job
from org.bccvl.tasks.utils import import_file_metadata_job
from org.bccvl.tasks.utils import import_result_job


LOG = logging.getLogger(__name__)


@app.task()
def move(move_args, context):
    errmsgs = []
    for src, dest in move_args:
        try:
            source = build_source(src, context['user']['id'], app.conf.get('bccvl', {}))
            destination = build_destination(dest, app.conf.get('bccvl', {}))
            movelib.move(source, destination)
        except Exception as e:
            msg = 'Download from %s to %s failed: %s', src, dest, str(e)
            errmsgs.append(msg)
            LOG.warn(msg)
    if errmsgs:
        raise Exception('Move data failed', errmsgs)


@app.task()
def update_metadata(url, filename, contenttype, context):
    try:
        set_progress('RUNNING', 'Download {0}'.format(url), None, context)
        tmpdir = tempfile.mkdtemp()
        tmpfile = '{}/{}'.format(tmpdir, filename)
        userid = context.get('user', {}).get('id')
        settings = app.conf.get('bccvl', {})
        src = build_source(url, userid, settings)
        dst = build_destination('file://{}'.format(tmpfile), settings)
        movelib.move(src, dst)
        item = {
            'filemetadata': extract_metadata(tmpfile, contenttype)
        }

        # Check that there are lon and lat columns
        # if upload is of type csv, we validate column names as well
        if contenttype == 'text/csv':
            if 'headers' not in item['filemetadata'] or 'lat' not in item['filemetadata']['headers'] or 'lon' not in item['filemetadata']['headers']:
                raise Exception("Missing 'lat'/'lon' column")

        set_progress('RUNNING', 'Import metadata for {0}'.format(url), None, context)

        import_job = import_file_metadata_job([item], url, context)
        import_job.link_error(set_progress_job("FAILED", "Metadata update failed for {0}".format(url), None, context))
        finish_job = set_progress_job("COMPLETED", 'Metadata update for {} complete'.format(url), None, context)
        (import_job | finish_job).delay()
    except Exception as e:
        set_progress('FAILED', 'Metadata update for {} failed: {}'.format(url, e), None, context)
        LOG.error('Metadata update for %s failed: %s', url, e)
    finally:
        if tmpdir and os.path.exists(tmpdir):
            shutil.rmtree(tmpdir)


@app.task()
def import_multi_species_csv(url, results_dir, import_context, context):
    # url .... source file
    # results_dir ... folder to place split files into
    # context ... the context with user and orig dataset
    try:
        set_progress('RUNNING', 'Split {0}'.format(url), None, context)
        # step 1: update main dataset metadata
        tmpdir = tempfile.mkdtemp()
        fd, tmpfile = tempfile.mkstemp(dir=tmpdir)
        userid = context.get('user', {}).get('id')
        settings = app.conf.get('bccvl', {})
        src = build_source(url, userid, settings)
        dst = build_destination('file://{}'.format(tmpfile), settings)
        movelib.move(src, dst)
        item = {
            'filemetadata': extract_metadata(tmpfile, "text/csv")
        }

        # Check that there are lon and lat columns
        # if upload is of type csv, we validate column names as well
        if 'headers' not in item['filemetadata'] or 'lat' not in item['filemetadata']['headers'] or 'lon' not in item['filemetadata']['headers']:
            raise Exception("Missing 'lat'/'lon' column")

        set_progress('RUNNING', 'Import metadata for {0}'.format(url), None, context)

        import_md_job = import_file_metadata_job([item], url, context)
        import_md_job.link_error(set_progress_job("FAILED", "Metadata update failed for {0}".format(url), None, context))

        # step 2: split csv file and create sub datasets
        # start reading csv file and create new datasets which will be linked up with dataset collection item
        # FIXME: large csv files should be streamed to seperate files (not read into ram like here)
        f = io.open(tmpfile, 'r')
        f = io.open(tmpfile, 'rb')
        csvreader = csv.reader(f)
        headers = csvreader.next()
        if 'species' not in headers:
            raise Exception('missing species column')
        speciesidx = headers.index('species')
        # create dict with all data .... species column used as key, and rest is just added
        data = {}
        for row in csvreader:
            if not row:
                continue
            species = row[speciesidx]
            if species not in data:
                # create new entry for species
                fname = '{0}.csv'.format(species)
                # TODO: make sure fname contains only legal filename characters
                fpath = os.path.join(tmpdir, fname)
                file = io.open(fpath, 'wb')
                fwriter = csv.writer(file)
                fwriter.writerow(headers)
                data[species] = {
                    'file': file,
                    'writer': fwriter,
                    'path': fpath,
                    'name': fname
                }
            data[species]['writer'].writerow(row)
        # ok we have got all data and everything in separate files
        # close all files
        for species in data:
            data[species]['file'].close()
            del data[species]['file']
            del data[species]['writer']
        # extract metadata
        for species in data:
            data[species]['filemetadata'] = extract_metadata(
                data[species]['path'],
                'text/csv'
            )
        # send files to destination
        for species in data:
            src = build_source('file://{}'.format(data[species]['path']))
            dst = build_destination(os.path.join(results_dir, data[species]['name']), app.conf.get('bccvl', {}))
            data[species]['url'] = dst['url']
            movelib.move(src, dst)
        # all files uploaded .... send import jobs
        set_progress('RUNNING', 'Create datasets for {0}'.format(url), None, context)
        items = []
        for species in data:
            # build item
            item = {
                'title': '{0} occurrences'.format(species),
                'description': '',
                'file': {
                    'url': data[species]['url'],
                    'filename': data[species]['name'],
                    'contenttype': 'text/csv',
                },
                'bccvlmetadata': {
                    'genre': 'DataGenreSpeciesOccurrence',
                    'categories': ['occurrence'],
                    'species': {
                        'scientificName': species,
                    }
                },
                'filemetadata': data[species]['filemetadata'],
                '_partof': {
                    # add back reference to orig dataset
                    # TODO: shouldn't use absolute path here
                    'path': context['context']
                }
            }
            items.append(item)
        # start import process
        start_import = set_progress_job('RUNNING', 'Import results', None, context)
        ########### What is results_dir being used for?
        import_job = import_result_job(items, results_dir, import_context)
        cleanup_job = import_cleanup_job(results_dir, context)
        import_job.link_error(set_progress_job('FAILED', 'Multi species import failed', None, context))
        import_job.link_error(cleanup_job)
        finish_job = set_progress_job('COMPLETED', 'Task succeeded', None, context)
        (start_import | import_md_job | import_job | cleanup_job | finish_job).delay()
        # FIXME: missing stuff...
        #        need to set multi species collection to finished at some stage
    except Exception as e:
        set_progress('FAILED', 'Error while splitting Multi Species CSV {}: {}'.format(url, e), None, context)
        LOG.error('Multi species split for %s faild: %s', url, e)
    finally:
        if tmpdir and os.path.exists(tmpdir):
            shutil.rmtree(tmpdir)
