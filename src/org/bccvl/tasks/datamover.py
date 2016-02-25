from __future__ import absolute_import
import csv
import json
import io
import logging
import os.path
import shutil
import tempfile

from org.bccvl import movelib
from org.bccvl.movelib.utils import build_source, build_destination
from org.bccvl.tasks.celery import app
from org.bccvl.tasks.utils import traverse_dict, extract_metadata
from org.bccvl.tasks.utils import set_progress, import_cleanup
from org.bccvl.tasks.utils import set_progress_job, import_cleanup_job
from org.bccvl.tasks.utils import import_ala_job, import_file_metadata_job
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
def pull_occurrences_from_ala(lsid, dest_url, context):
    # 1. set progress
    set_progress('RUNNING', 'Download {0} from ala'.format(lsid), None, context)
    # 2. do move
    src = None
    dst = None
    try:
        tmpdir = tempfile.mkdtemp(prefix='ala_download_')
        src = build_source('ala://ala?lsid={}'.format(lsid))
        dst = build_destination('file://{}'.format(tmpdir))
        movelib.move(src, dst)
        # extract metadata and do other stuff....
        set_progress('RUNNING', 'Extract metadata {0} from ala'.format(lsid), None, context)
        # open ala_dateset.json
        ala_ds = json.load(open(os.path.join(tmpdir, 'ala_dataset.json'), 'r'))
        # collect files inside ds per datatype
        files = dict(((f['dataset_type'], f) for f in ala_ds['files']))
        # read ala metadata from attribution file
        ala_md = json.load(open(files['attribution']['url'], 'r'))
        ala_csv = files['occurrence']['url']

        # build bccvl metadata:
        bccvlmd = {
            'genre': 'DataGenreSpeciesOccurrence',
            'categories': ['occurrence'],
            'species': {
                'scientificName': traverse_dict(ala_md, 'classification/scientificName'),
                'vernacularName': traverse_dict(ala_md, 'commonNames/0/nameString'),
                'taxonID': traverse_dict(ala_md, 'classification/guid'),
                'rank': traverse_dict(ala_md, 'classification/rank'),
                'genus': traverse_dict(ala_md, 'classification/genus'),
                'genusGuid': traverse_dict(ala_md, 'classification/genusGuid'),
                'family': traverse_dict(ala_md, 'classification/family'),
                'familyGuid': traverse_dict(ala_md, 'classification/familyGuid'),
                'order': traverse_dict(ala_md, 'classification/order'),
                'orderGuid': traverse_dict(ala_md, 'classification/orderGuid'),
                'clazz': traverse_dict(ala_md, 'classification/clazz'),
                'clazzGuid': traverse_dict(ala_md, 'classification/clazzGuid'),
                'phylum': traverse_dict(ala_md, 'classification/phylum'),
                'phylumGuid': traverse_dict(ala_md, 'classification/phylumGuid'),
                'kingdom': traverse_dict(ala_md, 'classification/kingdom'),
                'kingdomGuid': traverse_dict(ala_md, 'classification/kingdomGuid')
            },
        }
        # build item to import
        item = {
            'title': ala_ds['title'],
            'description': ala_ds['description'],
            'file': {
                'url': 'file://{}'.format(ala_csv),  # local file url
                'contenttype': 'text/csv',
                'filename': os.path.basename(ala_csv)
            },
            'bccvlmetadata': bccvlmd,
            'filemetadata': extract_metadata(ala_csv, 'text/csv'),
        }

        # move data file to destination and build data_url
        src = build_source('file://{}'.format(ala_csv))
        dst = build_destination(os.path.join(dest_url, os.path.basename(ala_csv)), app.conf.get('bccvl', {}))
        item['file']['url'] = dst['url']
        movelib.move(src, dst)
        # tell importer about new dataset (import it)
        set_progress('RUNNING', 'Import ala data {0}'.format(lsid), None, context)
        cleanup_job = import_cleanup_job(dest_url, context)
        import_job = import_ala_job([item], dest_url, context)
        import_job.link_error(set_progress_job("FAILED", "Import of ala data failed {0}".format(lsid), None, context))
        import_job.link_error(cleanup_job)
        finish_job = set_progress_job("COMPLETED", 'ALA import {} complete'.format(lsid), None, context)
        (import_job | cleanup_job | finish_job).delay()

    except Exception as e:
        set_progress('FAILED', 'Download {0} from ala: {1}'.format(lsid, e), None, context)
        import_cleanup(dest_url, context)
        LOG.error('Download from %s to %s failed: %s', src, dest_url, e)
    finally:
        if tmpdir and os.path.exists(tmpdir):
            shutil.rmtree(tmpdir)


@app.task()
def pull_occurrences_from_gbif(lsid, dest_url, context):
    # 1. set progress
    set_progress('RUNNING', 'Download {0} from gbif'.format(lsid), None, context)
    # 2. do move
    src = None
    dst = None
    try:
        tmpdir = tempfile.mkdtemp(prefix='gbif_download_')
        src = build_source('gbif://gbif?lsid={}'.format(lsid))
        dst = build_destination('file://{}'.format(tmpdir))
        movelib.move(src, dst)
        # extract metadata and do other stuff....
        set_progress('RUNNING', 'Extract metadata {0} from gbif'.format(lsid), None, context)
        # open gbif_dateset.json
        gbif_ds = json.load(open(os.path.join(tmpdir, 'gbif_dataset.json'), 'r'))
        # collect files inside ds per datatype
        files = dict(((f['dataset_type'], f) for f in gbif_ds['files']))
        # read gbif metadata from attribution file
        gbif_md = json.load(open(files['attribution']['url'], 'r'))
        gbif_csv = files['occurrence']['url']

        # build bccvl metadata:
        bccvlmd = {
            'genre': 'DataGenreSpeciesOccurrence',
            'categories': ['occurrence'],
            'species': {
                'scientificName': gbif_md.get('scientificName', None),
                'vernacularName': gbif_md.get('vernacularName', None),
                'taxonID': gbif_md.get('key', None),
                'rank': gbif_md.get('rank', None),
                'genus': gbif_md.get('genus', None),
                'genusGuid': gbif_md.get('genusKey', None),
                'family': gbif_md.get('family', None),
                'familyGuid': gbif_md.get('familyKey', None),
                'order': gbif_md.get('order', None),
                'orderGuid': gbif_md.get('orderKey', None),
                'clazz': gbif_md.get('class', None),
                'clazzGuid': gbif_md.get('classKey', None),
                'phylum': gbif_md.get('phylum', None),
                'phylumGuid': gbif_md.get('phylumKey', None),
                'kingdom': gbif_md.get('kingdom', None),
                'kingdomGuid': gbif_md.get('kingdomKey', None)
            },
        }
        # build item to import
        item = {
            'title': gbif_ds['title'],
            'description': gbif_ds['description'],
            'file': {
                'url': 'file://{}'.format(gbif_csv),  # local file url
                'contenttype': 'text/csv',
                'filename': os.path.basename(gbif_csv)
            },
            'bccvlmetadata': bccvlmd,
            'filemetadata': extract_metadata(gbif_csv, 'text/csv'),
        }

        # move data file to destination and build data_url
        src = build_source('file://{}'.format(gbif_csv))
        dst = build_destination(os.path.join(dest_url, os.path.basename(gbif_csv)), app.conf.get('bccvl', {}))
        item['file']['url'] = dst['url']
        movelib.move(src, dst)
        # tell importer about new dataset (import it)
        set_progress('RUNNING', 'Import gbif data {0}'.format(lsid), None, context)
        cleanup_job = import_cleanup_job(dest_url, context)
        import_job = import_ala_job([item], dest_url, context)
        import_job.link_error(set_progress_job("FAILED", "Import of gbif data failed {0}".format(lsid), None, context))
        import_job.link_error(cleanup_job)
        finish_job = set_progress_job("COMPLETED", 'GBIF import {} complete'.format(lsid), None, context)
        (import_job | cleanup_job | finish_job).delay()

    except Exception as e:
        set_progress('FAILED', 'Download {0} from gbif: {1}'.format(lsid, e), None, context)
        import_cleanup(dest_url, context)
        LOG.error('Download from %s to %s failed: %s', src, dest_url, e)
    finally:
        if tmpdir and os.path.exists(tmpdir):
            shutil.rmtree(tmpdir)

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
        tmpdir = tempfile.mkdtemp()
        fd, tmpfile = tempfile.mkstemp(dir=tmpdir)
        userid = context.get('user', {}).get('id')
        settings = app.conf.get('bccvl', {})
        src = build_source(url, userid, settings)
        dst = build_destination('file://{}'.format(tmpfile), settings)
        movelib.move(src, dst)
        # start reading csv file and create new datasets which will be linked up with dataset collection item
        # FIXME: large csv files should be streamed to seperate files (not read into ram like here)
        f = io.open(tmpfile, 'r')
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
                'filemetadata': data[species]['filemetadata']
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
        (start_import | import_job | cleanup_job | finish_job).delay()
        # FIXME: missing stuff...
        #        need to set multi species collection to finished at some stage
    except Exception as e:
        set_progress('FAILED', 'Error while splitting Multi Species CSV {}: {}'.format(url, e), None, context)
        LOG.error('Multi species split for %s faild: %s', url, e)
    finally:
        if tmpdir and os.path.exists(tmpdir):
            shutil.rmtree(tmpdir)
