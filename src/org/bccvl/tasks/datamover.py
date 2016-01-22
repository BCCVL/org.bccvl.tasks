from __future__ import absolute_import
import json
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
        if tmpdir:
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
        if tmpdir:
            shutil.rmtree(tmpdir)
