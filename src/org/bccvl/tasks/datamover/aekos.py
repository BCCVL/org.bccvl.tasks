from __future__ import absolute_import
import json
import logging
import os.path
import shutil
import tempfile
import urllib

from org.bccvl import movelib
from org.bccvl.movelib.utils import build_source, build_destination
from org.bccvl.tasks.celery import app
from org.bccvl.tasks.utils import traverse_dict, extract_metadata
from org.bccvl.tasks.utils import set_progress, import_cleanup
from org.bccvl.tasks.utils import set_progress_job, import_cleanup_job
from org.bccvl.tasks.utils import import_ala_job


LOG = logging.getLogger(__name__)


@app.task()
def pull_occurrences_from_aekos(species, dest_url, context):
    # 1. set progress
    set_progress('RUNNING', 'Download {0} from aekos'.format(species), None, context)
    # 2. do move
    src = None
    dst = None
    try:
        tmpdir = tempfile.mkdtemp(prefix='aekos_download_')
        src = build_source('aekos://occurrence?species={}'.format(species))
        dst = build_destination('file://{}'.format(tmpdir))
        movelib.move(src, dst)
        # extract metadata and do other stuff....
        set_progress('RUNNING', 'Extract metadata {0} from aekos'.format(species), None, context)

        # FIXME: below needs to be updated once we know what an occurrence dataset from aekos looks like

        # open ala_dateset.json
        aekos_ds = json.load(open(os.path.join(tmpdir, 'aekos_dataset.json'), 'r'))
        # collect files inside ds per datatype
        files = dict(((f['dataset_type'], f) for f in aekos_ds['files']))
        # read ala metadata from attribution file
        aekos_md = json.load(open(files['attribution']['url'], 'r'))
        aekos_csv = files['occurrence']['url']  # this is actually a zip file now

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
                'contenttype': 'application/zip',
                'filename': os.path.basename(ala_csv)
            },
            'bccvlmetadata': bccvlmd,
            'filemetadata': extract_metadata(ala_csv, 'application/zip'),
        }

        # Add the number of occurrence records to the metadata
        # To do: This is a hack. Any better solution.
        occurrence_csv_filename = os.path.join('data', 'ala_occurrence.csv')
        if occurrence_csv_filename in item['filemetadata']:
            # FIXME: copy all occurrence metadata to zip level, for backwards compatibility... this should go away after we fully support 'layered' occurrence zips.
            for key in ('rows', 'headers', 'bounds'):  # what about 'species' ?
                if key in item['filemetadata'][occurrence_csv_filename]['metadata']:
                    item['filemetadata'][key] = item['filemetadata'][occurrence_csv_filename]['metadata'][key]

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
def pull_traits_from_aekos(traits, species, envvars, dest_url, context):
    # 1. set progress
    set_progress('RUNNING', 'Download {0} from aekos'.format(species), None, context)
    # 2. do move
    src = None
    dst = None
    data = {'traits': traits, 'species': species, 'envvars': envvars}
    try:
        tmpdir = tempfile.mkdtemp(prefix='aekos_download_')
        src = build_source('aekos://traits?{}'.format(urllib.urlencode(data)))
        dst = build_destination('file://{}'.format(tmpdir))
        movelib.move(src, dst)
        # extract metadata and do other stuff....
        set_progress('RUNNING', 'Extract metadata {0} from aekos'.format(data), None, context)
    except Exception as e:
        set_progress('FAILED', 'Download Traits from aekos: {1}'.format(data, e), None, context)
        import_cleanup(dest_url, context)
        LOG.error('Download from %s to %s failed: %s', src, dest_url, e)
    finally:
        if tmpdir and os.path.exists(tmpdir):
            shutil.rmtree(tmpdir)
