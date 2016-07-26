from __future__ import absolute_import
import json
import logging
import os.path
import shutil
import tempfile

from org.bccvl import movelib
from org.bccvl.movelib.utils import build_source, build_destination
from org.bccvl.tasks.celery import app
from org.bccvl.tasks.utils import extract_metadata
from org.bccvl.tasks.utils import set_progress, import_cleanup
from org.bccvl.tasks.utils import set_progress_job, import_cleanup_job
from org.bccvl.tasks.utils import import_ala_job


LOG = logging.getLogger(__name__)

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
                'contenttype': 'application/zip',
                'filename': os.path.basename(gbif_csv)
            },
            'bccvlmetadata': bccvlmd,
            'filemetadata': extract_metadata(gbif_csv, 'application/zip'),
        }

        # Add the number of occurrence records to the metadata
        # TODO: This is a hack. Any better solution.
        occurrence_csv_filename = os.path.join('data', 'gbif_occurrence.csv')
        if occurrence_csv_filename in item['filemetadata']:
            # FIXME: copy all occurrence metadata to zip level, for backwards compatibility... this should go away after we fully support 'layered' occurrence zips.
            for key in ('rows', 'headers', 'bounds'):  # what about 'species' ?
                if key in item['filemetadata'][occurrence_csv_filename]['metadata']:
                    item['filemetadata'][key] = item['filemetadata'][occurrence_csv_filename]['metadata'][key]


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
