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
from org.bccvl.tasks.utils import import_ala_job, import_multi_species_csv_job


LOG = logging.getLogger(__name__)


@app.task()
def pull_occurrences_from_obis(lsid, dest_url, context, import_multspecies_params):
    # 1. set progress
    set_progress('RUNNING', 'Download {0} from OBIS'.format(lsid), None, context)
    # 2. do move
    src = None
    dst = None
    try:
        tmpdir = tempfile.mkdtemp(prefix='obis_download_')
        src = build_source('obis://obis?lsid={}'.format(lsid))
        dst = build_destination('file://{}'.format(tmpdir))
        movelib.move(src, dst)
        # extract metadata and do other stuff....
        set_progress('RUNNING', 'Extract metadata {0} from obis'.format(lsid), None, context)
        # open obis_dateset.json
        obis_ds = json.load(open(os.path.join(tmpdir, 'obis_dataset.json'), 'r'))
        # collect files inside ds per datatype
        files = dict(((f['dataset_type'], f) for f in obis_ds['files']))
        # read obis metadata from attribution file
        obis_md = json.load(open(files['attribution']['url'], 'r'))
        obis_csv = files['occurrence']['url']

        # build bccvl metadata:
        bccvlmd = {
            'genre': 'DataGenreSpeciesCollection' if import_multspecies_params else 'DataGenreSpeciesOccurrence',
            'categories': ['multispecies' if import_multspecies_params else 'occurrence'],
            'species': {
                'scientificName': obis_md.get('tname', None),
                'vernacularName': obis_md.get('tname', None),
                'taxonID': obis_md.get('id', None),
                'rank': obis_md.get('rank_name', None),
                'genus': obis_md.get('genus', None),
                'genusGuid': obis_md.get('genusKey', None),
                'family': obis_md.get('family', None),
                'familyGuid': obis_md.get('familyKey', None),
                'order': obis_md.get('order', None),
                'orderGuid': obis_md.get('orderKey', None),
                'clazz': obis_md.get('class', None),
                'clazzGuid': obis_md.get('classKey', None),
                'phylum': obis_md.get('phylum', None),
                'phylumGuid': obis_md.get('phylumKey', None),
                'kingdom': obis_md.get('kingdom', None),
                'kingdomGuid': obis_md.get('kingdomKey', None)
            },
        }
        # build item to import
        item = {
            'title': obis_ds['title'],
            'description': obis_ds['description'],
            'file': {
                'url': 'file://{}'.format(obis_csv),  # local file url
                'contenttype': 'application/zip',
                'filename': os.path.basename(obis_csv)
            },
            'bccvlmetadata': bccvlmd,
            'filemetadata': extract_metadata(obis_csv, 'application/zip'),
        }

        # Add the number of occurrence records to the metadata
        # TODO: This is a hack. Any better solution.
        occurrence_csv_filename = os.path.join('data', 'obis_occurrence.csv')
        if occurrence_csv_filename in item['filemetadata']:
            # FIXME: copy all occurrence metadata to zip level, for backwards
            # compatibility... this should go away after we fully support 'layered'
            # occurrence zips.
            for key in ('rows', 'headers', 'bounds'):  # what about 'species' ?
                if key in item['filemetadata'][occurrence_csv_filename]['metadata']:
                    item['filemetadata'][key] = item['filemetadata'][occurrence_csv_filename]['metadata'][key]

        # move data file to destination and build data_url
        src = build_source('file://{}'.format(obis_csv))
        dst = build_destination(os.path.join(dest_url, os.path.basename(obis_csv)), app.conf.get('bccvl', {}))
        item['file']['url'] = dst['url']
        movelib.move(src, dst)
        # tell importer about new dataset (import it)
        set_progress('RUNNING', 'Import obis data {0}'.format(lsid), None, context)
        cleanup_job = import_cleanup_job(dest_url, context)
        import_job = import_ala_job([item], dest_url, context)
        import_job.link_error(set_progress_job("FAILED", "Import of obis data failed {0}".format(lsid), None, context))
        import_job.link_error(cleanup_job)
        finish_job = set_progress_job("COMPLETED", 'OBIS import {} complete'.format(lsid), None, context)

        # Split multi-species dataset
        if import_multspecies_params:
            import_multispecies_job = import_multi_species_csv_job(item.get('file').get('url'),
                                                                   import_multspecies_params['results_dir'],
                                                                   import_multspecies_params['import_context'],
                                                                   context)
            import_multispecies_job.link_error(set_progress_job(
                "FAILED", u"Split multi-species dataset '{0}' from OBIS failed".format(item['title']), None, context))
            import_multispecies_job.link_error(cleanup_job)
            (import_job | import_multispecies_job | cleanup_job | finish_job).delay()
        else:
            (import_job | cleanup_job | finish_job).delay()
    except Exception as e:
        set_progress('FAILED', 'Download {0} from OBIS: {1}'.format(lsid, e), None, context)
        import_cleanup(dest_url, context)
        LOG.error('Download from %s to %s failed: %s', src, dest_url, e, exc_info=True)
    finally:
        if tmpdir and os.path.exists(tmpdir):
            shutil.rmtree(tmpdir)
