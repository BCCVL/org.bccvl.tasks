from __future__ import absolute_import
import json
import logging
import os.path
import shutil
import tempfile
import os
import io
import datetime
import csv

from org.bccvl import movelib
from org.bccvl.movelib.utils import build_source, build_destination, zip_occurrence_data
from org.bccvl.tasks.celery import app
from org.bccvl.tasks.utils import traverse_dict, extract_metadata
from org.bccvl.tasks.utils import set_progress, import_cleanup
from org.bccvl.tasks.utils import set_progress_job, import_cleanup_job
from org.bccvl.tasks.utils import import_ala_job, import_multi_species_csv_job
from org.bccvl.tasks.utils import UnicodeCSVReader
from org.bccvl.tasks.utils import UnicodeCSVWriter


LOG = logging.getLogger(__name__)

# Combine the specified named csv file in the source directories


def combine_csv(srcdirs, filename, destdir):
    with io.open(os.path.join(destdir, filename), mode='bw') as cf:
        csv_writer = UnicodeCSVWriter(cf)
        headers_written = False

        for srcdir in srcdirs:
            filepath = os.path.join(srcdir, filename)
            if not os.path.isfile(filepath):
                continue
            with io.open(filepath, 'r') as f:
                csv_reader = UnicodeCSVReader(f)
                headers = next(csv_reader)
                if not headers_written:
                    csv_writer.writerow(headers)
                    headers_written = True
                for row in csv_reader:
                    csv_writer.writerow(row)


def download_occurrence_from_ala(params, context):
    results = []
    species = []   # a list of species metadata
    ds_names = []

    for dataset in params:
        src = None
        dst = None
        occurrence_url = dataset['url'].rstrip('/') + "/occurrences/index/download"
        query = dataset['query']    # i.e. qid:<qid> or lsid:<lsid>
        qfilter = "zeroCoordinates,badlyFormedBasisOfRecord,detectedOutlier,decimalLatLongCalculationFromEastingNorthingFailed,missingBasisOfRecord,decimalLatLongCalculationFromVerbatimFailed,coordinatesCentreOfCountry,geospatialIssue,coordinatesOutOfRange,speciesOutsideExpertRange,userVerified,processingError,decimalLatLongConverionFailed,coordinatesCentreOfStateProvince,habitatMismatch"
        email = context.get('user', {}).get('email', '')
        ds_names.append(dataset.get('name', ''))

        # downlaod occurrence file
        # TODO: ignore file if not successfully download (exception), but continue??
        tmpdir = tempfile.mkdtemp(prefix='ala_download_')
        results.append(tmpdir)

        src = build_source('ala://ala?url={}&query={}&filter={}&email={}'.format(occurrence_url, query, qfilter, email))
        dst = build_destination('file://{}'.format(tmpdir))
        movelib.move(src, dst)

        # extract metadata and do other stuff....
        set_progress('RUNNING', 'Extract metadata for {0} from ala'.format(dataset['query']), None, context)
        # open ala_dateset.json
        ala_ds = json.load(open(os.path.join(tmpdir, 'ala_dataset.json'), 'r'))
        # collect files inside ds per datatype
        files = dict(((f['dataset_type'], f) for f in ala_ds['files']))

        # occurrence data file
        ala_csv = files['occurrence']['url']  # this is actually a zip file now

        # read ala metadata from attribution file.
        # May not have metadata for user uploaded dataset into sandbox
        if files.get('attribution'):
            ala_md_list = json.load(open(files['attribution']['url'], 'r'))
            for md in ala_md_list:
                species.append({
                    'scientificName': md.get('scientificName'),
                    'vernacularName': md.get('commonNameSingle') or md.get('scientificName'),
                    'taxonID': md.get('guid'),
                    'rank': md.get('rank'),
                    'genus': md.get('genus'),
                    'family': md.get('family'),
                    'order': md.get('order'),
                    'clazz': md.get('classs'),
                    'phylum': md.get('phylum'),
                    'kingdom': md.get('kingdom')
                })

    # Shall not happen
    if len(results) == 0:
        raise Exception("No occurrence dataset is downloaded from ALA")

    # Combine all the occurrence and citation files from each download into 1 dataset
    if len(results) > 1:
        destdir = tempfile.mkdtemp(prefix='ala_download_')
        results.append(destdir)
        os.mkdir(os.path.join(destdir, 'data'))
        combine_csv(results[:-1], 'data/ala_occurrence.csv', destdir)
        combine_csv(results[:-1], 'data/ala_citation.csv', destdir)

        # Zip it out and point to the new zip file
        ala_csv = os.path.join(destdir, 'ala_occurrence.zip')
        zip_occurrence_data(ala_csv,
                            os.path.join(destdir, 'data'),
                            ['ala_occurrence.csv', 'ala_citation.csv'])

        # Make a title & description for multispecies dataset
        imported_date = datetime.datetime.now().strftime('%d/%m/%Y')
        ds_name = ', '.join([name for name in ds_names if name] or [sp['scientificName'] for sp in species])
        title = "{} occurrences".format(ds_name)
        description = "Observed occurrences for {0}, imported from ALA on {1}".format(ds_name, imported_date)

    else:
        title = ala_ds['title']
        description = ala_ds['description']
        species = species[0]

    # build bccvl metadata:
    bccvlmd = {
        'genre': 'DataGenreSpeciesOccurrence',
        'categories': ['occurrence'],
        'species': species
    }

    # build item to import
    item = {
        'title': title,
        'description': description,
        'file': {
            'url': 'file://{}'.format(ala_csv),  # local file url
            'contenttype': 'application/zip',
            'filename': os.path.basename(ala_csv)
        },
        'bccvlmetadata': bccvlmd,
        'filemetadata': extract_metadata(ala_csv, 'application/zip'),
    }
    return (item, results)


@app.task()
def pull_occurrences_from_ala(params, dest_url, context, import_multspecies_params):
    # 1. set progress
    set_progress('RUNNING', 'Download occurrence dataset from ala', None, context)
    # 2. Download all the occurrence dataset in the params list
    results = []

    try:
        item, results = download_occurrence_from_ala(params, context)

        # This is the zip file path of the occurrence dataset
        ala_csv = item.get('file').get('url').split('file://')[1]

        # Add the number of occurrence records to the metadata
        # TODO: This is a hack. Any better solution.
        occurrence_csv_filename = os.path.join('data', 'ala_occurrence.csv')
        if occurrence_csv_filename in item['filemetadata']:
            # FIXME: copy all occurrence metadata to zip level, for backwards
            # compatibility... this should go away after we fully support 'layered'
            # occurrence zips.
            for key in ('rows', 'headers', 'bounds'):  # what about 'species' ?
                if key in item['filemetadata'][occurrence_csv_filename]['metadata']:
                    item['filemetadata'][key] = item['filemetadata'][occurrence_csv_filename]['metadata'][key]

        # move data file to destination and build data_url
        src = build_source('file://{}'.format(ala_csv))
        dst = build_destination(os.path.join(dest_url, os.path.basename(ala_csv)), app.conf.get('bccvl', {}))
        item['file']['url'] = dst['url']
        movelib.move(src, dst)

        # tell importer about new dataset (import it)
        set_progress("RUNNING", "Import dataset '{0}' from ALA".format(item['title']), None, context)
        cleanup_job = import_cleanup_job(dest_url, context)
        import_job = import_ala_job([item], dest_url, context)
        import_job.link_error(set_progress_job(
            "FAILED", "Import of dataset '{0}' from ALA failed".format(item['title']), None, context))
        import_job.link_error(cleanup_job)
        finish_job = set_progress_job("COMPLETED", "ALA import '{}' complete".format(item['title']), None, context)

        # Split multi-species dataset
        if import_multspecies_params:
            import_multispecies_job = import_multi_species_csv_job(item.get('file').get('url'),
                                                               import_multspecies_params['results_dir'],
                                                               import_multspecies_params['import_context'],
                                                               context)
            import_multispecies_job.link_error(set_progress_job(
                "FAILED", "Split multi-species dataset '{0}' from ALA failed".format(item['title']), None, context))
            import_multispecies_job.link_error(cleanup_job)
            (import_job | import_multispecies_job | cleanup_job | finish_job).delay()
        else:
            (import_job | cleanup_job | finish_job).delay()

    except Exception as e:
        set_progress('FAILED', 'Download occurrence dataset from ALA: {}'.format(e), None, context)
        import_cleanup(dest_url, context)
        LOG.error('Download from %s to %s failed: %s', params, dest_url, e, exc_info=True)
    finally:
        for tmpdir in results:
            if tmpdir and os.path.exists(tmpdir):
                shutil.rmtree(tmpdir)
