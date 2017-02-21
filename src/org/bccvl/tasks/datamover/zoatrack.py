from __future__ import absolute_import
import json
import logging
import io
import os
import os.path
import shutil
import tempfile
import urllib
import zipfile
import csv
from datetime import datetime

from org.bccvl import movelib
from org.bccvl.movelib.utils import build_source, build_destination
from org.bccvl.movelib.utils import zip_occurrence_data
from org.bccvl.tasks.celery import app
from org.bccvl.tasks.utils import traverse_dict, extract_metadata
from org.bccvl.tasks.utils import set_progress, import_cleanup
from org.bccvl.tasks.utils import set_progress_job, import_cleanup_job
from org.bccvl.tasks.utils import import_ala_job

SPECIES = 'species'
LONGITUDE = 'lon'
LATITUDE = 'lat'
EVENT_DATE = 'date'
YEAR = 'year'
MONTH = 'month'
ANIMAL_ID = 'animalId'

LOG = logging.getLogger(__name__)

def _process_trait_data(datadir):
    # check that it is valid trait csv file
    count = 0
    csvfile = os.path.join(datadir, 'zoatrack_trait.csv')
    with io.open(csvfile, mode='br+') as csv_file:
        csv_reader = csv.reader(csv_file)

        # Check if csv file header has the necessary columns
        columns = set(['decimalLatitude', 'decimalLongitude', 'speciesScientificName', 'month', 'year', 'eventDate', 'animalId'])
        csv_headers = next(csv_reader)
        missing_columns = ', '.join(columns.difference(csv_headers))
        if missing_columns:
            raise Exception("Missing columns '{}' in dataset".format(missing_columns))

        # These columns must be before other trait columns.
        for header in columns:
            if csv_headers.index(header) >= len(columns):
                raise Exception("Column '{}' must be before trait columns in the dataset".format(header))

        # rename column names
        index = csv_headers
        csv_headers[csv_headers.index('decimalLatitude')] = LATITUDE
        csv_headers[csv_headers.index('decimalLongitude')] = LONGITUDE
        csv_headers[csv_headers.index('speciesScientificName')] = SPECIES
        csv_headers[csv_headers.index('eventDate')] = EVENT_DATE

        # write to a temp file
        with io.open(os.path.join(datadir, 'trait_temp.csv'), mode='wb') as out_file:
            csv_writer = csv.writer(out_file)
            csv_writer.writerow(csv_headers)
            for row in csv_reader:
                csv_writer.writerow(row)
                count += 1

    # overwrite the trait csv file with the temp file
    os.remove(os.path.join(datadir, 'zoatrack_trait.csv'))
    os.rename(os.path.join(datadir, 'trait_temp.csv'), os.path.join(datadir, 'zoatrack_trait.csv'))
    return count

def download_zoatrack_trait_data(src_url, dest):
    # Get trait file
    data_dest = os.path.join(dest, 'data')
    try:
        trait_zipfile, _ = urllib.urlretrieve(src_url)
        # unzip and rename trait file
        with zipfile.ZipFile(trait_zipfile) as z:
                os.mkdir(data_dest)

                # rename trait data csv file
                z.extract('trait.csv', dest)
                os.rename(os.path.join(dest, 'trait.csv'),
                          os.path.join(data_dest, 'zoatrack_trait.csv'))

                # citation file is optional
                try:
                    z.extract('citation.txt', dest)
                    os.rename(os.path.join(dest, 'citation.txt'),
                              os.path.join(data_dest, 'zoatrack_citation.txt'))
                except Exception:
                    pass

    except KeyError:
        LOG.error("Cannot find file %s in downloaded zip file", 'trait.csv', exc_info=True)
        raise
    except Exception:
        # TODO: Not a zip file error.... does it have to raise?
        LOG.error("The downloaded file from %s is not a zip file", src_url, exc_info=True)
        raise

    count = 0
    try:
        count = _process_trait_data(data_dest)
    except Exception:
        LOG.error('Bad column header in downloaded trait file', exc_info=True)
        raise

    # Zip out files if available
    zip_occurrence_data(os.path.join(dest, 'zoatrack_trait.zip'),
                        data_dest,
                        ['zoatrack_trait.csv', 'zoatrack_citation.txt'])
    return os.path.join(dest, 'zoatrack_trait.zip'), count


@app.task()
def pull_traits_from_zoatrack(species, src_url, dest_url, context):
    # 1. set progress
    spName = ', '.join(species)
    set_progress('RUNNING', 'Download {0} from zoatrack'.format(
        spName), None, context)
    # 2. do download
    try:
        tmpdir = tempfile.mkdtemp(prefix='zoatrack_download_')

        # Trait data file is a zip file; trait data file and citation file
        trait_zip, count = download_zoatrack_trait_data(src_url, tmpdir)
        
        if count == 0:
            raise Exception("No trait data is found")

        # extract metadata and do other stuff....
        set_progress('RUNNING', 'Extract metadata {0} from zoatrack'.format(
            spName), None, context)

        # build item to import
        imported_date = datetime.now().strftime('%d/%m/%Y')
        title = "%s trait data" % (spName)
        description = "Observed trait data for %s, imported from ZoaTack on %s" % (
            spName, imported_date)

        item = {
            'title': title,
            'description': description,
            'file': {
                'url': 'file://{}'.format(trait_zip),  # local file url
                'contenttype': 'application/zip',
                'filename': os.path.basename(trait_zip)
            },
            #'bccvlmetadata': bccvlmd,
            'filemetadata': extract_metadata(trait_zip, 'application/zip'),
        }

        # Add the number of trait records to the metadata
        # To do: This is a hack. Any better solution.
        trait_csv_filename = os.path.join('data', 'zoatrack_trait.csv')
        if trait_csv_filename in item['filemetadata']:
            # FIXME: copy all occurrence metadata to zip level, for backwards
            # compatibility... this should go away after we fully support
            # 'layered' occurrence zips.
            for key in ('rows', 'headers', 'bounds'):
                if key in item['filemetadata'][trait_csv_filename]['metadata']:
                    item['filemetadata'][key] = item['filemetadata'][
                        trait_csv_filename]['metadata'][key]

        # TODO: clean this up
        #    remove citation file from metadata, otherwise it will be interpreted as data layer within zip file
        if 'data/zoatrack_citation.csv' in item.get('filemetadata', {}):
            del item['filemetadata']['data/zoatrack_citation.csv']

        # move data file to destination and build data_url
        src = build_source('file://{}'.format(trait_zip))
        dst = build_destination(os.path.join(
            dest_url, os.path.basename(trait_zip)), app.conf.get('bccvl', {}))
        item['file']['url'] = dst['url']
        movelib.move(src, dst)

        # tell importer about new dataset (import it)
        set_progress('RUNNING', 'Import zoatack trait data {0}'.format(spName), None, 
            context)
        cleanup_job = import_cleanup_job(dest_url, context)
        import_job = import_ala_job([item], dest_url, context)
        import_job.link_error(set_progress_job(
            "FAILED", "Import of zoatack trait data failed {0}".format(spName), None,
            context))
        import_job.link_error(cleanup_job)
        finish_job = set_progress_job(
            "COMPLETED", 'ZoaTack import {} complete'.format(spName), None,
            context)
        (import_job | cleanup_job | finish_job).delay()

    except Exception as e:
        set_progress('FAILED', 'Download Traits from zoatack: {0}'.format(e), None, context)
        import_cleanup(dest_url, context)
        LOG.error('Download from %s to %s failed: %s', src_url, dest_url, e, exc_info=True)
    finally:
        if tmpdir and os.path.exists(tmpdir):
            shutil.rmtree(tmpdir)
