import os.path
from pkg_resources import resource_filename
import shutil
import tempfile
import unittest

import mock

from org.bccvl import movelib

from org.bccvl.tasks.mdextractor import MetadataExtractor


class Test_csv_utf8(unittest.TestCase):

    def test_csv_extract(self):
        mdextractor = MetadataExtractor()
        md = mdextractor.from_file(
            resource_filename(__name__, 'occur_utf8.csv'),
            'text/csv')
        expect = {
            'bounds': {'bottom': -21.783,
                       'left': 114.166,
                       'right': 145.85,
                       'top': -5.166},
            'headers': [
                u'species',
                u'lon',
                u'lat',
                u'uncertainty',
                u'date',
                u'year',
                u'month'],
            'rows': 2,
            'species': [u'Pt\xe9ria penguin', u'Pteria penguin']
        }
        self.assertEqual(md, expect)

    # TODO: this test should go into movelib ?
    @mock.patch('org.bccvl.movelib.protocol.ala._download_metadata_for_lsid')
    @mock.patch('org.bccvl.movelib.protocol.ala._download_occurrence_by_lsid')
    def test_ala_utf8_move(self, mock_occur, mock_md):
        def fetch_occur_data(lsid, dest):
            occur_file = os.path.join(dest, 'ala_occurrence.csv')
            shutil.copyfile(resource_filename(__name__, 'data.csv'),
                            occur_file)
            return { 'url' : occur_file,
                     'name': 'ala_occurrence.csv',
                     'content_type': 'text/csv'}

        def fetch_meta_data(lsid, dest):
            metadata_file = os.path.join(dest, 'ala_metadata.json')
            shutil.copyfile(resource_filename(__name__, 'data.json'),
                            metadata_file)
            return { 'url' : metadata_file,
                     'name': 'ala_metadata.json',
                     'content_type': 'application/json'}

        mock_occur.side_effect = fetch_occur_data
        mock_md.side_effect = fetch_meta_data

        tmpdir = tempfile.mkdtemp()
        try:
            movelib.move({'url': 'ala://ala?lsid=urn:lsid:biodiversity.org.au:apni.taxon:262359'},
                         {'url': 'file://{}'.format(tmpdir)})
            self.assertEqual(mock_occur.call_count, 1)
            self.assertEqual(mock_md.call_count, 1)
            dl_list = os.listdir(tmpdir)
            self.assertEqual(set(dl_list),
                             set(['ala_occurrence.csv', 'ala_dataset.json', 'ala_metadata.json']))
        finally:
            shutil.rmtree(tmpdir)