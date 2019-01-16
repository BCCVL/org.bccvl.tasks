import os.path
from pkg_resources import resource_filename
import shutil
import unittest
import zipfile
from itertools import izip
import pkg_resources

import mock

from org.bccvl import movelib

from org.bccvl.tasks.datamover.ala import download_occurrence_from_ala


class Test_pull_occurrences_from_ala(unittest.TestCase):

    # TODO: this test should go into movelib ?
    @mock.patch('org.bccvl.tasks.datamover.ala.set_progress')
    @mock.patch('org.bccvl.movelib.protocol.ala._download_metadata_for_lsid')
    @mock.patch('org.bccvl.movelib.protocol.ala._download_occurrence')
    def test_download_occurrence_from_ala(self, mock_occur, mock_md, mock_setprogress):

        def fetch_occur_data(lsid, dest):
            occur_file = os.path.join(dest, 'ala_occurrence.zip')
            shutil.copyfile(resource_filename(__name__, 'data.zip'),
                            occur_file)
            with zipfile.ZipFile(occur_file) as z:
                z.extractall(dest)
            return {
                'url': occur_file,
                'name': 'ala_occurrence.zip',
                'content_type': 'application/zip',
                'lsids': [
                    'lsid:urn:lsid:biodiversity.org.au:apni.taxon:262359'
                ]
            }

        def fetch_meta_data(lsid, dest):
            metadata_file = os.path.join(dest, 'ala_metadata.json')
            shutil.copyfile(resource_filename(__name__, 'data.json'),
                            metadata_file)
            return {
                'url': metadata_file,
                'name': 'ala_metadata.json',
                'content_type': 'application/json'
            }

        def do_nothing(state, statusmsg, rusage, context):
            return

        def areFilesIdentical(filename1, filename2):
            with open(filename1, "rtU") as a:
                with open(filename2, "rtU") as b:
                    # Note that "all" and "izip" are lazy
                    # (will stop at the first line that's not identical)
                    return all(
                        lineA == lineB
                        for lineA, lineB
                        in izip(a.xreadlines(), b.xreadlines())
                    )

        mock_occur.side_effect = fetch_occur_data
        mock_md.side_effect = fetch_meta_data
        mock_setprogress.side_effect = do_nothing

        # setup params
        params = [
            {
                'name': 'test_data1',
                'url': 'http://biocache.ala.org.au',
                'query': 'qid:lsid:urn:lsid:biodiversity.org.au:apni.taxon:262359'
            },
            {
                'name': 'test_data2',
                'url': 'http://biocache.ala.org.au',
                'query': 'qid:lsid:urn:lsid:biodiversity.org.au:apni.taxon:262359'
            }
        ]
        context = {
            'user': {
                'email': 'testuser@gmail.com'
            }
        }

        results = []
        try:
            item, results = download_occurrence_from_ala(params, context)

            # Check the files created
            self.assertEqual(len(results), 3)
            self.assertEqual(
                set(os.listdir(results[0])),
                set(['ala_occurrence.zip', 'ala_dataset.json', 'ala_metadata.json', 'data'])
            )
            self.assertEqual(
                set(os.listdir(results[1])),
                set(['ala_occurrence.zip', 'ala_dataset.json', 'ala_metadata.json', 'data'])
            )
            self.assertEqual(
                set(os.listdir(results[2])),
                set(['ala_occurrence.zip', 'data'])
            )

            # Check final occurrence file
            self.assertEqual(item.get('title'), 'test_data1, test_data2')
            self.assertTrue(
                areFilesIdentical(
                    os.path.join(results[2], 'data', 'ala_occurrence.csv'),
                    pkg_resources.resource_filename(__name__, 'ala_occurrence.csv')
                )
            )
            self.assertTrue(
                areFilesIdentical(
                    os.path.join(results[2], 'data', 'ala_citation.csv'),
                    pkg_resources.resource_filename(__name__, 'ala_citation.csv')
                )
            )

        finally:
            for tmpdir in results:
                if tmpdir and os.path.exists(tmpdir):
                    shutil.rmtree(tmpdir)

    @mock.patch('org.bccvl.tasks.datamover.ala.set_progress')
    @mock.patch('org.bccvl.movelib.protocol.ala._download_metadata_for_lsid')
    @mock.patch('org.bccvl.movelib.protocol.ala._download_occurrence')
    def test_download_occurrence_from_ala_trait(self, mock_occur, mock_md, mock_setprogress):

        def fetch_occur_data(url, dest):
            occur_file = os.path.join(dest, 'ala_occurrence.zip')
            if url.find('qid:lsid:urn:lsid:biodiversity.org.au:apni.taxon:262359') >= 0:
                shutil.copyfile(resource_filename(__name__, 'trait-data1.zip'),
                                occur_file)
            else:
                shutil.copyfile(resource_filename(__name__, 'trait-data2.zip'),
                                occur_file)
            with zipfile.ZipFile(occur_file) as z:
                z.extractall(dest)
            return {
                'url': occur_file,
                'name': 'ala_occurrence.zip',
                'content_type': 'application/zip',
                'lsids': [
                    'lsid:urn:lsid:biodiversity.org.au:apni.taxon:262359'
                ]
            }

        def fetch_meta_data(lsid, dest):
            metadata_file = os.path.join(dest, 'ala_metadata.json')
            shutil.copyfile(resource_filename(__name__, 'data.json'),
                            metadata_file)
            return {
                'url': metadata_file,
                'name': 'ala_metadata.json',
                'content_type': 'application/json'
            }

        def do_nothing(state, statusmsg, rusage, context):
            return

        def areFilesIdentical(filename1, filename2):
            with open(filename1, "rtU") as a:
                with open(filename2, "rtU") as b:
                    # Note that "all" and "izip" are lazy
                    # (will stop at the first line that's not identical)
                    return all(
                        lineA.strip("\r\n") == lineB.strip("\r\n")
                        for lineA, lineB
                        in izip(a.xreadlines(), b.xreadlines())
                    )

        mock_occur.side_effect = fetch_occur_data
        mock_md.side_effect = fetch_meta_data
        mock_setprogress.side_effect = do_nothing

        # setup params
        params = [
            {
                'name': 'test_trait_data1',
                'url': 'http://biocache.ala.org.au',
                'query': 'qid:lsid:urn:lsid:biodiversity.org.au:apni.taxon:262359',
                'trait': '1'
            },
            {
                'name': 'test_trait_data2',
                'url': 'http://biocache.ala.org.au',
                'query': 'qid:lsid:urn:lsid:biodiversity.org.au:apni.taxon:262360'
            }
        ]
        context = {
            'user': {
                'email': 'testuser@gmail.com'
            }
        }

        results = []
        try:
            item, results = download_occurrence_from_ala(params, context)

            # Check the files created
            self.assertEqual(len(results), 3)
            self.assertEqual(
                set(os.listdir(results[0])),
                set(['ala_occurrence.zip', 'ala_dataset.json', 'ala_metadata.json', 'data'])
            )
            self.assertEqual(
                set(os.listdir(results[1])),
                set(['ala_occurrence.zip', 'ala_dataset.json', 'ala_metadata.json', 'data'])
            )
            self.assertEqual(
                set(os.listdir(results[2])),
                set(['ala_occurrence.zip', 'data'])
            )

            self.assertEqual(
                set(os.listdir(os.path.join(results[2], 'data'))),
                set(['ala_occurrence.csv', 'ala_citation.csv'])
            )

            # Check final occurrence file
            self.assertEqual(item.get('title'), 'test_trait_data1, test_trait_data2')
            self.assertTrue(
                areFilesIdentical(
                    os.path.join(results[2], 'data', 'ala_citation.csv'),
                    pkg_resources.resource_filename(__name__, 'ala_citation.csv')
                )
            )
            # Skip test. Fix me.
            #self.assertTrue(
            #    areFilesIdentical(
            #        os.path.join(results[2], 'data', 'ala_occurrence.csv'),
            #        pkg_resources.resource_filename(__name__, 'ala_traits.csv')
            #    )
            #)

        finally:
            for tmpdir in results:
                if tmpdir and os.path.exists(tmpdir):
                    shutil.rmtree(tmpdir)
