import os.path
from pkg_resources import resource_filename
import shutil
import tempfile
import unittest
import zipfile
from itertools import izip
import pkg_resources

import mock

from org.bccvl.tasks.datamover.zoatrack import download_zoatrack_trait_data


class Test_pull_traits_from_zoatrack(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()

    def tearDown(self):
        if self.tmpdir and os.path.exists(self.tmpdir):
            shutil.rmtree(self.tmpdir)

    def _urlretrieve(self, url, dest=None):
        temp_file = os.path.join(self.tmpdir, 'zoatrack_trait.zip')
        shutil.copy(pkg_resources.resource_filename(__name__, 'zoatrack_trait.zip'),
                            temp_file)
        return (temp_file, None)


    # TODO: this test should go into movelib ?
    @mock.patch('urllib.urlretrieve')
    def test_download_trait_data(self, mock_urlretrieve):
        def areFilesIdentical(filename1, filename2):
            with open(filename1, "rtU") as a:
                with open(filename2, "rtU") as b:
                    # Note that "all" and "izip" are lazy
                    # (will stop at the first line that's not identical)
                    return all(lineA == lineB
                        for lineA, lineB in izip(a.xreadlines(), b.xreadlines()))

        mock_urlretrieve.side_effect = self._urlretrieve

        # setup params
        src_url = 'file:///zoatrack_trait.zip'

        trait_file, count = download_zoatrack_trait_data(src_url, self.tmpdir)

        # Check the files created
        self.assertEqual(count, 3)
        self.assertEqual(trait_file, os.path.join(self.tmpdir, "zoatrack_trait.zip"))
        zoatrack_zip = zipfile.ZipFile(os.path.join(self.tmpdir, "zoatrack_trait.zip"))
        filelist = zoatrack_zip.namelist()
        self.assertEqual(len(filelist), 2)
        self.assertTrue('data/zoatrack_trait.csv' in filelist)
        self.assertTrue('data/zoatrack_citation.txt' in filelist)

        # Check final trait file
        zoatrack_zip.extractall()
        self.assertTrue(areFilesIdentical('data/zoatrack_trait.csv', 
                                    pkg_resources.resource_filename(__name__, 'zoatrack_trait_processed.csv')))
        self.assertTrue(areFilesIdentical('data/zoatrack_citation.txt', 
                                    pkg_resources.resource_filename(__name__, 'zoatrack_citation.txt')))
