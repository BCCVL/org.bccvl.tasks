import io
import os.path
import shutil
import tempfile
import unittest
from pkg_resources import resource_filename

import mock

from org.bccvl.tasks.datamover.tasks import import_multi_species_csv


class Test_multispecies(unittest.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()

    def tearDown(self):
        if self.tmpdir and os.path.exists(self.tmpdir):
            shutil.rmtree(self.tmpdir)

    def _file_download(self, source, dest=None):
        if source['url'] == 'file:///multispecies.csv':
            # initial download of multispecies file
            shutil.copy(resource_filename(__name__, 'multispecies.csv'),
                        dest)
            return
        # transfer of split out result files
        # .... remove file://
        shutil.copy(source['url'][7:], dest)

    # TODO: this test should go into movelib ?
    # other mocks?
    #     import_file_metadata_job([item], url, context)
    #     movelib.move(src, dst) ... storing split out multi species files
    #     import_result_job(items, results_dir, import_context) ... verify results?
    @mock.patch('org.bccvl.tasks.datamover.tasks.import_result_job')
    @mock.patch('org.bccvl.tasks.datamover.tasks.set_progress_job')
    @mock.patch('org.bccvl.tasks.datamover.tasks.set_progress')
    @mock.patch('org.bccvl.movelib.protocol.file.download')
    def test_multispecies_import(self, mock_file_download, mock_set_progress, mock_set_progress_job, mock_import_result_job):
        mock_file_download.side_effect = self._file_download

        # setup params
        src_url = 'file:///multispecies.csv'

        context = {
          'user': {
            'id': 'userid'
          },
          'context': '/my/object/path'
        }
        import_context = {}
        import_multi_species_csv(src_url, 'file://' + self.tmpdir, import_context, context)

        # Check import_result_job calls
        self.assertEqual(mock_import_result_job.call_count, 1)
        # first argument to import_result_job is single species items
        items = mock_import_result_job.call_args[0][0]
        self.assertEqual(len(items), 2)
