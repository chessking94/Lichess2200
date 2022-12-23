import os
import unittest

from src.func import get_conf, get_config

CONFIG_PATH = os.path.dirname(os.path.dirname(__file__))


class TestFunc(unittest.TestCase):
    def test_conf_token(self):
        self.assertIsNotNone(get_conf('LichessAPIToken'))

    def test_conf_connstring(self):
        self.assertIsNotNone(get_conf('SqlServerConnectionStringTrusted'))

    def test_config_logPath(self):
        log_path = get_config(CONFIG_PATH, 'logPath')
        self.assertTrue(os.path.isdir(log_path))

    def test_config_logFile(self):
        log_path = get_config(CONFIG_PATH, 'logPath')
        log_file = get_config(CONFIG_PATH, 'logFile')
        self.assertTrue(os.path.isfile(os.path.join(log_path, log_file)))

    def test_config_filePath(self):
        file_path = get_config(CONFIG_PATH, 'filePath')
        self.assertTrue(os.path.isdir(file_path))

    def test_config_fileName(self):
        file_path = get_config(CONFIG_PATH, 'filePath')
        file_name = get_config(CONFIG_PATH, 'fileName')
        self.assertTrue(os.path.join(file_path, file_name))


if __name__ == '__main__':
    unittest.main()
