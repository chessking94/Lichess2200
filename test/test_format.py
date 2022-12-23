import os
import unittest

from chess.pgn import read_game

from src.format import format_result, format_source_id

TESTDATA_FILENAME = os.path.join(os.path.dirname(__file__), 'game.pgn')


class TestFormat(unittest.TestCase):
    def setUp(self):
        self.testfile = open(TESTDATA_FILENAME, 'r', encoding='utf-8')
        self.testgame = read_game(self.testfile)

    def tearDown(self):
        self.testfile.close()

    def test_result_win(self):
        self.assertEqual(format_result(self.testgame, 'WhiteWin'), '1.0')

    def test_result_loss(self):
        self.assertEqual(format_result(self.testgame, 'BlackWin'), '0.0')

    def test_result_draw(self):
        self.assertEqual(format_result(self.testgame, 'Draw'), '0.5')

    def test_result_invalid(self):
        self.assertIsNone(format_result(self.testgame, 'DoesNotExist'))

    def test_sourceid_valid(self):
        self.assertEqual(format_source_id(self.testgame, 'Site'), 'wijf97Ts')

    def test_sourceid_invalid(self):
        self.assertIsNone(format_source_id(self.testgame, 'DoesNotExist'))


if __name__ == '__main__':
    unittest.main()
