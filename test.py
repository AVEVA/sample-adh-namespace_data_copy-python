import unittest

from program import main


class SDSPythonSampleTests(unittest.TestCase):
    @classmethod
    def test_main(cls):
        main(test=True)

if __name__ == '__main__':
    unittest.main()