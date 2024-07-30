import unittest
import test_core.parser_test

suite = unittest.TestLoader()
suite = suite.loadTestsFromModule(test_core.parser_test)

unittest.TextTestRunner().run(suite)