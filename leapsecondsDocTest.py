#!/usr/bin/env python


import doctest
import unittest

import leapseconds


def load_tests(loader, tests, ignore):  # pylint: disable=unused-argument
    tests.addTests(doctest.DocTestSuite(leapseconds))
    return tests


if __name__ == "__main__":
    unittest.main()
