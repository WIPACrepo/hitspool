#!/usr/bin/env python


import doctest
import unittest

import leapseconds


def load_tests(loader, tests, ignore):
    tests.addTests(doctest.DocTestSuite(leapseconds))
    return tests


if __name__ == "__main__":
    unittest.main()
