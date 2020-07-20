#!/usr/bin/env python
"""
IceCube helper methods
"""

import sys

# Import either the Python2 or Python3 function to reraise a system exception
try:
    # pylint: disable=unused-import
    from reraise2 import reraise_excinfo  # pylint: disable=syntax-error
except SyntaxError:
    # pylint: disable=unused-import
    from reraise3 import reraise_excinfo


# Select the appropriate raw input function for Python2/Python3
if sys.version_info >= (3, 0):
    read_input = input      # pylint: disable=invalid-name
else:
    read_input = raw_input  # pylint: disable=invalid-name,undefined-variable


class Comparable(object):
    """
    A class can extend/mixin this class and implement the compare_key()
    method containing all values to compare in their order of importance,
    and this class will automatically populate the special comparison and
    hash functions
    """
    def __eq__(self, other):
        if other is None:
            return False
        return self.compare_key == other.compare_key

    def __ge__(self, other):
        return not self < other

    def __gt__(self, other):
        if other is None:
            return False
        return self.compare_key > other.compare_key

    def __hash__(self):
        return hash(self.compare_key)

    def __le__(self, other):
        return not self > other

    def __lt__(self, other):
        if other is None:
            return True
        return self.compare_key < other.compare_key

    def __ne__(self, other):
        return not self == other

    @property
    def compare_key(self):
        "Return the keys to be used by the Comparable methods"
        raise NotImplementedError(str(type(self)))
