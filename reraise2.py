#!/usr/bin/env python
"Wrap a function around the Python2 method of reraising exceptions"


# XXX not needed in Python3
def reraise_excinfo(exc_info):
    "Reraise Python2 exception"
    raise exc_info[0], exc_info[1], exc_info[2]
