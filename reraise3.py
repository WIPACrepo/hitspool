#!/usr/bin/env python
"Wrap a function around the Python3 method of reraising exceptions"


# XXX replace this function with the actual code after Python3 port is complete
def reraise_excinfo(exc_info):
    "Reraise Python3 exception"
    raise exc_info[0].with_traceback(exc_info[1], exc_info[2])
