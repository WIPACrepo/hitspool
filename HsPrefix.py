#!/usr/bin/env python


class HsPrefix(object):
    """
    Old file prefix logic
    """
    ANON = "ANON"
    HESE = "HESE"
    SNALERT = "SNALERT"

    __LIST = (HESE, SNALERT, ANON)

    @classmethod
    def guess_from_dir(cls, copydir):
        if copydir.endswith("HsDataCopy"):
            return cls.SNALERT
        elif 'hese' in copydir:
            return cls.HESE
        return cls.ANON

    @classmethod
    def is_valid(cls, prefix):
        return prefix is not None and prefix in cls.__LIST


if __name__ == "__main__":
    for pfx in (HsPrefix.ANON, HsPrefix.HESE, HsPrefix.SNALERT, "XXX"):
        print "%s valid? == %s" % (pfx, HsPrefix.is_valid(pfx))
