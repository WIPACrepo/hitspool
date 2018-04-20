#!/usr/bin/env python


class HsPrefix(object):
    """
    Old file prefix logic
    """
    ANON = "ANON"
    HESE = "HESE"
    SNALERT = "SNALERT"
    LIVE = "i3live"

    __LIST = (HESE, SNALERT, ANON, LIVE)

    @classmethod
    def guess_from_dir(cls, copydir):
        if copydir is not None:
            if 'hese' in copydir:
                return cls.HESE
            elif 'HsDataCopy' in copydir:
                return cls.SNALERT
            elif 'HitSpool' in copydir and 'satellite' in copydir:
                return cls.LIVE
        return cls.ANON

    @classmethod
    def is_valid(cls, prefix):
        return prefix is not None and prefix in cls.__LIST


if __name__ == "__main__":
    for pfx in (HsPrefix.ANON, HsPrefix.HESE, HsPrefix.SNALERT, HsPrefix.LIVE,
                "XXX"):
        print "%s valid? == %s" % (pfx, HsPrefix.is_valid(pfx))
