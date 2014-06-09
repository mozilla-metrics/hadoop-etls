#!/usr/bin/env python

import datetime
import types


FHR_START_DATE = datetime.date(2013, 5, 22) # oldest record in hbase has date = 23-May-2013
FHR_END_DATE = datetime.date.today() + datetime.timedelta(days = 1)

CHR_TRANSLATION_MAP = None


def _get_translation_map():
    s = {}
    for i in range(256):
        s[i] = chr(i)
    
    for i in range(32):
        if i != 9:
            s[i] = ' '

    return ''.join(s[i] for i in s.keys())

CHR_TRANSLATION_MAP = _get_translation_map()
    

def is_valid_date(datex):
    try:
        if isinstance(datex, types.StringType):
            datex = datetime.datetime.strptime(datex, "%Y-%m-%d").date()
        
        if isinstance(datex, datetime.datetime):
            datex = datex.date()

        if isinstance(datex, datetime.date):
            return (FHR_START_DATE <= datex <= FHR_END_DATE)
    except:
        pass

    return False


def strip_invalid_chars(strx):
    return strx.translate(CHR_TRANSLATION_MAP)
