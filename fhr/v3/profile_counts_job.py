#!/usr/bin/env python

import sys, os
import codecs
import datetime
import urllib

import mrjob.job
import mrjob.protocol

try: # workaround for local mode
    import util
    from base_etl_job import BaseETLJob
except ImportError:
    pass


class ProfileCountJob(BaseETLJob):

    def mapper(self, _, line):

        sep = self.options.field_separator
        snapshot_date = self.options.snapshot_date

        def make_key(envx, used_on_dt):
            try:
                d = datetime.datetime.strptime(used_on_dt, "%Y-%m-%d")
                if not util.is_valid_date(d):
                    raise ValueError
            except ValueError:
                mark_invalid_date()
                raise

            key_fields = [ snapshot_date,
                           '3',
                           d.strftime("%Y-%m-%d"), 
                           envx["product"], 
                           envx["version"], 
                           envx["channel"], 
                           envx.get("locale", '#unknown'), 
                           envx["country"] ] + \
                           envx["os"] + \
                           [envx["arch"]]

            return sep.join(map(util.strip_invalid_chars, key_fields))

        rec = self.get_fhr_report(line)
        if not rec:
            return

        try:
            data_days = rec.getDaysData()
            
            for dobj in data_days:
                try:
                    yield make_key(dobj.env, dobj.date), 1
                except ValueError:
                    pass

        except Exception as e:
            self.mark_invalid_report()

    def combiner(self, key, counts):
        yield key, sum(counts)

    def reducer(self, key, counts):
        kparts = key.split(self.options.field_separator)
        
        yield None, self.options.field_separator.join(kparts[:-1] + [str(sum(counts)), kparts[-1]])


if __name__ == '__main__':
    ProfileCountJob.run()

    
