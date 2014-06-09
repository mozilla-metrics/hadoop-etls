#!/usr/bin/env python

import sys, os
import codecs
import datetime

import mrjob.job
import mrjob.protocol

try: # workaround for mrjob local mode
    import util
    from base_etl_job import BaseETLJob
except ImportError:
    pass


class ProfileAgeJob(BaseETLJob):

    def mapper(self, _, line):

        sep = self.options.field_separator
        snapshot_date = self.options.snapshot_date

        def make_key(envx):
            d = datetime.datetime.fromtimestamp(0) + datetime.timedelta(days = envx["profile_creation"])
            if not util.is_valid_date(d):
                raise ValueError

            age = (datetime.datetime.strptime(snapshot_date, "%Y-%m-%d") - 
                   d).days

            key_fields = [ snapshot_date,
                           '3',
                           d.strftime("%Y-%m-%d"), 
                           envx["product"], 
                           envx["version"], 
                           envx["channel"], 
                           envx.get("locale", '#unknown'), 
                           envx["country"] ] + \
                           envx["os"] + \
                           [str(age)]
                           
            return sep.join(map(util.strip_invalid_chars, key_fields))

        rec = self.get_fhr_report(line)
        if not rec:
            return

        try:
            yield make_key(rec.getEnv()), 1
        except Exception as e:
            self.mark_invalid_report()

    def combiner(self, key, counts):
        yield key, sum(counts)

    def reducer(self, key, counts):
        yield None, self.options.field_separator.join((key, str(sum(counts))))


if __name__ == '__main__':
    ProfileAgeJob.run()

    
