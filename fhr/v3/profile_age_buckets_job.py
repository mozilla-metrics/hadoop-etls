#!/usr/bin/env python

import sys, os
import codecs
import datetime
import urllib

import mrjob.job
import mrjob.protocol

try: # workaround 
    import util
    from base_etl_job import BaseETLJob
except ImportError:
    pass

from bisect import bisect_right


class ProfileAgeJob(BaseETLJob):

    def mapper(self, _, line):

        sep = self.options.field_separator
        snapshot_date = self.options.snapshot_date

        def make_key(envx):
            d = datetime.datetime.fromtimestamp(0) + datetime.timedelta(days = envx["profile_creation"])
            if not util.is_valid_date(d):
                raise ValueError

            buckets = {
                7 : "week 1",
                14 : "week 2",
                21 : "week 3",
                28 : "week 4",
                91 : "1 - 3 months",
                182 : "3 - 6 months",
                365 : "6 - 12 months",
                730 : "1 - 2 years",
                1095 : "2 - 3 years",
                sys.maxint : "3+ years"
                }

            bucket_limits = buckets.keys()
            bucket_limits.sort()
            
            bi = bisect_right(bucket_limits, envx["profile_creation"])
            bkt = buckets[bucket_limits[bi]]
            
            key_fields = [ snapshot_date,
                           '3',
                           envx["product"], 
                           envx["version"], 
                           envx["channel"], 
                           envx.get("locale", '#unknown'), 
                           envx["country"] ] + \
                           envx["os"] + \
                           [bkt]
                           
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
