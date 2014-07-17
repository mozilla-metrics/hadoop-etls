#!/usr/bin/env python

import sys, os
import codecs
import datetime

import mrjob.job
import mrjob.protocol

try: # workaround 
    import util
    from base_etl_job import BaseETLJob
except ImportError:
    pass


class SearchCountJob(BaseETLJob):

    def mapper(self, _, line):

        sep = self.options.field_separator
        snapshot_date = self.options.snapshot_date

        def make_key(envx, used_on_dt, srch_loc, srch_partner):
            try:
                d = datetime.datetime.strptime(used_on_dt, "%Y-%m-%d")
                if not util.is_valid_date(d):
                    raise ValueError
            except ValueError:
                self.mark_invalid_date()

            key_fields = [ snapshot_date,
                           '3',
                           d.strftime("%Y-%m-%d"), 
                           envx["product"], 
                           envx["version"], 
                           envx["channel"], 
                           envx.get("locale", '#unknown'), 
                           envx["country"] ] + \
                           envx["os"] + \
                           [ srch_partner,
                             srch_loc ]
                             
                           

            return sep.join(map(util.strip_invalid_chars, key_fields))

        rec = self.get_fhr_report(line)
        if not rec:
            return

        try:
            data_days = rec.getDaysData()
            pkn = 'org.mozilla.searches.counts'
            
            for dobj in data_days:
                for location, counts in dobj.data.get(pkn, {}).iteritems():
                    if not isinstance(counts, dict):
                        continue
                    for partner, cnt in counts.iteritems():
                        try:
                            yield make_key(dobj.env, dobj.date, location, partner), (1, cnt)
                        except ValueError:
                            pass

        except Exception as e:
            self.mark_invalid_report()

    def combiner(self, key, vals):
        n, c = map(sum, apply(zip, list(vals)))
        yield key, (n ,c)

    def reducer(self, key, vals):
        n, c = map(sum, apply(zip, list(vals)))
        yield None, self.options.field_separator.join([key, str(n), str(c)])


if __name__ == '__main__':
    SearchCountJob.run()

    
