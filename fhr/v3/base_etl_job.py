#!/usr/bin/env python

import sys, os
import codecs
import datetime

import mrjob.job
import mrjob.protocol

try: # workaround 
    from fhrdata import FHRData
    import util
except ImportError:
    pass


class BaseETLJob(mrjob.job.MRJob):

    HADOOP_INPUT_FORMAT = 'SequenceFileAsTextInputFormat'
    OUTPUT_PROTOCOL = mrjob.protocol.RawValueProtocol

    def mark_invalid_input(self):
        self.increment_counter("errors", "invalid_input_line")

    def mark_invalid_json(self):
        self.increment_counter("errors", "invalid_json")
    
    def mark_invalid_report(self):
        self.increment_counter("errors", "invalid_report")

    def mark_invalid_date(self):
        self.increment_counter("error", "invalid_date")

    def configure_options(self):
        super(BaseETLJob, self).configure_options()
        self.add_passthrough_option(
            '--field-separator', default=chr(1), 
            help="Specify field separator")

        self.add_passthrough_option(
            '--snapshot-date', default=datetime.datetime.now().strftime("%Y-%m-%d"), 
            help="Specify field separator")

    def get_fhr_report(self, line):
        rec = raw_json = None

        try:
            raw_json = line.split("\t",1)[1]
        except:
            self.mark_invalid_input()
            return

        try:
            rec = FHRData(raw_json)
        except:
            self.mark_invalid_json()
            return

        return rec
    
