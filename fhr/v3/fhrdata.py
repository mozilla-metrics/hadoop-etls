#!/usr/bin/env python

import sys

try:
    import simplejson as json
except ImportError:
    import json

import re
import zlib
import base64
import datetime
from operator import itemgetter
from pprint import pprint
import copy


class DaysData(object):
    def __init__(self, env, data, date):
        self.date = date
        self.env = env
        self.data = data


class Environment(object):
    pass


class FHRData(object):
    def __init__(self, strx):
        self._parsers = {
            2 : self._parseV2,
            3 : self._parseV3
        }

        self._orig = strx
        self._json = json.loads(strx, encoding = 'utf-8')

        self.environments = {}

        self.ping_dates = None

        self._parse()

    def _parse(self):
        self.version = self._json["version"]
        self.ping_dates = (self._json["thisPingDate"], self._json["lastPingDate"])
        if self.version not in self._parsers.keys():
            raise NotImplementedError, "version % not supported" % (self.version)
        else:
            self._parsers[self.version]()
            
    def _parseV2(self):
        payload = self._json
        sysinfo = payload["data"]["last"]["org.mozilla.sysinfo.sysinfo"]
        geckoinfo =  payload["geckoAppInfo"]

        env = {}
        env['version'] = geckoinfo["version"]			
        env['channel'] = geckoinfo["updateChannel"]
        env['locale'] = payload["data"]["last"]["org.mozilla.appInfo.appinfo"]["locale"]
        env['country'] = payload["geoCountry"]
        env['profile_creation'] = payload["data"]["last"]["org.mozilla.profile.age"]["profileCreation"]

        env['os'] = [sysinfo["name"], sysinfo["version"]]
        env['ncpus'] = sysinfo["cpuCount"]
        env['memory'] = sysinfo["memoryMB"]
        env['data'] = payload["data"]

        self.environments['current'] = env

    def _parseV3(self):
        
        payload = self._json
        envs = payload["environments"]

        def parse_base_info(envx):
            base_info = {}
            sysinfo = envx["org.mozilla.sysinfo.sysinfo"]
            geckoinfo =  envx["geckoAppInfo"]

            base_info['version'] = geckoinfo["version"]			
            base_info['channel'] = geckoinfo["updateChannel"]
            base_info['product'] = geckoinfo["name"]

            try:
                base_info['locale'] = envx["org.mozilla.appInfo.appinfo"]["appLocale"]
            except:
                base_info['locale'] = "#unknown"

            base_info['country'] = payload["geoCountry"]
            base_info['profile_creation'] = envx["org.mozilla.profile.age"]["profileCreation"]

            base_info['os'] = [sysinfo["name"], sysinfo["version"]]
            base_info['ncpus'] = sysinfo["cpuCount"]
            base_info['memory'] = sysinfo["memoryMB"]
            base_info['arch'] = sysinfo["architecture"]

            return base_info

        current_info = parse_base_info(envs["current"])

        self.environments['current'] = current_info
        self.environments[envs["current"]["hash"]] = current_info

        # remaining envs are diff to current_env

        for i in envs:
            if i == 'current':
                continue

            # TODO : too expensive
            env = copy.deepcopy(envs["current"])

            for j in envs[i].keys():
                env[j].update(envs[i][j])

            self.environments[i] = parse_base_info(env)


    def getDaysData(self):
        data = []

        if self.version == 2:
            d = self._json["data"]["days"]
            for i in d.keys():
                data.append(DaysData(self.environments["current"], d[i], i))
        elif self.version == 3:
            d = self._json["data"]["days"]
            for i in d.keys():
                for j in d[i].keys():
                    data.append(DaysData(self.environments[j], d[i][j], i))

    
        return data

    def getEnv(self):
        return self.environments["current"]
