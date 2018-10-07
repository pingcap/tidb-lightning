#!/usr/bin/env python2.7

from __future__ import print_function

import urllib2
import json
import sys

TABLE_CONCURRENCY = 4

open_engines = set()
resp = urllib2.urlopen('http://127.0.0.1:40250/api/v1/dump')
for line in resp:
    event = json.loads(line)
    if event['Phase'] == 'S' and event['RPC'] == 'OpenEngine':
        open_engines.add(event['Args'][0])
        if len(open_engines) > TABLE_CONCURRENCY:
            print('Table concurrency is violated!', open_engines)
            sys.exit(1)
    elif event['Phase'] == 'S' and event['RPC'] == 'CloseEngine':
        open_engines.remove(event['Args'][0])
