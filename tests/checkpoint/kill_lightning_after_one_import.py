#!/usr/bin/env python2.7

from __future__ import print_function

import sys
import urllib2
import time
import os
import json

sys.path.append('tests')
from _utils import kill_lightning

since = 0
pidfile = sys.argv[1]
while True:
    resp = urllib2.urlopen('http://127.0.0.1:38227/api/v1/dump?since={0}'.format(since))
    for line in resp:
        event = json.loads(line)
        since = event['TS']
        if event['Phase'] == 'E' and event['RPC'] == 'ImportEngine' and 'Error' not in event:
            kill_lightning(pidfile, sleep_dur=0.1)
            break

    time.sleep(0.25)
