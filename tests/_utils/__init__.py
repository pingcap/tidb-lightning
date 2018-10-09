#!/usr/bin/env python2.7

from __future__ import print_function

import fcntl
import os
import time

def kill_lightning(pidfile, sleep_dur=0):
    with open(pidfile, 'rb+', buffering=0) as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        try:
            pid = int(f.read().strip())
            print('\x1b[31;1mGoing to kill', pid, '\x1b[0m')
            f.seek(0)
            f.truncate(0)
            time.sleep(sleep_dur)
            os.kill(pid, 2)
        except (ValueError, OSError) as e:
            print('WARNING: Cannot kill lightning due to', e)
            pass
        finally:
            fcntl.flock(f, fcntl.LOCK_UN)
