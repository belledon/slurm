#job.py
######################################################
# 2/2/2017
# Class representation of slurm job.
# _____________________________________________________
# author(s): Mario E. Belledonne

import sys
import subprocess
import numpy as np
from time import sleep


def command(c, input = None, check_err = True):
    """ Processes commands for `subprocess`.
    """
    if input is None:
        p = subprocess.Popen(c,
                             stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE,
                             universal_newlines=True)
        out,err = p.communicate()
    else:
        p = subprocess.Popen(c,
                             stdin=subprocess.PIPE,
                             stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE,
                             universal_newlines=True)
        # Feed input into subprocess
        out,err = p.communicate(input=input)

    sys.stdout.flush()
    if not err or not check_err:
        return out
    else:
        print("Command {0!s} resulted in the error :".format(
            str(c[0])))
        print(err)
        print(out)
        sys.stdout.flush()
        return False

def parsePoll(out):
    parsed = [[c for c in line.split(" ") if c] for line in out if line]
    return np.array(parsed)
