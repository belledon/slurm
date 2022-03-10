# slurm.py
######################################################
# 03/23/17
# Batch:
#   A Python class to manage JobArrays in slurm.
#   Designed to support singularity environments.
#
# Main:
#   Submit a single job directly into a singularity
#   container from the command line.
#
# package:
#   A utility function useful for subdividing jobs
#   if needed
# _____________________________________________________
# author(s): Mario E. Belledonne

import shlex
import tempfile
import argparse
import subprocess
from time import sleep
from os import environ

from . import job


class BatchFile:

    """
    Temporary file that contains the batch job data.

    Attributes:
        array (iterable): Collection of arguments (one line per job)
        home (optional,str): path of file. Defaults to $PWD
    """

    def __init__(self, array, home=None):

        if home is None:
            d = environ["PWD"]
        else:
            d = home

        with tempfile.NamedTemporaryFile(delete=False, dir=d) as f:

            print("Writing batch file to {}".format(f.name))
            for line in array:
                f.write((line + "\n").encode("ascii"))

            self.name = f.name

    def read(self):
        with open(self.name, 'r') as f:
            data = f.read()
            print(data)

def parseOut(out):
    """ Parses the output of a job-array submission.
    Arguments:
        out (str) : Output from sbatch to parse.
    """
    h = 'Submitted batch job '
    out = out.split('\n')
    p = [line for line in out if h in line]
    return [line.replace(h, '') for line in p]

class Batch:

    """
    Slurm interface of sumbitting jobarrays.

    Handles formatting for IO with `slurm.sbatch`.
    """

    def __init__(self, interpreter, func, batch, flags, extras, resources):
        self.interpreter = interpreter
        self.resources = resources
        self.func = func
        self.flags = flags
        self.extras = extras
        self.batch = batch
        self.batch_file = None
        self.jobArray = None

    @property
    def resources(self):
        return self._resources

    @property
    def raw_args(self):
        return self._raw_args

    @resources.setter
    def resources(self, r):
        if not isinstance(r, dict):
            raise ValueError(
                'Submitted resources must be a valid `dict` object'
            )
        self._raw_args = r
        rule1 = lambda p: '{0!s}={1!s}'.format(*p)
        rule2 = lambda p: '{0!s}'.format(p[0])
        l = [rule2(p) if p[1] is None else rule1(p) for p in r.items()]
        self._resources = list(map(lambda x: '#SBATCH --'+x, l))



    def v_read_batch_file(self, size = 1, offset = 0):
        """ Returns strings containing bash commands to read from batch file

        A Helper function that includes the logic for teasing out the arguments
        to pass to `func` across jobs in a job array.

        This also includes logic for dense jobs where there are multiple
        calls to `func` when `chunk > 1`.

        Arguments:
            size (int, optional): The total # of calls to `func`
            offset (int, optional): Which call we are currently on

        Returns:
            A list of calls to `func` interpretable by bash
        """
        # Variables to read args from `BatchFile`
        idx = "$SLURM_ARRAY_TASK_ID * {size} + {offset} + 1".format(
            size = size, offset = offset)
        v_file = ["IND=$(({0!s}))".format(idx)]
        key = "awk 'NR == n' n=$IND \"{0!s}\"".format(self.batch_file.name)
        v_file += ["ARGS=\"$({0!s})\"".format(key)]
        v_file += ["IFS=' '", "read -r -a jobargs <<< \"$ARGS\""]

        value = "\"${jobargs[@]}\""

        # Flags are shared across all jobs
        flags = ' '.join(self.flags)

        # Final line calling execution
        v_file += [' '.join([self.func, value, flags])]
        return v_file

    def job_file(self, chunk = 1, tmp_dir = None):
        """
        Generates a string that represents the virtual job file.
        Each job file has the format of:

        ___
        <shebang>
        <SBATCH args>
        .
        .
        <extras>
        .
        .
        for i in [0..chunk):
            <Read data for chunk 0>
            <Run cmd for chunk 0>
        """
        # The dependent variables called with func.
        # These may include dynamic flags
        arguments = [' '.join([ str(e) for e in b ])
            for b in self.batch if len(b) > 0]
        self.batch_file = BatchFile(arguments, home = tmp_dir)

        # determine the dimensions of the jobarray (n_jobs, cmds per job)
        n_args = len(arguments)
        if not n_args % chunk == 0:
            raise ValueError('Cannot chunk into jobs of uneven size')

        job_size = int(n_args/chunk)

        # The header contains all environmental variables
        v_file = [self.interpreter] + self.resources + \
                 ['#SBATCH --array=0-{0:d}'.format(chunk-1)] +\
                 self.extras

        for rep in range(job_size):
            v_file += self.v_read_batch_file(size = job_size, offset = rep)

        return v_file

    # def buildJobs(self, jobArray, size):
    #     self.jobArray = job.JobArray(jobArray, size, cpu = self.cpu,
    #             mem = self.mem, qos = self.qos, time = self.time)

    def run(self, n = 1, check_submission = True, script=None):
        """
        Submits the job array to Slurm using `sbatch`.

        Arguments:
            n (int, optional): The size of the job array.
            Default is 1. Note that time is not adjusted.

            check_submission (bool, optional): Whether subprocess will validate
            submission call.

        Returns:
           True if submission was successful. Otherwise, False.
        """
        # Feed input into subprocess
        if script is None:
            script = self.job_file(chunk = n)
            script = '\n'.join(script)
        result = job.command('sbatch', input=script,
                             check_err = check_submission)

        if not result:
            raise SystemError("Failed to submit template")

        parsed = parseOut(result)
        if not parsed:
            print("Template failed")
            print(script)
            print(out)
            return False
        else:
            print("JobArray Submitted to {0!s}".format(parsed[0]))
            return True
