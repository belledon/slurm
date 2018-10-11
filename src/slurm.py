#slurm.py
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

import argparse
import subprocess

from . import job
from . import parallelGlobals

from time import sleep
from os import environ
import shlex
import tempfile

class BatchFile:

    """
    Temporary file that contains the batch job data.

    Attributes:
        - name (str): path of file
    """

    def __init__(self, array, home=0):

        if home == 0:
            d = environ["PWD"]
        else:
            d = home

        with tempfile.NamedTemporaryFile(delete=False, dir=d) as f:

            print("Writing batch file to {}".format(f.name))
            for line in array:
                f.write((line + "\n").encode("ascii"))

            self.name = f.name

def parseOut(out):
    """
    Parses the output of a job-array submission.
    """
    h = 'Submitted batch job '
    out = out.split('\n')
    p = [line for line in out if h in line]
    return [line.replace(h, '') for line in p]

def sampleBatch(call):
    return '\n'.join(call)

def buildModules(modules):
    """
    Returns a list that is evaluated to import modules on CentOS.
    Purges any modules present.
    """
    return  ["module purge"] + \
        ['module add {!s}'.format(m) for m in modules]

def buildExports(exports):
    """
    Returns a list that evaluates to environemntal variables.
    """
    return ["export {0!s}={1!s}".format(name, val)
        for name, val in exports]

def args(parser):
    """
    Adds slurm specific arguments to an instance of `argparse.ArgumentParser`.
    """
    parser.add_argument('--time', '-t', type=str,   default = "10",
                        help="Maximum time using `sbatch` designations.")

    parser.add_argument('--cpus', '-c', type = int, default = 2,
                        help = "Number of cores per job")

    parser.add_argument('--mem', '-m', type = str, default = "1G",
                        help  = "Amount of memory per job as \"int[B,K,M,G])\"")

    parser.add_argument('--qos', '-q', type = str, default = 'normal',
                        help = "Quality of service")

    parser.add_argument('--gres', '-g', type = str,
                        help  = "Specify  gres as \{type:number\}")

    return parser

class Batch:

    """
    Slurm interface of sumbitting jobarrays.

    Handles formatting for IO with `slurm.sbatch`.
    """

    def __init__(self, interpreter, modules, exports, func, batch,
        flags, args):
        self.interpreter = interpreter
        self.modules = buildModules(modules)
        self.exports = buildExports(exports)
        self.raw_args = args
        self.func = func
        self.flags = flags
        self.batch = batch
        self.batch_file = None
        self.jobArray = None

    @property
    def cpu(self):
        return self._cpu

    @property
    def mem(self):
        return self._mem

    @property
    def time(self):
        return self._time

    @property
    def gres(self):
        return self._gres

    @property
    def qos(self):
        return self._qos

    @property
    def raw_args(self):
        return self._raw_args

    @raw_args.setter
    def raw_args(self, args):
        self._cpu = args.cpus
        self._mem = args.mem
        self._time = args.time
        self._gres = args.gres
        self._qos = args.qos


    def sbatch_attributes(self):
        """
        Generates the resource attributes for `sbatch`.
        """
        print("Creating slurm configuration...")
        # Assign static variables for slurm
        precursor = []

        # cpu
        print("Assigning cpus")
        precursor.append('-c {:d}'.format(self.cpu))
        if parallelGlobals.setCores(self.cpu):
            print("Jobs will use {0:d} core(s)".format(self.cpu))
        else:
            print("OOPS! Something went wrong with " +\
                "setting number of cores... Will give only one.")

        # memory
        print("Assigning memory")
        precursor.append('--mem={!s}'.format(self.mem))
        print("Jobs will use {!s} of memory".format(self.mem))

        # time
        print("Jobs will "+\
            "have a maximum time: {0!s}".format(self.time))
        precursor.append( '--time={0!s}'.format(self.time))

        # qos
        print("Jobs will be subbmitted under "+self.qos)
        precursor.append("--qos=" + self.qos)
        if self.qos == "use-everything" or self.qos == 'normal':
            precursor.append("--requeue")

        # gpus
        if self.gres is not None:
            print("Jobs will have gres".format(self.gres))
            precursor.append("--gres={0!s}".format(self.gres))

        return precursor

    def v_read_batch_file(self, size = 1, offset = 0):

        # Variables to read args from `BatchFile`
        idx = "$SLURM_ARRAY_TASK_ID * {size} + {offset} + 1".format(
            size = size, offset = offset)
        v_file = ["IND=$(({0!s}))".format(idx)]
        key = "awk 'NR == n' n=$IND \"$JOBARRAY\""
        v_file += ["ARGS=\"$({0!s})\"".format(key)]
        v_file += ["IFS=' '", "read -r -a jobargs <<< \"$ARGS\""]

        value = "\"${jobargs[@]}\""

        # Flags are shared across all jobs
        flags = ' '.join(self.flags)

        # Final line calling execution
        v_file += [' '.join([self.func, value, flags])]
        return v_file

    def job_file(self, chunk = 1):
        """
        Generates a string that represents the virtual job file.
        Each job file has the format of:

        ___
        <shebang>
        <module imports>
        <variable exports>

        for i in [0..chunk):
            <Read data for chunk 0>
            <Run cmd for chunk 0>
        """
        if self.interpreter == '#!/bin/sh':
                print('Using interpreter: sh')
        elif self.interpreter == '#!/bin/bash':
            print('Using interpreter: bash')
        elif self.interpreter == 'local':
            print('Running locally')

        else:
            raise ValueError(
                'Iterpreter {!s} not recognized!'.format(
                    self.interpreter))

        # The dependent variables called with func.
        # These may include dynamic flags
        arguments = [' '.join([ str(e) for e in b ])
            for b in self.batch if len(b) > 0]
        self.batch_file = BatchFile(arguments)

        # determine the dimensions of the jobarray (n_jobs, cmds per job)
        n_args = len(arguments)
        if not n_args % chunk == 0:
            raise ValueError('Cannot chunk into jobs of uneven size')

        job_size = int(n_args/chunk)

        # Export path to the `BatchFile`
        print("Exporting job array file")
        environ["JOBARRAY"] = self.batch_file.name

        # The header contains all environmental variables
        v_file = [self.interpreter] + self.modules + self.exports

        for rep in range(job_size):
            v_file += self.v_read_batch_file(size = job_size, offset = rep)

        return v_file

    def buildJobs(self, jobArray, size):
        self.jobArray = job.JobArray(jobArray, size, cpu = self.cpu,
                mem = self.mem, qos = self.qos, time = self.time)

    def run(self, chunk = 1):
        """
        Submits the job array to Slurm using `sbatch`.

        Arguments:
            chunk (int, optional): The factor to subdivide the number of jobs.
            Default is 1 (no subdivision). Note that time is not adjusted.
        """
        sbatchArgs = self.sbatch_attributes()
        script = self.job_file(chunk = chunk)


        call = ['sbatch', '--array=0-{0:d}'.format(chunk - 1)] + sbatchArgs

        print("Template Job:")
        print(sampleBatch(script))

        # Feed input into subprocess
        script = '\n'.join(script)
        result = job.command(call, input=script)

        if not result:
            raise SystemError("Failed to submit template")

        parsed = parseOut(result)
        if not parsed:
            print("Template failed")
            print(script)
            print(out)
            return False
        else:
            print("Building JobArray record")
            self.buildJobs(parsed[0], chunk)
            print("JobArray Submitted")
            return True


    def monitor(self):
        jobArray = self.jobArray
        if not jobArray:
            print("No jobs for batch... attempting to generate")
            self.run()

        print("Checking jobs")
        complete, pending, running = jobArray.update()
        while complete < jobArray.size :

            sleep(1)
            complete, pending, running = jobArray.update()
            print(("Pending: {0:04d} | Running: {1:04d} | " +\
                "Complete {2:04d}").format(
                len(pending), len(running), complete), end="\r")

        print("\n")

        return True
