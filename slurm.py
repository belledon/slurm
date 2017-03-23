#slurm.py
######################################################
# 03/23/17
# Batch:
# 	A Python class to manage JobArrays in slurm.
# 	Designed to support singularity environments.
#
# Main:
# 	Submit a single job directly into a singularity
# 	container from the command line.
#
# package:
# 	A utility function useful for subdividing jobs
# 	if needed
# _____________________________________________________
# author(s): Mario E. Belledonne
import job
import argparse
import subprocess

import numpy as np
import parallelGlobals as pg

from time import sleep
from os import environ
from os import putenv
from collections import namedtuple


def package(pre, data):
	size = len(pre)
	for i,d in enumerate(data):
		pre[i%size].append(d)
	return pre

def parseOut(out):
	h = 'Submitted batch job '
	out = out.split('\n')
	p = [line for line in out if h in line]
	return [line.replace(h, '') for line in p]

def sampleBatch(call):
	return '\n'.join(call)

def buildModules(modules):
	return  ["module purge"] + \
		['module add {!s}'.format(m) for m in modules]

def buildExports(exports):
	return ["export {0!s}={1!s}".format(name, val)
		for name, val in exports]

def args(parser):
	parser.add_argument('--time', '-t', type=str, 	default = "10",
		help="Maximum time per grasp in minutes.")

	parser.add_argument('--cpus', '-cpu', type = int, default = 2,
		help = "Number of cpus per job")

	parser.add_argument('--mem', '-m', type = int, default = 1,
		help  = "Amount of memory  per  job")

	parser.add_argument('--qos', '-q', type = str, default = None,
		help = "Quality of service")

	return parser

class Batch:
	def __init__(self, interpreter, modules, exports, func, batch,
		flags, args):
		self.interpreter = interpreter
		self.modules = buildModules(modules)
		self.exports = buildExports(exports)
		self.func = func
		self.flags = flags
		self.cpu = args.cpus
		self.mem = args.mem
		self.time = args.time
		self.qos = args.qos
		self.batch = batch
		self.jobArray = None


	def buildPrecursor(self):
		print("Creating slurm configuration...")
		# Assign static variables for slurm
		precursor = []
		precursor.append("-N1")
		# precursor.append("--profile=all")
		# cpu
		print("Assigning cpus")
		precursor.append('-c {:d}'.format(self.cpu))
		if pg.setCores(self.cpu):
			print("Jobs will use {:d} core(s)".format(self.cpu))
		else:
			print("OOPS! Something went wrong with " +\
				"setting number of cores... Will give only one.")
		# memory
		print("Assigning memory")
		precursor.append('--mem={:d}G'.format(self.mem))
		print("Jobs will use {:d}GB(s)".format(self.mem))
		# time
		print("Jobs will "+\
			"have a maximum time: {0!s}".format(self.time))
		precursor.append( '--time={0!s}'.format(self.time))
		# qos
		if self.qos:
			print("Jobs will be subbmitted under "+self.qos)
			precursor.append("--qos=" + self.qos)
		else:
			print("Jobs will not have qos")

		return precursor


	def prepareBatch(self):
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

		# The header contains all environmental variables
		if self.interpreter == 'local':
			header = []
		else:
			header = [self.interpreter]  + self.modules +\
				self.exports

		# The main function call
		print('Will iterate on: ' + self.func)
		# Flags are shared across all jobs
		flags = ' '.join(self.flags)
		# The dependent variables called with func.
		# These may include dynamic flags
		arguments = [' '.join([ str(e) for e in b ])
			for b in self.batch if b]
		# Export the dynamic variables if using job array
		print("Exporting job array")
		if len(arguments) > 0:
			environ["JOBARRAY"] = ",".join(
				["{}".format(a) for a in arguments])
		# bash command to extract the dependent variables from the bash array
		key = "\"${array[$SLURM_ARRAY_TASK_ID]}\""
		# returns all of the dependent variables for the jobid
		value = "\"${jobargs[@]}\""
		# bash command to split the JOBARRAY string to jobid accesible arrays
		header += ["IFS=$','", "read -r -a array <<< \"$JOBARRAY\""]
		# bash command to split the jobid specific array into individual
		# 	arguments
		header += ["IFS=' '", "read -r -a jobargs <<< {}".format(key)]
		# header += ["echo \"jobarray is\" {0!s}".format("${#array[@]}")]
		# header += ["echo \"id val is\" {0!s}".format("$SLURM_ARRAY_TASK_ID")]
		# header += ["echo \"array is\" \"{0!s}\"".format(key)]
		# header += ["echo \"value is\" \"{0!s}\"".format(value)]
		calls = header + [' '.join([self.func, value, flags])]
		return calls

	def buildJobs(self, jobArray, size):
		self.jobArray = job.JobArray(jobArray, size, cpu = self.cpu,
				mem = self.mem, qos = self.qos,	time = self.time)

	def run(self):
		sbatchArgs = self.buildPrecursor()
		script = self.prepareBatch()
		print("Running as job array")
		numJobs = len(self.batch)
		call = ['sbatch', '--array=0-{0:d}'.format(numJobs - 1)] +\
			sbatchArgs
		print("Template Job:")
		print(sampleBatch(script))

		# Feed input into subprocess
		script = '\n'.join(script)
		result = job.command(call, input=script)

		if not result:
			print("Failed to submit template")

		parsed = parseOut(result)
		if not parsed:
			print("Template failed")
			print(script)
			print(out)
			return False
		else:
			print("Building JobArray record")
			self.buildJobs(parsed[0], numJobs)
			print("JobArray Submitted")
			return True

	def runTogether(self):

		batch = self.prepareBatch()

		# Adjust the resources (time)
		if self.time is not None:
			self.time = self.time * len(batch)
		sbatchArgs = self.buildPrecursor()

		script = '\n'.join(batch[0][:-1]+[s[-1] for s in batch])
		print(script)
		call = ['sbatch'] + sbatchArgs

		p = subprocess.Popen(call, stdin=subprocess.PIPE,
			stdout=subprocess.PIPE, stderr=subprocess.PIPE,
			universal_newlines=True)
		out,err = p.communicate(input=script)
		if err:
			print("Call resulted in error:")
			print(err)
		self.jobs = self.buildJobs([parseOut(out)[0]])

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

def main():
	parser = argparse.ArgumentParser(description = "Submit a lone job to slurm")
	parser.add_argument(
		"command",
		help = "Command to submit",
		type = str,
		nargs = "+")
	parser.add_argument(
		"container",
		help = "Path to container",
		type = str)
	parser.add_argument(
		"-mo",
		"--modules",
		help = "Modules to import",
		type = str,
		nargs = "+")
	parser.add_argument(
		"-e",
		"--exports",
		help = "Exports to export",
		type = str,
		nargs = "+")

	arguments = args(parser).parse_args()
	interpreter = "#!/bin/sh"
	func = "xvfb-run -a singularity exec -B /om:/om {0!s} /usr/bin/python3 {1!s}".format(
			arguments.container, arguments.command[0])
	command = [[x.replace("'", "") for x in arguments.command[1:]]]


	if arguments.modules:
		modules = arguments.modules
	else:
		modules = []
	modules.append('openmind/singularity/2.2.1')
	modules.append("openmind/xvfb-fix/0.1")
	if arguments.exports:
		exports = zip(arguments.exports[0::2], arguments.exports[1::2])
	else:
		exports = []
	flags = []
	print("Creating slurm batch")
	batch = Batch(interpreter, modules, exports, func, command, flags, arguments)
	print("Submitting slurm batch")
	batch.run()
	print("Submitted!")
if __name__ == '__main__':
	main()
