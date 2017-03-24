#job.py
######################################################
# 2/2/2017
# Class representation of slurm job.
# _____________________________________________________
# author(s): Mario E. Belledonne

import numpy as np
import subprocess
from time import sleep

jobStates = ["ST", "CD", "PD", "PE", "RU"]
categories = ["JOBID", 'PARTITION', 'NAME', 'USER', 'ST', 'TIME', 'NODES',
'NODELIST(REASON)']


def command(c, input = None):

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

	if not err:
		return out
	else:
		print("Command {0!s} resulted in the error :".format(
			str(c[0])))
		print(err)
		print(out)
		return False

def parsePoll(out):
	parsed = [[c for c in line.split(" ") if c] for line in out if line]
	return np.array(parsed)
#
class JobArray:
	def __init__(self, arrayID, size, parent = None, cpu=1, mem=1, qos=None, time=None):
		self.arrayID = arrayID
		self.parent = parent
		self.cpu = cpu
		self.mem = mem
		self.qos = qos
		self.time = time
		self.size = size
		self.jobs = ["{0!s}_{1:d}".format(arrayID, jobID)
					for jobID in range(size)]


	def __eq__(self, other):
		if isinstance(other, self.__class__):
			return self.jobId == other.jobId
		return False

	def __hash__(self):
		return hash(self.jobId)

	# Find the jobs for the job array
	def update(self):
		arrayID = self.arrayID
		jobs = self.jobs
		# Ping SLURM
		statuses = self.poll()
		#Check which jobs do not have a report
		notReported = [job for job in jobs if job not in statuses]
		# Missing jobs are already complete
		numComplete = len(notReported)

		search = lambda s: [job for job in iter(statuses) if statuses[job] == s]
		pending = search("PD")
		running = search("R") + search("CG")

		if (len(pending) + len(running)) < len(statuses):
			print("The number of PD and R jobs does not sum to the number of"+\
				" total jobs reported... Does not compute")
			print(statuses)
		return numComplete, pending, running


	def poll(self):
		arrayID = self.arrayID
		data = parsePoll(command(["squeue", "-r", "-j {0!s}".format(
			arrayID)]).split('\n'))
		if len(data) <= 1:
			# print("WARNING: DID NOT RECIEVE STATUS"+\
			# " OF JOBARRAY {0!s} FROM SLURM".format(arrayID))
			return {}
		# columns for the jobid and status
		f = lambda l,t: [i for i,c in enumerate(l) if c == t ][0]
		header = data[0]
		idCol = f(header, "JOBID")
		statusCol = f(header, "ST")
		jobIds = data[1:, idCol]
		statuses = data[1:, statusCol]
		jobs = {jobId: status for jobId,status in zip(jobIds, statuses)}
		# col = [i for i,c in enumerate(parsed[0]) if c == "ST" ][0]
		return jobs


	def cancel(self):
	 	return command(('scancel', ['-j {0:d}'.format(self.jobId)]))
