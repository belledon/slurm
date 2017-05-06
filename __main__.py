import argparse
from src import slurm 

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
		"-f",
		"--flags",
		help = "Flags for command (omit \"-\")",
		type = str,
		nargs = "+")

	parser.add_argument(
		"-e",
		"--exports",
		help = "Exports to export",
		type = str,
		nargs = "+")

	arguments = slurm.args(parser).parse_args()
	interpreter = "#!/bin/sh"
	func = "xvfb-run -a singularity exec -B /om:/om {0!s} /usr/bin/python3 {1!s}".format(
			arguments.container, arguments.command[0])
	
	command = [arguments.command[1:]]
	flag_keys, flag_vals = arguments.flags[0::2], arguments.flags[1::2]
	flags = ["-{} {}".format(*t) for t in zip(flag_keys, flag_vals)]

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

	print("Creating slurm batch")
	batch = slurm.Batch(interpreter, modules, exports, func, command, flags, arguments)
	print("Submitting slurm batch")
	batch.run()
	print("Submitted {}".format(batch.jobArray))
if __name__ == '__main__':
	main()