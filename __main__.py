import argparse
from slurm import slurm 

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

	arguments = slurm.args(parser).parse_args()
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
	batch = slurm.Batch(interpreter, modules, exports, func, command, flags, arguments)
	print("Submitting slurm batch")
	batch.run()
	print("Submitted!")
if __name__ == '__main__':
	main()