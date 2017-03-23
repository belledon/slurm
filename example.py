import argparse
import slurm
# Note: It is recommended to add this a submodule within that path
# that you will be working in. Thus you'll use something like:
#  ` from slurm import slurm`
# Also don't forget your __init__.py's!

def main():
    # Use argparse to make a parser containing your batch arguments
    parser = argparse.ArgumentParser(description="This is a cool example")

    parser.add_argument( 'name', type=str,
        help='Your name')

    parser.add_argument( 'food', type=str,
        help = 'Your favorite food')

    # Pass the parser to slurm so it can add its own optional parameters
    # such as cpu, memory...
    # Call `python example.py -h` to see these arguments
    args = slurm.args(parser).parse_args()

    # Now lets define any job-inpendent arguments (to be applied to all jobs)

    # Interpreter to read the virtual job shell
    interpreter  = '#!/bin/sh'

    # Modules to import. Note: modules are not automatically passed
    # into each subprocess from the current environment.
    modules = []
    # For example (not used here)
    modules.append('openmind/singularity/librefactor-1g397644e')

    # Any variables to export in the form: ("NAME", "VALUE")
    exports = []
    exports.append(("NAME", args.name))
    exports.append(("FOOD", args.food))

    # Any command postfix-flags to be used in all batches
    flags = []

    # Finally the actual execution command.
    # Note this could be a really complicated function call, say to
    # a singularity container, matlab, python, or even better, a python
    # environment inside a singularity container.

    func = "echo $NAME really loves $FOOD"

    # These are the job dependent arguments.
    # They must be a list-of-lists where each inner list are the arguments
    # for `func`
    batches = [["For the {0:d}th time...".format(i)] for i in range(10)]

    # Constructs the batch object
    batch = slurm.Batch(interpreter, modules, exports, func, batches, flags,
        args)
    # Submit the jobarray
    batch.run()
    # Watch the status of the jobs until completion
    batch.monitor()
if __name__ == '__main__':
    main()
