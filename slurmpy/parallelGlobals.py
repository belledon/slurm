# To allow thread safe global distribution of variables related to joblib.Parallel

from os import environ


# Assign the global core number to initialize as 1
_NUM_CORES = 1
# Ensure a _LOCKED state prior to modification
_LOCKED = True

def _unlock():
	global _LOCKED
	if _LOCKED:
		_LOCKED = False
		return True
	else:
		print("Cannot unlock, already unlocked")
		return False

def _lock():
	global _LOCKED
	if _LOCKED:
		print("Cannot lock, already locked")
		return False
	else:
		_LOCKED = True
		return True

def _getCores():
	return environ.get('NUM_CORES') 

def _setCores(n):
	#try:
	environ['NUM_CORES'] = str(n)
	return True
	#except:
	#	print("Could not change NUM_CORES")
	#	return False

def setCores(n):
	if _unlock():
		global _NUM_CORES
		if _setCores(n):
			_NUM_CORES = n
			if _lock():
				print("Set number of cores to {:d}".format(n))
			else:
				print("Attempted to lock an already locked state?")
			return True

		else:
			return False
	else:
		print("Cannot change the state when currently being modified")
		return False
		
def initialize():
        print('Initializing Parallel Globals')

        global _NUM_CORES
        _NUM_CORES = _getCores()
        if _NUM_CORES is None:
                print("Setting intial cores to 1")
                setCores(1)
        else:
                print("Environmental Variable: NUM_CORES found.")
        global _LOCKED
        _LOCKED = True
        return True


_INITIALIZED = initialize()




def getCores():
	global _INITIALIZED
	if not _INITIALIZED:
		print("Attemping to retrieve cores prior to"+\
			"initializing parralelGlobals!")
		initialize()
	global _LOCKED
	if _LOCKED:
		global _NUM_CORES
		return int(_NUM_CORES)
	else:
		print("The state is _LOCKED.. trying again in 5s")
		return getCores()

