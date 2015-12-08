import mappings
import numpy as np
import os

"Create serialized np arrays from all .csv files, "


def open_convert():
	npa=np.genfromtxt( fn, delimiter=',', dtype='float')

def open_pickle():
	npa=np.load(my_pickle)

def check_same():
	npa=np.genfromtxt( fn, delimiter=',', dtype='float')
	npa_p=np.load(my_pickle)
	
	
for s in mappings.scenarios:	
	this_root_dir=s['location']
	for root, dirs, files in os.walk(this_root_dir):
		for d in dirs:
			for f in os.listdir(os.path.join(root, d)):
				fn=os.path.join(root, d, f)
				if os.path.isfile(fn) and fn.endswith('.csv'):
					print('processing {}'.format(fn), end=" ")
					pfn=fn + ".pkl"
					npa=np.genfromtxt( fn, delimiter=',', dtype='float')
					npa.dump(pfn)
					print(".....DONE")
					a=1