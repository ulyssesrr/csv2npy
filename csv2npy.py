#!/usr/bin/env python3
# vim: set fileencoding=utf-8 :
import sys
from optparse import OptionParser

import numpy as np

def get_int_tuple_from_args(option, opt, value, parser):
	setattr(parser.values, option.dest, tuple(map(int, value.split(','))))
    
parser = OptionParser(usage="%prog [options] <datasetdir>", epilog="2016 - Ulysses Rangel Ribeiro")
parser.add_option("-d", "--delimiter", dest="delimiter", default=",", help="CSV delimiter (Default: ,).")
parser.add_option("-s", "--skip-rows", action="store", type="int", dest="skiprows", default=0, help="Skip the first N rows.")
parser.add_option('-c', '--usecols', type='string', action='callback', callback=get_int_tuple_from_args, dest='usecols')
parser.add_option("-n", "--num-rows", action="store", type="int", dest="n_rows", default=None, help="Number of rows to read.")
parser.add_option("-t", "--dtype", dest="dtype", default="float32", help="Data type (Default: float32).")
parser.add_option("-P", "--no-allow-pickle", action="store_false", default="True", dest="allow_pickle", help="See numpy.save docs.")
parser.add_option("-F", "--no-fix-imports", action="store_false", default="True", dest="fix_imports", help="See numpy.save docs.")
(options, args) = parser.parse_args()

if len(args) == 0:
	parser.print_help()
	sys.exit()
	
print(options)

def iter_loadtxt(fname, delimiter=',', skiprows=0, dtype=float, usecols=None, count=-1):
	if usecols is not None:
		usecols = np.array(usecols)
	def iter_func():
		with open(fname, 'r') as infile:
			for _ in range(skiprows):
				next(infile)
			for line in infile:
				line = np.array(line.rstrip().split(delimiter))
				if usecols is not None:
					line = line[usecols]
				for item in line:
					yield dtype(item)
				#print("iter_loadtxt.rowlength", len(line))
				iter_loadtxt.rowlength = len(line)

	data = np.fromiter(iter_func(), dtype=dtype, count=count)
	data = data.reshape((-1, iter_loadtxt.rowlength))
	return data

n_files = len(args)
for i, f in enumerate(args):
	idx = i + 1
	print("[%d/%d] Loading: %s" % (idx, n_files, f))
	data = iter_loadtxt(fname=f, dtype=getattr(np, options.dtype),  delimiter=options.delimiter, skiprows=options.skiprows, usecols=options.usecols, count=options.n_rows*len(options.usecols))
	print("[%d/%d] Saving: %s.npy" % (idx, n_files, f))
	np.save("%s.npy" % (f), data, allow_pickle=options.allow_pickle, fix_imports=options.fix_imports)
	
	
print("All files processed.")
