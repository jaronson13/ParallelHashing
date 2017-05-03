""" NAME: Jesse Aronson
	DESC: This program parallelizes the usage of the built-in Python hash function using multiprocessing.

"""

from multiprocessing import Process, Manager
from collections import Counter
import time

def hash_worker(table, partition):
#DESC: This function takes in a partition of a set of strings and hashes each one.
#	   Handles the parallel hashing of strings.
#	   Also handles the linear hashing of strings if partition = complete number of strings
#INPUT: table = hash table of size n
#	    partition = various strings from an input file or array
#OUTPUT: No return - strings are hashed and stored in the table

	for string in partition:
		key = hash(string)
		index = key % len(table)
		
		if table[index] == 0:  # if index is open (no collision)
			table[index] = string
		else:  # if index is taken, begin chaining
			if type(table[index]) is list: # check to see if chain already exists
				new_list = table[index]
				new_list.append(string)

				table[index] = new_list

			elif type(table[index]) is str: # create new chain if collision but no chain yet
				new_list = []
				new_list.append(table[index])
				new_list.append(string)

				table[index] = new_list
def create_parallel_table(table_size, processes, filename):
#DESC:  This function creates a hash table and sends processes to the worker function.
#		This funcion aids in the insertion process of creating the hash table.
#INPUT:  table_size = number to represent size of hash table
#	     processes = number of processes used to multiprocess the insertion of values
#		 filename = file containing strings (one string per line)
#OUTPUT: table = an implemented hash table with strings from filename inserted

	manager = Manager()
	procs = []
	table = manager.list(range(table_size)) # initialize a new hash table of size table_size
	for i in range(len(table)):
		table[i] = 0

	my_file = open(filename, "r+", encoding='utf-8', errors='ignore')
	file_list = list(map(str.strip, my_file.readlines()))

	num_divisions = processes
	division = int(len(file_list) / num_divisions)  # create n divisions of the file

	start = 0
	end = division

	for i in range(num_divisions): # iterate through each division of the data

		# create new process for each division
		procs.append(Process(target=hash_worker, args=(table,file_list[start:end])))

		start += division
		end += division

	for proc in procs:
		proc.start()
	for proc in procs:
		proc.join()

	return table

def create_linear_table(table_size, filename):
#DESC:   This function inserts strings from filename without using parallelism.
#INPUT:  table_size = number to represent size of hash table
#		 filename = file containing strings (one string per line)
#OUTPUT: table = an implemented hash table with strings from filename inserted

	table = list(range(table_size)) # initialize a new hash table of size table_size
	for i in range(len(table)):
		table[i] = 0

	my_file = open(filename, "r+", encoding='utf-8', errors='ignore')
	file_list = list(map(str.strip, my_file.readlines()))

	hash_worker(table, file_list)

	return table

def main():
	
	start = time.time()
	# parallel_table = create_parallel_table(3000, 6, "text.txt")
	# end = time.time()
	# print(end - start)


	# start1 = time.time()
	# linear_table = create_linear_table(3000, "text.txt")
	# end1 = time.time()
	# print(end1 - start1)


if __name__ == "__main__":
	main()
