""" NAME: Jesse Aronson
	DESC: This program parallelizes the usage of the built-in Python hash function using threading.
"""

from threading import Thread
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
def create_parallel_table(table_size, threads, filename):
#DESC:  This function creates a hash table and sends threads to the worker function.
#		This funcion aids in the insertion process of creating the hash table.
#INPUT:  table_size = number to represent size of hash table
#	     threads = number of threads used to thread the insertion process
#		 filename = file containing strings (one string per line)
#OUTPUT: table = an implemented hash table with strings from filename inserted

	all_threads = []
	table = list(range(table_size)) # initialize a new hash table of size table_size
	for i in range(len(table)):
		table[i] = 0

	my_file = open(filename, "r+")
	file_list = list(map(str.strip, my_file.readlines()))

	num_divisions = threads
	division = int(len(file_list) / num_divisions)  # create n divisions of the file

	start = 0
	end = division

	for i in range(num_divisions): # iterate through each division of the data

		# create new thread for each division
		all_threads.append(Thread(target=hash_worker, args=(table,file_list[start:end])))

		start += division
		end += division

	for thread in all_threads:
		thread.start()
	for thread in all_threads:
		thread.join()

	return table

def create_linear_table(table_size, filename):
#DESC:   This function inserts strings from filename without using parallelism.
#INPUT:  table_size = number to represent size of hash table
#		 filename = file containing strings (one string per line)
#OUTPUT: table = an implemented hash table with strings from filename inserted

	table = list(range(table_size)) # initialize a new hash table of size table_size
	for i in range(len(table)):
		table[i] = 0

	my_file = open(filename, "r+")
	file_list = list(map(str.strip, my_file.readlines()))

	hash_worker(table, file_list)

	return table

def main():
	
	start = time.time()
	# parallel_table = create_parallel_table(1000, 50, "text.txt")
	# end = time.time()
	# print("%.5f" % (end - start))


	# start1 = time.time()
	# linear_table = create_linear_table(1000, "text.txt")
	# end1 = time.time()
	# print("%.5f" % (end1 - start1))


if __name__ == "__main__":
	main()
