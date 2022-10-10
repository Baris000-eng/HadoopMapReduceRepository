#!/usr/bin/python
import sys

# reading entire line from STDIN (standard input)
for l in sys.stdin:
	 # to remove leading and trailing whitespace
	 l = l.strip()
	 # split each line to words
	 wordList = l.split()

	 for w in wordList:
	 	print('%s\t%s' % (w, 1)) #for each word in the word list, assign the count of word to 1 

