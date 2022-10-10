#!/usr/bin/env python  
import sys

cur_count = 0
cur_word = None

for l in sys.stdin:
    l = l.strip()#remove all the spaces in the end and beginning
    word, wordCount = l.split('\t', 1) #split the data based on tab
    wordCount = int(wordCount)  #change the type of the count variable to integer.
    if cur_word != word:
         if cur_word:
	 	    print('%s\t%s' % (cur_word, cur_count))
	    cur_count = wordCount
	    cur_word = word
    else:
	    cur_count = cur_count + wordCount

if cur_word == word:
	print('%s\t%s' % (cur_word, cur_count))
