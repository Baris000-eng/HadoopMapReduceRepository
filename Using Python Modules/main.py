from itertools import groupby

def mapping_function(word):
    wordWithAlphabetChars = ''.join([j for j in word if j.isalpha()])
    return [wordWithAlphabetChars, 1]


def reduction_function(key, values):
    return [key, sum([v[1] for v in values])]  # return count sum for each word


with open("input.txt") as file:  # reading the txt file
    words = [my_word for line in file for my_word in line.split()]  # splitting the words

map_res = map(mapping_function, words)  # map all words in the txt file with a count of 1.
sorted_map_res = sorted(map_res, key=lambda y: y[0])  # sort the map outcome according to words.
result = []
grouped_and_sorted = groupby(sorted_map_res, key=lambda x: x[0])  # group the sorted outcome
for key, value in grouped_and_sorted:
    value = list(value)
    print([key, value])
    result.append(reduction_function(key, value))  # converting grouper object to list
    # for each word, append the total word count to the result list

for i in range(0, len(result)):
    print(str(result[i][0]) + "=>" + str(result[i][1]))  # displaying results
