# open method is sued to open any file in python

file = open('textSample.txt')

# read all the content of file
# print(file.read())

# read first 2 characters in file
# print(file.read(2))

# read line by line
# print(file.readline())
# print(file.readline())

# print line by line using readline method - if there are 100 lines ,how to print all those

# line = file.readline()
# while line != "":
#     print(line)
#     line = file.readline()

for line in file.readlines():
    print(line)

# anytime you open any file, please close it as well to avoid any memory leak
file.close()
