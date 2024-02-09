# write into file. you need to tell python in which mode you are opening a file

# ----------- operations ----------------
# read the file and store all in list
# reverse the list
# write the list back to file

with open('textSample.txt', 'r') as reader:
    content = reader.readlines()
    reversed(content)  # in python we have method which will reverse the list automatically
with open('textSample.txt', 'w') as writer:
    for line in reversed(content):
        writer.write(line)
