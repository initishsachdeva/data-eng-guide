Items = 0
# in Python we use keyword "raise" for throwing an exception

if Items != 2:
    pass  # this will not fail your condition if it returns false
    # raise Exception("Values not matching")

assert (Items == 0)  # it expectes conditions to return true and if it is false it will break

# try-catch block

try:
    with open('textSample1.txt', 'r') as reader:  # this file doesn't exist
        reader.read()

# in python we use keyword 'except' as a catch block
except Exception as e:
    print("file does not exist", e)

# it executes everytime irrespective of errors you receive earlier in block
finally:
    print("cleaning the resources")
