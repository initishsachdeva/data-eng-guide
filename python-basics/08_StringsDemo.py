str = "ABCDEFGH.com"
str1 = "IJKLMNOP"
str2 = "ABC"
# print specific chars of a string
print(str[1])

# print substring
print(str[0:3])

# concatenate two string
print(str + str1)

# verify if string is present in particular string or not
print(str2 in str)

# split the string
splitStr = str.split(".")
print(splitStr)
print(splitStr[0])
print(splitStr[1])

# trim -> for removing white spaces
strTrim = " whitespaces     "
print(strTrim)
print(strTrim.strip())
print(strTrim.lstrip())  # remove space from beginning
print(strTrim.rstrip())  # remove space from end
