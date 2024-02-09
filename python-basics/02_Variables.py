# ------------variables in python--------------------------------
a = 3  # it will automatically understand the data type at run time
print(a)

b = "Hello world !!"
print(b)

c, d, e = 5, 6.2, "Combination of different data types"
print(c, d, e)

# -------------How to concatenate string and integer in python--------------
# print("Value is" + c)
# The above statement will throw error because TypeError: can only concatenate str (not "int") to str

# In python, we can concatenate strings and integer value by using below statement
print("{} {}".format("Value is", c))

# --------------How to get type at run time-----------------------------
print(type(a))
print(type(b))
print(type(c))
print(type(d))
print(type(e))
