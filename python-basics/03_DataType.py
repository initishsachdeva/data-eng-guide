# -------------------Data types in python--------------------

# Numeric = int(long is deprecated, so every long number will fall under int),float, complex = (100 + 3j) will fall under complex.
# String = whatever you put in double or single quotes will fall under string

# --------------Numeric and String discussed in 02_Variable.py file

# -----------List (array in another programming language)

values = [1, 2, "abc", 4.5]  # in python, you can have multiple data type in list, and it starts with 0th index.
print("printing list items", values[0])
print("printing list items", values[1])
print("printing list items", values[2])
print("printing list items", values[3])

print("printing LAST INDEX from list items",
      values[-1])  # To print last index from list, this will print last index from list

print("printing SUBSTRING from list items",
      values[1:3])  # Substring where 3 is exclusive, so it will print till abc string

values.insert(3, "def")  # you can inject value in bw
print("INSERTING new values into list items", values)

values.append(234566)  # to add value at last index
print("APPENDING at last Index in list items", values)

values[2] = "ABC"  # to edit the already existing value
print("To EDIT or MODIFY the already present items in list ", values)

del values[0]  # to delete the already existing value
print("DELETING list items", values)

# ----------------------- Tuple ------------------------

# --List and Tuple are same thing but Tuple is immutable whereas List is mutable where you can modify the list and
# tuple once created you cannot modify it.

listValues = [1, 2, "abc", 4.5]
listValues[3] = "def"
print("printing list items", listValues)

tupleValues = (1, 2, "abc", 4.5)
# tupleValues[3] = "def" # this will throw error because tuple cannot be modified
print("printing TUPLE items", tupleValues)

# --------------------Dictionary----------------

# it will have KEY Value Pait like in java we hve HASHMAP

sampleDictionary = {1: "First", 2: "Second", 3: 3, "A": 4}
print("printing sample dictionary elements using keys", sampleDictionary[1])
print("printing sample dictionary elements using keys", sampleDictionary[2])
print("printing sample dictionary elements using keys", sampleDictionary[3])
print("printing sample dictionary elements using keys",
      sampleDictionary["A"])  # if key is string make it in double quotes

# ------------------How to create dictionaries at run time ------------------------
runTimeDict = {}  # empty dictionary

runTimeDict["firstname"] = "ABC"
runTimeDict["lastname"] = "DEF"

print("add values to empty dictionary", runTimeDict)

runTimeDict["gender"] = "M"

print("add values to empty dictionary", runTimeDict["gender"])
