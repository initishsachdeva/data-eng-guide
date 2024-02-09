# Conditional Statements -> If-else, while loop, for loop

# 1. if-else loop

greeting = "Good Morning"

if greeting == "Morning":
    print(" If loops executed - > Condition matches")
else:
    print(" Else loops executed - > Condition do not match")
print(" If-else block is finished")

if greeting == "Good Morning":
    print(" If loops executed - > Condition matches")
else:
    print(" Else loops executed - > Condition do not match")
print(" If-else block is finished")

a = 5
if a >= 5:
    print(" If loops executed - > Condition matches")
else:
    print(" Else loops executed - > Condition do not match")
print(" If-else block is finished")

# 2. For Loop
obj = [2, 3, 4, 5, 6]
for i in obj:
    print("printing list values using for loop ", i)

for j in obj:
    print("printing list values using for loop with multiple of 2 - > ", j * 2)

# ---- sum if first natural numbers 1+2+3+4+5 = 15
# range(i,j) - > i to j-1
sum = 0
for k in range(1, 6):  # in java we usually write it as for (i =0; i<5;i++)
    sum = sum + k
print("printing sum of first five natural numbers - > ", sum)

for l in range(1, 10, 2):  # This will print with difference of 2 = 1,3,5,7,9
    print("printing with diff of 2 - > ", l)

for m in range(10):  # This will print  = 0 to 9
    print("printing values - > ", m)

# 3. while Loop in Python
temp = 4
while temp > 1:
    if temp != 3:
        print("While Loop execute with temp value is", temp)
    temp = temp - 1
print(" while loop finish it's execution")

temp1 = 10
while temp1 > 1:
    if temp1 == 9:
        temp1 = temp1 - 1
        continue
    if temp1 == 2:
        break
    print("While Loop execute with temp1 value is", temp1)
    temp1 = temp1 - 1
print(" while loop finish it's execution")
