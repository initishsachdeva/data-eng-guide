# classes are user defined blueprint or prototype
# Classes includes - > methods, class variables, instance variables, constructors

class MyClass:
    # variables defined in a class are called class variables
    num = 100

    # default constructor -> in python constructor is created with 2 underscores with init keyword
    def __init__(self):
        print("default constructor executes")

    def __init__(self, c, d):
        self.c = c
        self.d = d
        print("parametrised constructor executes")

    def getData(self):
        print("executing first method in my new class")

    def sum(self, a, b):
        # variables declared inside a constructor/methods are called instance variables
        self.a = a
        self.b = b
        print(a + b + MyClass.num)

    def subtract(self):
        # variables declared inside a constructor/methods are called instance variables
        return self.c - self.d


# in Python we create object of a class by calling the className

obj = MyClass(10, 5)
obj.getData()
print(obj.num)
obj.sum(2, 3)
print(obj.subtract())
