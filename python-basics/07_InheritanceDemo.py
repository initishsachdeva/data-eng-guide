class ParentClass:

    def sum(self, a, b):
        # variables declared inside a constructor/methods are called instance variables
        self.a = a
        self.b = b
        print(a + b)


class ChildClass(ParentClass):
    print(sum())


obj = ChildClass(2, 2)
