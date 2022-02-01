from abc import ABC, abstractmethod

class Shape(ABC):

    @abstractmethod
    def sides(self):
        pass

    def color(self):
        print('My color is red')

class Triangle(Shape):

    # must have this method from the abstract class or will throw an error
    def sides(self):
        print('I have 3 sides')

    # if this method is not created in the subclass then the color method from the shape class will be used
    def color(self):
        print('My color is blue')


shape1 = Triangle()

shape1.color()
