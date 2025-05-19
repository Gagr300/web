class Product:
    def __init__(self, name, number):
        self.name = name
        self.number = number

    def buy(self, number):
        if self.number >= number:
            self.number -= number
            return True
        else:
            return False
