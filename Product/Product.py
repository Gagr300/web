class Product:
    def __init__(self, name, number, cost=0, emoji=''):
        self.name = name
        self.number = number
        self.cost = cost
        self.emoji = emoji

    def buy(self, number):
        if self.number >= number:
            self.number -= number
            return True
        else:
            return False

    def to_dict(self):
        return {
            'name': self.name,
            'number': self.number,
            'cost': self.cost,
            'emoji': self.emoji
        }