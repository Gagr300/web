# Product.py
class Product:
    def __init__(self, name, number, cost=0, emoji='', color=None):
        self.name = name
        self.number = number
        self.cost = cost
        self.emoji = emoji
        self.color = color or [255, 255, 255]  # Default to white if no color

    def buy(self, number):
        if self.number >= number:
            self.number -= number
            return True
        else:
            return False

    def get_color_css(self):
        """Возвращает цвет в формате CSS rgba"""
        if len(self.color) == 3:
            return f'rgb({self.color[0]}, {self.color[1]}, {self.color[2]})'
        elif len(self.color) == 4:
            return f'rgba({self.color[0]}, {self.color[1]}, {self.color[2]}, {self.color[3]/255 if self.color[3] > 1 else self.color[3]})'
        return 'rgb(255, 255, 255)'

    def get_light_color(self, opacity=0.2):
        """Возвращает более светлый вариант цвета для фона"""
        if len(self.color) >= 3:
            return f'rgba({self.color[0]}, {self.color[1]}, {self.color[2]}, {opacity})'
        return 'rgba(255, 255, 255, 0.2)'

    def to_dict(self):
        return {
            'name': self.name,
            'number': self.number,
            'cost': self.cost,
            'emoji': self.emoji,
            'color': self.color
        }