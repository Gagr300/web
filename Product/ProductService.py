import json
import os
import threading
from .Product import Product

class ProductService:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(ProductService, cls).__new__(cls)
                cls._instance._initialize()
            return cls._instance

    def _initialize(self):
        self.products = {}
        self.data_file = os.path.join(os.path.dirname(__file__), 'products.jsonl')
        self._load_products()

    def _load_products(self):
        """Загрузка продуктов из файла с сохранением цветов"""
        if os.path.exists(self.data_file):
            with open(self.data_file, 'r', encoding='utf-8') as f:
                for line in f:
                    if line.strip():
                        product_data = json.loads(line.strip())
                        # Создаем продукт с цветом, если он есть
                        color = product_data.get('color')
                        if not color:
                            # Если цвета нет, генерируем случайный цвет
                            import random
                            color = [
                                random.randint(50, 250),
                                random.randint(50, 250),
                                random.randint(50, 250)
                            ]

                        self.products[product_data['name']] = Product(
                            name=product_data['name'],
                            number=product_data['number'],
                            cost=product_data.get('cost', 0),
                            emoji=product_data.get('emoji', ''),
                            color=color
                        )
        else:
            # Инициализация начальными данными с цветами
            initial_products = [
                {'name': 'виноград', 'number': 10, 'cost': 51, 'emoji': '🍇', 'color': [88, 66, 124]},
                {'name': 'голубика', 'number': 12, 'cost': 53, 'emoji': '🫐', 'color': [70, 65, 150]},
                {'name': 'черника', 'number': 10, 'cost': 51, 'emoji': '🫐', 'color': [79, 134, 247]},
                {'name': 'вишня', 'number': 20, 'cost': 50, 'emoji': '🍒', 'color': [207, 2, 52]},
                {'name': 'яблоко', 'number': 15, 'cost': 50, 'emoji': '🍎', 'color': [227, 16, 72]},
                {'name': 'клубника', 'number': 10, 'cost': 51, 'emoji': '🍓', 'color': [251, 41, 67]},
                {'name': 'арбуз', 'number': 13, 'cost': 66, 'emoji': '🍉', 'color': [253, 70, 89]},
                {'name': 'апельсин', 'number': 17, 'cost': 55, 'emoji': '🍊', 'color': [253, 143, 57]},
                {'name': 'персик', 'number': 24, 'cost': 80, 'emoji': '🍑', 'color': [244, 121, 131]},
                {'name': 'морковь', 'number': 67, 'cost': 16, 'emoji': '🥕', 'color': [233, 105, 44]},
                {'name': 'манго', 'number': 13, 'cost': 80, 'emoji': '🥭', 'color': [255, 166, 43]},
                {'name': 'ананас', 'number': 6, 'cost': 53, 'emoji': '🍍', 'color': [243, 208, 146]},
                {'name': 'груша', 'number': 6, 'cost': 45, 'emoji': '🍐', 'color': [255, 254, 197]},
                {'name': 'банан', 'number': 6, 'cost': 53, 'emoji': '🍌', 'color': [250, 254, 75]},
                {'name': 'лимон', 'number': 10, 'cost': 45, 'emoji': '🍋', 'color': [253, 233, 16]},
                {'name': 'киви', 'number': 32, 'cost': 56, 'emoji': '🥝', 'color': [142, 229, 63]},
                {'name': 'молоко', 'number': 20, 'cost': 51, 'emoji': '🥛', 'color': [253, 255, 245]}
            ]

            with open(self.data_file, 'w', encoding='utf-8') as f:
                for product in initial_products:
                    f.write(json.dumps(product, ensure_ascii=False) + '\n')
                    self.products[product['name']] = Product(
                        name=product['name'],
                        number=product['number'],
                        cost=product['cost'],
                        emoji=product['emoji'],
                        color=product['color']
                    )

    def _save_products(self):
        """Сохранение продуктов в файл с сохранением цветов"""
        with open(self.data_file, 'w', encoding='utf-8') as f:
            for product in self.products.values():
                product_data = {
                    'name': product.name,
                    'number': product.number,
                    'cost': product.cost,
                    'emoji': product.emoji,
                    'color': product.color  # Сохраняем цвет!
                }
                f.write(json.dumps(product_data, ensure_ascii=False) + '\n')

    def get_all_products(self):
        """Получить все продукты"""
        return list(self.products.values())

    def get_product(self, name):
        """Получить продукт по имени"""
        return self.products.get(name)

    def update_product_stock(self, name, quantity):
        """Обновить количество продукта"""
        if name in self.products:
            product = self.products[name]
            if product.number >= quantity:
                product.number -= quantity
                self._save_products()
                return True
        return False

    def get_products_by_names(self, product_names):
        """Получить продукты по списку имен"""
        result = []
        for name in product_names:
            if name in self.products:
                result.append(self.products[name])
        return result


product_service = ProductService()