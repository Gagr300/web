# ProductService.py (исправленная версия)
import json
import os
import threading


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
        """Загрузка продуктов из файла"""
        if os.path.exists(self.data_file):
            with open(self.data_file, 'r', encoding='utf-8') as f:
                for line in f:
                    if line.strip():
                        product_data = json.loads(line.strip())
                        self.products[product_data['name']] = Product(
                            name=product_data['name'],
                            number=product_data['number'],
                            cost=product_data.get('cost', 0),
                            emoji=product_data.get('emoji', '')
                        )
        else:
            # Инициализация начальными данными
            initial_products = [
                {'name': 'яблоко', 'number': 30, 'cost': 50, 'emoji': '🍎'},
                {'name': 'банан', 'number': 65, 'cost': 150, 'emoji': '🍌'},
                {'name': 'апельсин', 'number': 10, 'cost': 155, 'emoji': '🍊'},
                {'name': 'груша', 'number': 7, 'cost': 121, 'emoji': '🍐'},
                {'name': 'лимон', 'number': 18, 'cost': 45, 'emoji': '🍋'},
                {'name': 'персик', 'number': 24, 'cost': 80, 'emoji': '🍑'},
                {'name': 'манго', 'number': 16, 'cost': 180, 'emoji': '🥭'},
                {'name': 'морковь', 'number': 74, 'cost': 16, 'emoji': '🥕'}
            ]

            with open(self.data_file, 'w', encoding='utf-8') as f:
                for product in initial_products:
                    f.write(json.dumps(product, ensure_ascii=False) + '\n')
                    self.products[product['name']] = Product(
                        name=product['name'],
                        number=product['number'],
                        cost=product['cost'],
                        emoji=product['emoji']
                    )

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

    def _save_products(self):
        """Сохранение продуктов в файл"""
        with open(self.data_file, 'w', encoding='utf-8') as f:
            for product in self.products.values():
                product_data = {
                    'name': product.name,
                    'number': product.number,
                    'cost': product.cost,
                    'emoji': product.emoji
                }
                f.write(json.dumps(product_data, ensure_ascii=False) + '\n')

    def get_products_by_names(self, product_names):
        """Получить продукты по списку имен"""
        result = []
        for name in product_names:
            if name in self.products:
                result.append(self.products[name])
        return result


product_service = ProductService()