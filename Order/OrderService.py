import json
import os
import threading
from .Order import Order


class OrderService:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(OrderService, cls).__new__(cls)
                cls._instance._initialize()
            return cls._instance

    def _initialize(self):
        self.orders = {}
        self.next_id = 1
        self.data_file = os.path.join(os.path.dirname(__file__), 'orders.jsonl')
        self._load_orders()

    def _load_orders(self):
        """Загрузка заказов из файла"""
        if os.path.exists(self.data_file):
            with open(self.data_file, 'r', encoding='utf-8') as f:
                for line in f:
                    if line.strip():
                        order_data = json.loads(line.strip())
                        order = Order(order_data['id'], order_data['products_amount'])
                        order.date = order_data['date']
                        order.status = order_data['status']
                        self.orders[order.id] = order
                        if order.id >= self.next_id:
                            self.next_id = order.id + 1

    def create_order(self, products_amount):
        """Создать новый заказ"""
        order_id = self.next_id
        self.next_id += 1

        order = Order(order_id, products_amount)
        self.orders[order_id] = order
        self._save_order(order)

        return order

    def get_order(self, order_id):
        """Получить заказ по ID"""
        return self.orders.get(order_id)

    def update_order_status(self, order_id, status):
        """Обновить статус заказа"""
        if order_id in self.orders:
            order = self.orders[order_id]
            if status == 'Done':
                order.done_order()
            elif status == 'Rejected':
                order.reject_order()
            self._save_order(order)
            return True
        return False

    def _save_order(self, order):
        """Сохранить заказ в файл"""
        order_data = {
            'id': order.id,
            'products_amount': order.products_amount,
            'date': order.date.isoformat(),
            'status': order.status
        }

        with open(self.data_file, 'a', encoding='utf-8') as f:
            f.write(json.dumps(order_data, ensure_ascii=False) + '\n')


order_service = OrderService()