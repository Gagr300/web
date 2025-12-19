import threading
import time
from .Payment import Payment

class PaymentService:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(PaymentService, cls).__new__(cls)
            return cls._instance

    def process_payment(self, order_id, payment_method):
        """Обработать платеж"""
        payment = Payment()

        if payment_method == 'reject':
            payment.done = False
        elif payment_method == 'timeout':
            time.sleep(10)

        return payment


    def update_product_stock(self, name, quantity):
        """Обновить количество продукта"""
        if name in self.products:
            product = self.products[name]
            if product.number >= quantity:
                product.number -= quantity
                self._save_products()
                return {'success': True, 'new_stock': product.number}
            else:
                return {'success': False, 'message': f'Недостаточно товара. В наличии: {product.number}'}
        return {'success': False, 'message': 'Товар не найден'}


    def get_product_details(self, name):
        """Получить детальную информацию о продукте"""
        product = self.products.get(name)
        if product:
            return {
                'name': product.name,
                'number': product.number,
                'cost': product.cost,
                'emoji': product.emoji,
                'available': product.number > 0,
                'low_stock': product.number <= 5
            }
        return None


payment_service = PaymentService()
