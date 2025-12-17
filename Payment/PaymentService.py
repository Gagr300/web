import threading


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
        # Здесь может быть логика обработки платежей через различные системы
        # В данном примере просто имитируем успешный платеж
        from .Payment import Payment
        payment = Payment()

        # Имитация различных сценариев
        if payment_method == 'reject':
            payment.done = False
        elif payment_method == 'timeout':
            # Имитация таймаута
            import time
            time.sleep(10)  # Долгая обработка

        return payment


payment_service = PaymentService()