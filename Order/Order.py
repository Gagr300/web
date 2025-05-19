import datetime


class Order:
    def __init__(self, id: int, products_amount: dict):
        self.id = id
        self.products_amount = products_amount
        self.date = datetime.datetime.now()
        self.status = 'In progress'

    def done_order(self):
        self.status = 'Done'

    def reject_order(self):
        self.status = 'Rejected'
