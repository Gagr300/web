class PaymentOutbox:
    def __init__(self, queue, body, date):
        self.queue = queue
        self.body = body
        self.date = date
        self.status = 'new'

    def done(self):
        self.status = 'done'
