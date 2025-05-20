import random
import threading
import json
import time
from pika import BlockingConnection, ConnectionParameters
from Order import Order
from Outbox import OrderOutbox

connection_parameters = ConnectionParameters(
    host='localhost',
    port=5672,
)

orders = dict()

outbox = []

fruits = ['apple', 'banana', 'orange', 'pear', 'peach', 'mango']


def process_payment_done(ch, method, properties, body):
    print(f'[order] {json.loads(body)["id"]}  Message received <payment_done>: {json.loads(body)}')
    orders[json.loads(body)['id']].done_order()
    ch.basic_ack(delivery_tag=method.delivery_tag)


def process_payment_not_done_order(ch, method, properties, body):
    print(f'[order] [not] {json.loads(body)["id"]} Message received <payment_not_done>: {json.loads(body)}')
    orders[json.loads(body)['id']].reject_order()
    ch.basic_ack(delivery_tag=method.delivery_tag)


def process_product_not_found(ch, method, properties, body):
    print(f'[order] [not] {json.loads(body)["id"]} Message received <product_not_found>: {json.loads(body)}')
    orders[json.loads(body)['id']].reject_order()
    ch.basic_ack(delivery_tag=method.delivery_tag)


def main():
    with BlockingConnection(connection_parameters) as conn:
        with conn.channel() as ch:
            ch.queue_declare(queue='order_created_queue')
            ch.queue_declare(queue='payment_done_queue')
            ch.queue_declare(queue='payment_not_done_order_queue')
            ch.queue_declare(queue='product_not_found_queue')
            ch.queue_declare(queue='product_found_queue')
            for i in range(30):
                order = dict()

                for _ in range(random.randint(1, 2)):
                    order.update({random.choice(fruits): random.randint(1, 4)})
                try:
                    orders.update({i: Order(i, order)})
                    outbox.append(
                        OrderOutbox('order_created_queue',
                                    json.dumps({'id': i, 'order': orders[i].products_amount}),
                                    time.time()))
                    # ch.basic_publish(exchange='',
                    #                  routing_key='order_created_queue',
                    #                  body=json.dumps(dict(id=i, order=orders[i].products_amount))
                    #                  )
                    print(f'[order] {i} Message sent!')
                except BaseException as e:
                    orders.pop(i, 0)
                    if outbox[-1].queue == 'order_created_queue' and outbox[-1].data == json.dumps(
                            {i: Order(i, order)}):
                        outbox.pop()
                    print('except', e)

            ch.basic_consume(
                queue='payment_done_queue',
                on_message_callback=process_payment_done,
            )

            ch.basic_consume(
                queue='payment_not_done_order_queue',
                on_message_callback=process_payment_not_done_order,
            )

            ch.basic_consume(
                queue='product_not_found_queue',
                on_message_callback=process_product_not_found,
            )

            def repeater(interval, function):
                threading.Timer(interval, repeater, [interval, function]).start()
                function()

            def send():
                global outbox
                for i, message in enumerate(outbox):
                    if message.status == 'new':
                        try:
                            ch.basic_publish(exchange='',
                                             routing_key=message.queue,
                                             body=message.body)
                            message.status = 'done'
                        except BaseException as e:
                            message.status = 'new'
                            print('except:', e)
                outbox = [message for message in outbox if message.status == 'new']

            # def pde():
            #     conn.process_data_events(time_limit=None)

            repeater(1, send)

            # repeater(30, pde)

            print('[order] Waiting...')
            ch.start_consuming()


if __name__ == '__main__':
    main()
