import random
import json
import time
from pika import BlockingConnection, ConnectionParameters
from Order import Order

connection_parameters = ConnectionParameters(
    host='localhost',
    port=5672,
)

orders = dict()

fruits = ['apple', 'banana', 'orange', 'pear', 'peach', 'mango']


def process_payment_done(ch, method, properties, body):
    print(f'[order] Message received <payment_done>: {json.loads(body)}')
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
    t = time.time()
    with BlockingConnection(connection_parameters) as conn:
        with conn.channel() as ch:
            ch.queue_declare(queue='order_created_queue')
            ch.queue_declare(queue='payment_done_queue')
            ch.queue_declare(queue='payment_not_done_order_queue')
            ch.queue_declare(queue='product_not_found_queue')
            for i in range(30):
                order = dict()

                for _ in range(random.randint(1, 2)):
                    order.update({random.choice(fruits): random.randint(1, 4)})
                orders.update({i: Order(i, order)})
                ch.basic_publish(exchange='',
                                 routing_key='order_created_queue',
                                 body=json.dumps({'id': i, 'order': orders[i].products_amount})
                                 )
                print(f'[order] {json.loads(body)["id"]} Message sent!')

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


            while ((time.time() - t) < 30):
                t = time.time()
                conn.process_data_events(time_limit=None)

            print('[order] Waiting...')
            ch.start_consuming()


if __name__ == '__main__':
    main()
