import json
import time
from pika import BlockingConnection, ConnectionParameters
from Product import Product

connection_parameters = ConnectionParameters(
    host='localhost',
    port=5672,
)

products = {
    'apple': Product('apple', 30),
    'banana': Product('banana', 2),
    'orange': Product('orange', 23),
    'pear': Product('pear', 25),
    'peach': Product('peach', 15),
    'mango': Product('peach', 15),
}


def main():
    t = time.time()
    with BlockingConnection(connection_parameters) as conn:
        with conn.channel() as ch:

            def process_order_created(ch, method, properties, body):
                print(f'[product] {json.loads(body)["id"]} Message received: {body.decode()}')
                order_products = json.loads(body)['order']
                flag = True
                for product in order_products:
                    if product in products.keys() and products[product].number >= order_products[product]:
                        products[product].number -= order_products[product]
                    else:
                        flag = False

                if flag:
                    ch.queue_declare(queue='product_found_queue')
                    ch.basic_publish(exchange='',
                                     routing_key='product_found_queue',
                                     body=body
                                     )
                    print(f'[product] {json.loads(body)["id"]} Message sent!')
                else:
                    ch.queue_declare(queue='product_not_found_queue')
                    ch.basic_publish(exchange='',
                                     routing_key='product_not_found_queue',
                                     body=body
                                     )
                    print(f'[product] [not] {json.loads(body)["id"]} Message sent!')

                ch.basic_ack(delivery_tag=method.delivery_tag)

            def process_payment_not_done(ch, method, properties, body):
                print(f'[product] {json.loads(body)["id"]} Message received: {json.loads(body)}')
                order_products = json.loads(body)['order']
                for product in order_products:
                    products[product].number += order_products[product]
                ch.queue_declare(queue='payment_not_done_order_queue')
                ch.basic_publish(exchange='',
                                 routing_key='payment_not_done_order_queue',
                                 body=body
                                 )
                ch.basic_ack(delivery_tag=method.delivery_tag)
                print(f'[product] [not] {json.loads(body)["id"]} Message sent!')

            ch.queue_declare(queue='order_created_queue')
            ch.queue_declare(queue='payment_not_done_queue')

            ch.basic_consume(
                queue='order_created_queue',
                on_message_callback=process_order_created,
            )

            ch.basic_consume(
                queue='payment_not_done_queue',
                on_message_callback=process_payment_not_done,
            )
            while ((time.time() - t) < 30):
                t = time.time()
                conn.process_data_events(time_limit=None)

            print('[product] Waiting...')
            ch.start_consuming()


if __name__ == '__main__':
    main()
