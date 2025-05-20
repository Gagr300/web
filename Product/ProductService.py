import json
import time
import threading
from pika import BlockingConnection, ConnectionParameters
from Product import Product
from Outbox import ProductOutbox

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

outbox = []


def main():
    t = time.time()
    with BlockingConnection(connection_parameters) as conn:
        with conn.channel() as ch:

            ch.queue_declare(queue='order_created_queue')
            ch.queue_declare(queue='payment_not_done_queue')
            ch.queue_declare(queue='payment_not_done_order_queue')
            ch.queue_declare(queue='product_found_queue')

            def process_order_created(ch, method, properties, body):
                print(f'[product] {json.loads(body)["id"]} Message received: {body.decode()}')
                try:
                    order_products = json.loads(body)['order']
                    flag = True
                    for product in order_products:
                        if not (product in products.keys() and products[product].number >= order_products[product]):
                            flag = False

                    if flag:
                        for product in order_products:
                            if product in products.keys() and products[product].number >= order_products[product]:
                                products[product].number -= order_products[product]

                        outbox.append(ProductOutbox('product_found_queue', body, time.time()))
                        # ch.basic_publish(exchange='',
                        #                  routing_key='product_found_queue',
                        #                  body=body
                        #                  )
                        print(f'[product] {json.loads(body)["id"]} Message sent!')
                    else:
                        ch.queue_declare(queue='product_not_found_queue')
                        outbox.append(ProductOutbox('product_not_found_queue', body, time.time()))
                        # ch.basic_publish(exchange='',
                        #                  routing_key='product_not_found_queue',
                        #                  body=body
                        #                  )
                        print(f'[product] [not] {json.loads(body)["id"]} Message sent!')
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                except BaseException as e:
                    order_products = json.loads(body)['order']
                    for product in order_products:
                        if product in products.keys() and products[product].number >= order_products[product]:
                            products[product].number += order_products[product]
                    if outbox[-1].queue == 'product_found_queue' and outbox[-1].data == body:
                        outbox.pop()
                    if outbox[-1].queue == 'product_not_found_queue' and outbox[-1].data == body:
                        outbox.pop()
                    print('except:', e)

            def process_payment_not_done(ch, method, properties, body):
                print(f'[product] {json.loads(body)["id"]} Message received: {json.loads(body)}')
                try:
                    order_products = json.loads(body)['order']
                    for product in order_products:
                        products[product].number += order_products[product]
                    outbox.append(ProductOutbox('payment_not_done_order_queue', body, time.time()))
                    # ch.basic_publish(exchange='',
                    #                  routing_key='payment_not_done_order_queue',
                    #                  body=body
                    #                  )
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    print(f'[product] [not-payment] {json.loads(body)["id"]} Message sent!')
                except BaseException as e:
                    order_products = json.loads(body)['order']
                    for product in order_products:
                        if product in products.keys() and products[product].number >= order_products[product]:
                            products[product].number -= order_products[product]
                    if outbox[-1].queue == 'payment_not_done_order_queue' and outbox[-1].data == body:
                        outbox.pop()
                    print('except:', e)

            ch.basic_consume(
                queue='order_created_queue',
                on_message_callback=process_order_created,
            )

            ch.basic_consume(
                queue='payment_not_done_queue',
                on_message_callback=process_payment_not_done,
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

            #threading.Timer(30.0, pde).start()

            print('[product] Waiting...')
            ch.start_consuming()


if __name__ == '__main__':
    main()
