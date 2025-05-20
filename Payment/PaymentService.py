import json
import random
import time
import threading
from pika import BlockingConnection, ConnectionParameters
from Payment import Payment
from Outbox import PaymentOutbox

connection_parameters = ConnectionParameters(
    host='localhost',
    port=5672,
)

payments = []

outbox = []


def main():
    t = time.time()
    with BlockingConnection(connection_parameters) as conn:
        with conn.channel() as ch:
            def process_product_found_queue(ch, method, properties, body):
                print(f'[payment] {json.loads(body)["id"]} Message received: {json.loads(body)}')

                if random.random() < 0.5:
                    ch.queue_declare(queue='payment_done_queue')
                    try:
                        outbox.append(PaymentOutbox('payment_done_queue', body, time.time()))
                        # ch.basic_publish(exchange='',
                        #                  routing_key='payment_done_queue',
                        #                  body=body
                        #                  )
                        print(f'[payment] {json.loads(body)["id"]} Message sent!')
                        # Подтверждение обработки сообщения
                        ch.basic_ack(delivery_tag=method.delivery_tag)
                    except BaseException as e:
                        if outbox[-1].queue == 'payment_done_queue' and outbox[-1].data == body:
                            outbox.pop()
                        print('except:', e)
                else:
                    ch.queue_declare(queue='payment_not_done_queue')
                    try:
                        outbox.append(PaymentOutbox('payment_not_done_queue', body, time.time()))
                        # ch.basic_publish(exchange='',
                        #                  routing_key='payment_not_done_queue',
                        #                  body=body
                        #                  )
                        print(f'[payment] [not] {json.loads(body)["id"]} Message sent!')
                        # Подтверждение обработки сообщения
                        ch.basic_ack(delivery_tag=method.delivery_tag)
                    except BaseException as e:
                        if outbox[-1].queue == 'payment_not_done_queue' and outbox[-1].data == body:
                            outbox.pop()
                        print('except:', e)

            ch.queue_declare(queue='product_found_queue')
            ch.basic_consume(
                queue='product_found_queue',
                on_message_callback=process_product_found_queue,
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

            print('[payment] Waiting...')
            ch.start_consuming()


if __name__ == '__main__':
    main()
