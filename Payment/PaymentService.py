import json
import random
import time
from pika import BlockingConnection, ConnectionParameters
from Payment import Payment


connection_parameters = ConnectionParameters(
    host='localhost',
    port=5672,
)

payments = []


def main():
    t = time.time()
    with BlockingConnection(connection_parameters) as conn:
        with conn.channel() as ch:
            def process_product_found_queue(ch, method, properties, body):
                print(f'[payment] {json.loads(body)["id"]} Message received: {json.loads(body)}')

                if random.random() < 0.1:
                    ch.queue_declare(queue='payment_done_queue')
                    ch.basic_publish(exchange='',
                                     routing_key='payment_done_queue',
                                     body=body
                                     )
                    print(f'[payment] {json.loads(body)["id"]} Message sent!')
                else:
                    ch.queue_declare(queue='payment_not_done_queue')
                    ch.basic_publish(exchange='',
                                     routing_key='payment_not_done_queue',
                                     body=body
                                     )
                    print(f'[payment] [not] {json.loads(body)["id"]} Message sent!')
                # Подтверждение обработки сообщения
                ch.basic_ack(delivery_tag=method.delivery_tag)


            ch.queue_declare(queue='product_found_queue')
            ch.basic_consume(
                queue='product_found_queue',
                on_message_callback=process_product_found_queue,
            )
            while ((time.time() - t) < 30):
                t = time.time()
                conn.process_data_events(time_limit=None)
            print('[payment] Waiting...')
            ch.start_consuming()


if __name__ == '__main__':
    main()
