import signal
import sys
import time
import os
from collections import namedtuple
from time import gmtime, strftime

from multiprocessing import Process

from kafka.errors import NoBrokersAvailable
from kafka import KafkaConsumer

Arg = namedtuple('Arg', ['key', 'default', 'type'])

APP_KAFKA_HOST = Arg(key="APP_KAFKA_HOST", default="kafka-broker:9092", type=str)
APP_KAFKA_TOPICS = Arg(key="APP_KAFKA_TOPICS", default="indexing-time producing-time storage-time filter-time "
                                                       "query-results configs", type=str)


def env(arg: Arg):
    val = os.environ.get(arg.key, arg.default)
    return arg.type(val)


def consume_data(topic, broker, output_path):
    while True:
        try:
            consumer = KConsumer(topic=topic, broker=broker, output_path=output_path)
            consumer.consume()
            print("Done producing, good bye!", flush=True)
            return
        except SystemExit:
            print("Good bye!", flush=True)
            return
        except NoBrokersAvailable:
            print("No brokers available... retrying in 2 seconds.")
            time.sleep(2)
            continue
        except Exception as e:
            print(e)
            return


class KConsumer(object):
    def __init__(self, topic, broker, output_path):
        print("init consumer with topic:%s, bootstrap_servers=[%s]..." % (topic, broker))
        self.consumer = KafkaConsumer(topic, bootstrap_servers=[broker], group_id=topic + '-group')
        self.output_path = output_path

    def consume(self):
        for message in self.consumer:
            with open(self.output_path + message.topic + '.txt', 'a') as f:
                f.write("%s:%d:%d: key=%s value=%s\n" % (message.topic, message.partition,
                                                         message.offset, message.key,
                                                         message.value))
            # TODO plot graph, normalized JSON messages
            # print("{} consume, key: {}, value: {}".format(
            #     json.loads(message.key.decode()),
            #     json.loads(message.value.decode())
            # ))


def handler(number, frame):
    sys.exit(0)


def main():
    # setup an exit signal handler
    signal.signal(signal.SIGTERM, handler)
    signal.signal(signal.SIGINT, handler)

    topics = env(APP_KAFKA_TOPICS).split(' ')

    print(topics)

    output_path = '/app/results/' + strftime("results_%Y_%m_%d_%H_%M_%S/", gmtime())
    os.mkdir(output_path)

    if 'indexing-time' in topics:
        t1 = Process(target=consume_data, args=('indexing-time', env(APP_KAFKA_HOST), output_path,))
        t1.start()
    if 'producing-time' in topics:
        t2 = Process(target=consume_data, args=('producing-time', env(APP_KAFKA_HOST), output_path))
        t2.start()
    if 'storage-time' in topics:
        t3 = Process(target=consume_data, args=('storage-time', env(APP_KAFKA_HOST), output_path))
        t3.start()
    if 'filter-time' in topics:
        t4 = Process(target=consume_data, args=('filter-time', env(APP_KAFKA_HOST), output_path))
        t4.start()
    if 'query-results' in topics:
        t5 = Process(target=consume_data, args=('query-results', env(APP_KAFKA_HOST), output_path))
        t5.start()
    if 'configs' in topics:
        t6 = Process(target=consume_data, args=('configs', env(APP_KAFKA_HOST), output_path))
        t6.start()


if __name__ == "__main__":
    main()
