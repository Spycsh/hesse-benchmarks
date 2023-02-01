import json
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
APP_KAFKA_TOPICS = Arg(key="APP_KAFKA_TOPICS", default="storage-time filter-time "
                                                       "query-results", type=str)
# Dump the statistics to file every APP_DUMP_INTERVAL records
APP_DUMP_INTERVAL = Arg(key="APP_DUMP_INTERVAL", default="500", type=int)
# Dump the last statistics and END signal if this is specified
APP_DUMP_END = Arg(key="APP_DUMP_END", default="-1", type=int)

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
        # message attrs sent from Hesse egress: topic, partition, offset, key, value
        for idx, message in enumerate(self.consumer):
            if message.topic == "storage-time" or message.topic == "filter-time":
                end = env(APP_DUMP_END)
                interval = env(APP_DUMP_INTERVAL)
                # if APP_DUMP_END == -1, record every interval (Streaming)
                # otherwise, record when idx < APP_DUMP_END (Dataset)
                if (idx < end or end == -1) and idx % interval == 0:
                    with open(self.output_path + message.topic + '.txt', 'a') as f:
                        value_dict = json.loads(message.value.decode('utf-8'))
                        f.writelines("Index: {} Overall time: {} Average time: {}\n"
                            .format(message.key.decode('utf-8'), value_dict['overall_time'], value_dict['average_time']))
                elif idx == end:
                    with open(self.output_path + message.topic + '.txt', 'a') as f:
                        value_dict = json.loads(message.value.decode('utf-8'))
                        f.writelines("Index: {} Overall time: {} Average time: {}\n"
                            .format(message.key.decode('utf-8'), value_dict['overall_time'], value_dict['average_time']))
                        f.writelines("===========END OF RECORDING TOPIC {}===========".format(message.topic))
            elif message.topic == "query-results":
                qid_uid = str(message.key.decode('utf-8')).split(' ')
                value_dict = json.loads(message.value.decode('utf-8'), strict=False)
                with open(self.output_path + message.topic + '.txt', 'a') as f:
                    f.writelines("%s %s %s '%s'\n" % (qid_uid[0], qid_uid[1], value_dict['time'], value_dict['result_string']))


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

    if 'storage-time' in topics:
        t3 = Process(target=consume_data, args=('storage-time', env(APP_KAFKA_HOST), output_path))
        t3.start()
    if 'filter-time' in topics:
        t4 = Process(target=consume_data, args=('filter-time', env(APP_KAFKA_HOST), output_path))
        t4.start()
    if 'query-results' in topics:
        t5 = Process(target=consume_data, args=('query-results', env(APP_KAFKA_HOST), output_path))
        t5.start()


if __name__ == "__main__":
    main()
