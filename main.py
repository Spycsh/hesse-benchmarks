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

import matplotlib.pyplot as plt

Arg = namedtuple('Arg', ['key', 'default', 'type'])

APP_KAFKA_HOST = Arg(key="APP_KAFKA_HOST", default="kafka-broker:9092", type=str)
APP_KAFKA_TOPICS = Arg(key="APP_KAFKA_TOPICS", default="indexing-time storage-time filter-time "
                                                       "query-results", type=str)
APP_PLOT_FLAG = Arg(key="APP_PLOT_FLAG", default="false", type=lambda s: s.lower() == "true")   # whether to plot results
APP_DELEY_PLOT = Arg(key="APP_DELEY_PLOT", default="20", type=int)  # how long to wait after one file is created and before plotting

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
                # f.write("%s:%d:%d: key=%s value=%s\n" % (message.topic, message.partition,
                #                                          message.offset, message.key,
                #                                          message.value))
                if message.topic == "storage-time" or message.topic == "filter-time":
                    value_dict = json.loads(message.value.decode('utf-8'))
                    f.write("%s %s %s %s\n" % (message.key.decode('utf-8'),
                                               value_dict['time'],
                                               value_dict['overall_time'],
                                               value_dict['average_time']))
                elif message.topic == "query-results":
                    qid_uid = str(message.key.decode('utf-8')).split(' ')
                    value_dict = json.loads(message.value.decode('utf-8'), strict=False)
                    f.write("%s %s %s '%s'\n" % (qid_uid[0], qid_uid[1], value_dict['time'], value_dict['result_string']))
                else:
                    f.write("%s\n" % message.value.decode('utf-8'))


def handler(number, frame):
    sys.exit(0)


def plot_result(output_path, topics, delay):
    storage_time_file_path = output_path + "storage-time.txt"
    filter_time_file_path = output_path + "filter-time.txt"
    query_results_file_path = output_path + "query-results.txt"

    while ('storage-time' in topics):
        if os.path.isfile(storage_time_file_path):
            time.sleep(delay)
            with open(storage_time_file_path, 'r') as f:
                lines = f.readlines()
                x = [int(line.split()[0]) for line in lines]
                y = [int(line.split()[1]) for line in lines]
                print(x)
                print(y)
                plt.plot(x, y)
                plt.xlabel('storage operation index')
                plt.ylabel('storage time')
                plt.title('storage time for each edge')
                plt.savefig(output_path + 'storage-time.png', dpi=300, bbox_inches='tight')
                plt.cla()
            break

    while ('filter-time' in topics):
        if os.path.isfile(filter_time_file_path):
            time.sleep(delay)
            with open(filter_time_file_path, 'r') as f:
                lines = f.readlines()
                x = [int(line.split()[0]) for line in lines]
                y = [int(line.split()[1]) for line in lines]
                print(x)
                print(y)
                plt.plot(x, y)
                plt.xlabel('filter operation index')
                plt.ylabel('filter time')
                plt.title('filter time for each edge')
                plt.savefig(output_path + 'filter-time.png', dpi=300, bbox_inches='tight')
                plt.cla()
            break

    while ('query-results' in topics):
        if os.path.isfile(query_results_file_path):
            time.sleep(delay)
            with open(query_results_file_path, 'r') as f:
                lines = f.readlines()
                # query results sort
                x = sorted([int(line.split()[0]) for line in lines])
                y = sorted([int(line.split()[2]) for line in lines])
                print(x)
                print(y)
                plt.plot(x, y)
                plt.xlabel('query index')
                plt.ylabel('query time')
                plt.title('handling time for each query')
                plt.savefig(output_path + 'query-results-time.png', dpi=300, bbox_inches='tight')
                plt.cla()
        break


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
    if 'storage-time' in topics:
        t3 = Process(target=consume_data, args=('storage-time', env(APP_KAFKA_HOST), output_path))
        t3.start()
    if 'filter-time' in topics:
        t4 = Process(target=consume_data, args=('filter-time', env(APP_KAFKA_HOST), output_path))
        t4.start()
    if 'query-results' in topics:
        t5 = Process(target=consume_data, args=('query-results', env(APP_KAFKA_HOST), output_path))
        t5.start()

    if env(APP_PLOT_FLAG):
        plot_result(output_path, topics, env(APP_DELEY_PLOT))


if __name__ == "__main__":
    main()
