# hesse-benchmarks

## What is it

hesse-benchmarks is a Docker image, which is basically a multi-thread Kafka consumer that automatically

1) read the benchmarking results (time) from hesse egressed Kafka topics.
2) store benchmarking results in the container folder volume mapping to the host benchmarks folder of [hesse](https://github.com/Spycsh/hesse)

## Notice

Hesse exposes three topics: `storage-time`,`filter-time` and `storage-time`.

`storage-time` and `filter-time` only record the **in-memory** storage and lookup on Statefun side, 
and **NOT** cover the time that the states are sent to Flink side and read from/written to RocksDB.

`query-results` records all the query results and the time of the query computation.

`storage-time` can also be used to track whether your graph is fully ingested or not by set APP_DUMP_END to the length of your dataset. `storage-time` and `filter-time` statistics will not be dumped after APP_DUMP_END of messages. If you want to record an unboundless stream, please just set APP_DUMP_END to -1 or just delete that line in your docker-compose.yaml.

APP_DUMP_INTERVAL is an integer, after that integer number of messages we record the overall statistics once and dump to a file. Please remember to set the APP_DUMP_INTERVAL big enough to avoid too many access to file system.

## Build

> Notice: use tag v2.0 for latest updates

```shell
docker build . -t spycsh/hesse-benchmarks
docker run -d -v /app:/mnt -e 'APP_KAFKA_HOST=kafka:9092' -it spycsh/hesse-benchmarks
docker commit <container_id> spycsh/hesse-benchmarks:v2.0
docker push spycsh/hesse-benchmarks:v2.0
```

## How to use

```yaml
version: '2.1'
services:
  ... # other services
  hesse-benchmarks:
    image: spycsh/hesse-benchmarks:v2.0
    depends_on:
      - kafka
    links:
      - kafka:kafka
    environment:
      APP_KAFKA_HOST: kafka:9092
      APP_KAFKA_TOPICS: 'storage-time filter-time query-results'
      APP_DUMP_INTERVAL: 5000
      APP_DUMP_END: 10000
    volumes:
    - ./benchmarks:/app/results
```