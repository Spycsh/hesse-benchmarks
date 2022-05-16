# hesse-benchmarks

![hesse-benchmarks](doc/hesse-benchmarks.png)

## Build

```shell
docker build . -t spycsh/hesse-benchmarks
docker run -d -v /app:/mnt -e 'APP_KAFKA_HOST=kafka:9092' -it spycsh/hesse-benchmarks
docker commit <container_id> spycsh/hesse-benchmarks
docker push spycsh/hesse-benchmarks
```

## How to use

```yaml
version: '2.1'
services:
  ... # other services
  hesse-benchmarks:
    image: spycsh/hesse-benchmarks:latest
    depends_on:
      - kafka
    links:
      - kafka:kafka
    environment:
      APP_KAFKA_HOST: kafka:9092
      APP_KAFKA_TOPICS: 'indexing-time producing-time storage-time filter-time query-results'
    volumes:
    - ./benchmarks:/app/results
```

## What is it

hesse-benchmarks is a Docker image, which is basically a multi-thread Kafka consumer that automatically

1) read the benchmarking results (time) from hesse egressed Kafka topics.
2) store and plot benchmarking results in the container folder volume mapping to the host benchmarks folder of [hesse](https://github.com/Spycsh/hesse)

With the plotted benchmarking results, we are expected to

1) find the sweet spot of indexing bucket size
2) persist and plot the time for Kafka producing in hesse
3) persist and plot the time for edge storage in RocksDB
4) persist and plot the time for filtering (retrieving) state of a query
5) persist and plot the duration of the queries