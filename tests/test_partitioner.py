"""The ingestor must partition keys like Java clients (murmur2), not librdkafka's
CRC32 default, so per-key ordering holds across producers (#5)."""

from confluent_kafka import Producer

from marketlake.ingest.coinbase import producer_conf


def java_murmur2(data: bytes) -> int:
    """org.apache.kafka.common.utils.Utils.murmur2, as a signed 32-bit int."""
    m, r = 0x5BD1E995, 24
    length = len(data)
    h = (0x9747B28C ^ length) & 0xFFFFFFFF
    for i in range(length // 4):
        k = int.from_bytes(data[4 * i:4 * i + 4], "little")
        k = (k * m) & 0xFFFFFFFF
        k ^= k >> r
        k = (k * m) & 0xFFFFFFFF
        h = (h * m) & 0xFFFFFFFF
        h ^= k
    tail = data[length & ~3:]
    if len(tail) >= 3:
        h ^= tail[2] << 16
    if len(tail) >= 2:
        h ^= tail[1] << 8
    if len(tail) >= 1:
        h ^= tail[0]
        h = (h * m) & 0xFFFFFFFF
    h ^= h >> 13
    h = (h * m) & 0xFFFFFFFF
    h ^= h >> 15
    return h


def java_partition(key: bytes, partitions: int) -> int:
    # Java's DefaultPartitioner: toPositive(murmur2(key)) % numPartitions.
    return (java_murmur2(key) & 0x7FFFFFFF) % partitions


def test_keys_land_on_the_same_partition_as_java_clients():
    # librdkafka's built-in mock cluster: no broker needed, topics auto-create.
    producer = Producer({**producer_conf("unused:9092"), "test.mock.num.brokers": 1})
    keys = [f"coinbase:{s}" for s in ("BTC-USD", "ETH-USD", "SOL-USD", "DOGE-USD",
                                      "AVAX-USD", "LINK-USD", "XRP-USD", "ADA-USD")]
    got = {}
    for key in keys:
        producer.produce("trades.raw", key=key, value=b"x",
                         on_delivery=lambda err, msg: got.__setitem__(msg.key().decode(), msg.partition()))
    assert producer.flush(10) == 0
    partitions = len(producer.list_topics("trades.raw").topics["trades.raw"].partitions)
    assert got == {k: java_partition(k.encode(), partitions) for k in keys}
