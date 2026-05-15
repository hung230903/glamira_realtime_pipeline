from confluent_kafka import Consumer, Producer, KafkaException, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic

from config.base import server_consumer_config, local_producer_config, SERVER_TOPIC, SERVER_BOOTSTRAP_SERVERS, \
    LOCAL_TOPIC, LOCAL_BOOTSTRAP_SERVERS
from config.logger import setup_logger

logger = setup_logger(name="KafkaProducer", log_folder="kafka", log_file="producer.log")

# Commit offsets every N messages instead of every message
_COMMIT_INTERVAL = 100


def delivery_report(err, produced_msg, msg_count_holder):
    """Callback on produce success/failure — DO NOT commit offset here."""
    if err:
        logger.error(f"Delivery failed: {err}")
    else:
        msg_count_holder[0] += 1
        count = msg_count_holder[0]
        logger.info(
            f"[#{count}] Forwarded → {produced_msg.topic()} "
            f"[partition={produced_msg.partition()}, offset={produced_msg.offset()}]"
        )


def run_producer():
    # Add session.timeout.ms and max.poll.interval.ms to consumer config
    consumer_cfg = server_consumer_config
    consumer_cfg["session.timeout.ms"] = 45000  # 45s (default 10s)
    consumer_cfg["max.poll.interval.ms"] = 300000  # 5 mins
    consumer_cfg["enable.auto.commit"] = False

    try:
        consumer = Consumer(consumer_cfg)
        # Add short timeout for producer to avoid hanging consumer loop too long
        producer_cfg = local_producer_config
        producer_cfg["message.timeout.ms"] = 30000  # 30s
        producer = Producer(producer_cfg)
        
        consumer.subscribe([SERVER_TOPIC])
        logger.info(
            f"Kafka producer started: [{SERVER_TOPIC}] "
            f"({SERVER_BOOTSTRAP_SERVERS}) → "
            f"[{LOCAL_TOPIC}] ({LOCAL_BOOTSTRAP_SERVERS})"
        )
    except Exception as e:
        import sys
        logger.error(f"Failed to initialize Kafka clients or subscribe: {e}")
        sys.exit(1)

    msg_count_holder = [0]
    pending_commit = 0  # Number of uncommitted messages

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                # Still commit if there is pending (during idle)
                if pending_commit > 0:
                    try:
                        consumer.commit(asynchronous=False)
                        pending_commit = 0
                    except KafkaException as e:
                        logger.error(f"Offset commit failed: {e}")
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                logger.error(f"Consumer error: {msg.error()}")
                continue

            try:
                producer.produce(
                    LOCAL_TOPIC,
                    value=msg.value(),
                    on_delivery=lambda err, p_msg: delivery_report(
                        err, p_msg, msg_count_holder
                    ),
                )
                pending_commit += 1
            except BufferError:
                logger.warning("Producer queue is full, flushing and retrying...")
                producer.poll(1)
                continue
            except Exception as e:
                logger.error(f"Produce error: {e}")

            # Trigger delivery callbacks
            producer.poll(0)

            # Commit offset every _COMMIT_INTERVAL messages
            if pending_commit >= _COMMIT_INTERVAL:
                producer.flush()  # Ensure all messages are sent
                try:
                    consumer.commit(asynchronous=False)
                    pending_commit = 0
                except KafkaException as e:
                    logger.error(f"Offset commit failed: {e}")

    except KeyboardInterrupt:
        logger.info("Shutting down producer...")
    finally:
        logger.info(f"Total messages forwarded: {msg_count_holder[0]}")
        logger.info("Flushing producer and closing consumer...")
        producer.flush(timeout=10)
        # Final commit before closing
        try:
            consumer.commit(asynchronous=False)
        except KafkaException:
            pass
        consumer.close()
        logger.info("Producer stopped.")


if __name__ == "__main__":
    run_producer()
