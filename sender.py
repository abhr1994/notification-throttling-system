import threading

from confluent_kafka import Consumer, admin, KafkaException

from utils import reset_offset, read_ccloud_config

reset = False



conf = read_ccloud_config('client.properties')
conf['group.id'] = "python-sender"
conf['auto.offset.reset'] = 'earliest'


def consume_messages(consumer):
    try:
        consumer.subscribe(['target_topic'], on_assign=reset_offset)
        thread_id = threading.get_ident()  # Get the thread ID
        thread_name = threading.current_thread().name  # Get the thread name
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                print("Waiting...")
            elif msg.error():
                print("ERROR: %s".format(msg.error()))
            else:
                # Extract the (optional) key and value, and print.
                key = msg.key().decode('utf-8')
                value = msg.value().decode('utf-8')
                # print("{thread_id} {thread_name} Consumed event from topic {topic}: key = {key} value = {value}".format(
                #     thread_id=thread_id, thread_name=thread_name, topic=msg.topic(), key=key, value=value))
                print(f"{thread_name} Sending notification to user: {key} with message: {value}")
    except Exception as e:
        print(str(e))
    finally:
        # Leave group and commit final offsets
        consumer.close()


# Find the number of partitions
num_partitions = 1  # Default value incase of failure
admin_client = admin.AdminClient(conf)
topic = 'target_topic'
try:
    metadata = admin_client.list_topics(topic=topic).topics[topic]

    if metadata.error is not None:
        raise KafkaException(metadata.error)

    # Get the number of partitions
    num_partitions = len(metadata.partitions)
    print(f"The topic '{topic}' has {num_partitions} partitions.")

except KafkaException as e:
    print(f"Failed to retrieve metadata for topic '{topic}': {e}")

confluent_consumer_objs = []
try:
    for i in range(num_partitions):
        confluent_consumer_objs.append(Consumer(conf))

    threads = []
    for cf_obj in confluent_consumer_objs:
        thread = threading.Thread(target=consume_messages, args=(cf_obj,))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()
except KeyboardInterrupt:
    for cobj in confluent_consumer_objs:
        cobj.close()
