import threading
import time
import traceback

import redis
from confluent_kafka import Consumer, Producer

from utils import delivery_callback, reset_offset, read_ccloud_config

r = redis.Redis(host='localhost', port=6379, decode_responses=True)

THROTTLE_NOTIFICATION_COUNT = 5

reset = False

lock = threading.Lock()

conf = read_ccloud_config('client.properties')
producer = Producer(conf)


def produce_data_to_topic(topic, key, msg):
    producer.produce(topic, key=key, value=msg, callback=delivery_callback)
    producer.poll(10000)
    producer.flush()


def update_redis(key, value):
    global lock

    # Acquire the lock
    lock.acquire()

    try:
        # Update the global dictionary
        r.set(key, value)
    finally:
        # Release the lock
        lock.release()


def throttler_writes(consumer, topic_src, topic_bkp, tgt_topic, duration=600):
    msg_wait_count = 0
    start_time = time.time()
    thread_name = threading.current_thread().name
    print(f"{thread_name}: Polling the Topic: {topic_src}. Backup Topic is {topic_bkp}.")
    consumer.subscribe([topic_src], on_assign=reset_offset)
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:

                msg_wait_count = msg_wait_count + 1
                print(f"{thread_name}: Waiting...Polling for new messages in {topic_src}... Polling Wait Count: {msg_wait_count}")
                if msg_wait_count >= 20:
                    break
            elif msg.error():
                print("ERROR: %s".format(msg.error()))
            else:
                # Extract the (optional) key and value, and print.
                msg_wait_count = 0
                key = msg.key().decode('utf-8')
                value = msg.value().decode('utf-8')
                print("\n{thread_name} : Consumed event from topic {topic}: key = {key:12} value = {value:12}".format(
                    thread_name=thread_name, topic=msg.topic(), key=key, value=value))

                user_msg_count = r.get(key)
                print(f"{thread_name}: User :{key} Current_Notification_Count: {user_msg_count}")
                if user_msg_count is None or int(user_msg_count) < THROTTLE_NOTIFICATION_COUNT:
                    print(f"{thread_name}: Writing the message to {tgt_topic}")
                    produce_data_to_topic(tgt_topic, key, value)
                    if user_msg_count is None:
                        update_redis(key, 1)
                    else:
                        update_redis(key, int(user_msg_count) + 1)
                else:
                    print(f"{thread_name}: Writing the message to the backup topic: {topic_bkp}")
                    produce_data_to_topic(topic_bkp, key, value)
            if time.time() - start_time >= duration:
                break

    except Exception as e:
        print(thread_name, str(e))
        traceback.print_exc()
    finally:
        # Leave group and commit final offsets
        consumer.close()


toggle_duration = 600  # 10 minutes

# Cleanup all the keys
keys = r.keys("*")
if keys:
    r.delete(*keys)

conf['auto.offset.reset'] = 'earliest'
conf['group.id'] = "python-throttler-1"
num_partitions = 1  # Default value incase of failure


def throttle_controller(topic_src, topic_bkp, tgt_topic, duration=600):
    start_time = time.time()
    confluent_consumer_objs = []
    try:
        for i in range(num_partitions):
            confluent_consumer_objs.append(Consumer(conf))

        threads = []
        for cf_obj in confluent_consumer_objs:
            thread = threading.Thread(target=throttler_writes, args=(cf_obj, topic_src, topic_bkp, tgt_topic, duration))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()
    except KeyboardInterrupt:
        for cobj in confluent_consumer_objs:
            cobj.close()
    finally:
        elapsed_time = time.time() - start_time
        if elapsed_time > duration:
            elapsed_time = 600
        return elapsed_time


# Main program loop
while True:
    # Cleanup all the keys
    keys = r.keys("*")
    if keys:
        r.delete(*keys)

    time_elapsed = throttle_controller("p1", "p1", "target_topic")
    if toggle_duration - time_elapsed > 60:
        # Now Poll the main topic
        print(f"\nPolling the main topic: p0 for {int(toggle_duration - time_elapsed)} seconds...")
        time_elapsed = throttle_controller("p0", "p1", "target_topic", duration=int(toggle_duration - time_elapsed))

    # Wait for the specified duration
    print(f"\nSleeping for {toggle_duration} seconds before toggling...")
    time.sleep(toggle_duration)
