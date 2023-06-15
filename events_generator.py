import random
import time

from faker import Faker

from confluent_kafka import Producer

fake = Faker()

# List of random job titles
job_titles = [
    "Software Engineer",
    "Data Analyst",
    "Product Manager",
    "Marketing Specialist",
    "Graphic Designer",
    "Financial Analyst",
    "Human Resources Manager",
    "Sales Representative",
    "Customer Support Specialist",
    "Operations Manager"
]

user_names = [
    "John",
    "Emma",
    "Michael",
    "Sophia",
    "William",
    "Olivia",
    "James",
    "Ava",
    "Benjamin",
    "Isabella"
]


def delivery_callback(err, msg):
    if err:
        print('ERROR: Message failed delivery: {}'.format(err))
    else:
        print("Produced event to topic {topic}: key = {key:12} value = {value:12}".format(
            topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))


# Generate random job notifications for random users
def generate_job_notifications(num_notifications):
    notifications = {}

    for _ in range(num_notifications):
        user = random.choice(user_names)
        job_title = random.choice(job_titles)
        company_name = fake.company()
        notification = f"New job opportunity: {job_title} at {company_name} for {user}"
        notifications.setdefault(user, []).append(notification)

    return notifications


def read_ccloud_config(config_file):
    conf = {}
    with open(config_file) as fh:
        for line in fh:
            line = line.strip()
            if len(line) != 0 and line[0] != "#":
                parameter, value = line.strip().split('=', 1)
                conf[parameter] = value.strip()
    return conf


def produce_test_events(producer, topic, time_limit, function_to_call):
    start_time = time.time()

    while True:
        num_notifications = 10
        job_notifications = function_to_call(num_notifications)
        # Print the job notifications
        for user_id in job_notifications:
            # Send to Confluent Kafka
            values = job_notifications[user_id]
            for value in values:
                producer.produce(topic, key=user_id, value=value, callback=delivery_callback)
                print(f"{user_id}: {value}")

        producer.poll(10000)
        producer.flush()

        time.sleep(30)
        if time.time() - start_time >= time_limit:
            break


conf = read_ccloud_config('client.properties')

producer = Producer(conf)
produce_test_events(producer, topic="topic_test", time_limit=1200, function_to_call=generate_job_notifications)
