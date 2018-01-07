import logging
import signal
import threading

import os

import time
from google.api_core.exceptions import AlreadyExists

from pubsub_batch_process.helper.publish import PublishingThread
from pubsub_batch_process.helper.processor import ProcessorThread
from pubsub_batch_process.helper.subscribe import SubscriptionThread
from pubsub_batch_process.processor import Processor
from pubsub_batch_process.pubsub.publisher import Publisher
from pubsub_batch_process.pubsub.subscriber import Subscriber

fmt = "%(levelname)s:%(name)s:%(processName)s:%(threadName)s:%(message)s"
logging.basicConfig(level="DEBUG", format=fmt)
logging.getLogger(
    "google.cloud.pubsub_v1.subscriber.policy.base"
).setLevel("WARNING")
logging.getLogger(
    "google.cloud.pubsub_v1.subscriber.policy.thread"
).setLevel("WARNING")
logging.getLogger(
    "google.cloud.pubsub_v1.subscriber._consumer"
).setLevel("WARNING")
logging.getLogger(
    "google.cloud.pubsub_v1.publisher.batch.thread"
).setLevel("WARNING")

os.environ["PUBSUB_EMULATOR_HOST"] = "localhost:8538"
PROJECT_ID = "test"
TOPIC_NAME = "topic-{}".format(int(time.time()))
SUBSCRIPTION_NAME = "{}.subscription1".format(TOPIC_NAME)

logger = logging.getLogger(__name__)


def ensure_topic_and_subscription():
    from google.cloud.pubsub_v1 import PublisherClient, SubscriberClient
    publisher_client = PublisherClient()
    try:
        publisher_client.create_topic(
            publisher_client.topic_path(PROJECT_ID, TOPIC_NAME)
        )
    except AlreadyExists:
        pass

    subscriber_client = SubscriberClient()
    try:
        subscriber_client.create_subscription(
            subscriber_client.subscription_path(PROJECT_ID, SUBSCRIPTION_NAME),
            publisher_client.topic_path(PROJECT_ID, TOPIC_NAME)
        )
    except AlreadyExists:
        pass


class ItemsQueue:
    items = []

    def __init__(self):
        self.item_lock = threading.Lock()

    def put(self, item):
        with self.item_lock:
            self.items.append(item)

    def get_all(self):
        with self.item_lock:
            all_items = self.items
            self.items = []
        return all_items


def main():
    items_queue = ItemsQueue()

    publishing_thread = PublishingThread(Publisher(PROJECT_ID, TOPIC_NAME))

    subscription_thread = SubscriptionThread(
        Subscriber(PROJECT_ID, SUBSCRIPTION_NAME),
        items_queue
    )

    processor = Processor()
    processor_thread = ProcessorThread(processor, items_queue)

    publishing_thread.start()
    subscription_thread.start()
    processor_thread.start()

    def signal_handler(signum, _):
        if signum == signal.SIGTERM:
            logger.info("Got SIGTERM. closing subscription")
        elif signum == signal.SIGINT:
            logger.info("Got SIGINT. closing subscription")

        logging.info("Stopping publishing thread")
        publishing_thread.stop()
        logging.info("Stopping subscription thread")
        subscription_thread.stop()
        logging.info("Stopping saver thread")
        processor_thread.stop()

        logger.info("Exiting...")
        import sys
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    publishing_thread.join()
    subscription_thread.join()
    processor_thread.join()


if __name__ == "__main__":
    ensure_topic_and_subscription()
    main()
