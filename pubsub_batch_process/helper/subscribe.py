import logging
import threading

import time

from pubsub_batch_process.item import Item

logger = logging.getLogger(__name__)


class SubscriptionThread(threading.Thread):
    def __init__(self, subscriber, items_queue):
        self.subscriber = subscriber
        self.subscription = None

        self.items_queue = items_queue

        self._exiting = threading.Event()

        super(SubscriptionThread, self).__init__(
            target=self.run,
            name="subscription-thread",
            args=()
        )
        self.daemon = True

    def callback(self, message):
        try:
            item = Item.from_str(message.data.decode("utf-8"))
        except ValueError as e:
            logger.warning("not a valid item: {}".format(str(e)))
            message.ack()
            return

        logger.debug("Putting item {} into the queue".format(item))
        self.items_queue.put(item)
        message.ack()

    def run(self):
        logger.info("Running...")
        self.subscription = self.subscriber.subscribe(self.callback)
        while True:
            if self._exiting.is_set():
                logger.info("Got exit signal. Closing subscription.")
                self.subscription.close()
                logger.info("Subscription closed.")
                break
            logger.debug("Waiting...")
            time.sleep(30)
            logger.debug("Closing subscription")
            self.subscription.close()
            logger.debug("Opening subscription")
            self.subscription.open(self.callback)

    def start(self):
        self._exiting.clear()
        super(SubscriptionThread, self).start()

    def stop(self):
        self._exiting.set()
