import json
import logging
import random
import threading

import time

from google.api_core.exceptions import GoogleAPIError

logger = logging.getLogger(__name__)


class PublishingThread(threading.Thread):
    def __init__(self, publisher):
        """
        Args:
            publisher (pubsub_batch_process.pubsub.publisher.Publisher):
        """
        self.publisher = publisher

        super(PublishingThread, self).__init__(
            target=self.run,
            name="publishing-thread"
        )
        self.daemon = True

        self._exiting = threading.Event()

    @property
    def exiting(self):
        return self._exiting.is_set()

    def run(self):
        while True:
            if self.exiting:
                logger.info("Exiting")
                break

            item_id = random.randint(1, 1000000)
            self.publish(json.dumps({"item_id": item_id}))

            # sleep 0.1 ~ 1.0 second
            time.sleep(random.randint(1, 10) / 10)

    def publish(self, message_str):
        try:
            message_id = self.publisher.publish(message_str)
            logger.debug(
                "Done publishing with message_id: {}".format(message_id)
            )
        except GoogleAPIError:
            logger.exception(
                "Error publishing message: {}".format(message_str)
            )

    def start(self):
        self._exiting.clear()
        super(PublishingThread, self).start()

    def stop(self):
        self._exiting.set()
