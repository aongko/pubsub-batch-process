import logging
import threading

import schedule
import time

logger = logging.getLogger(__name__)


class ProcessorThread(threading.Thread):
    def __init__(self, processor, items_queue):
        """

        Args:
            processor (pubsub_batch_process.processor.Processor):
            items_queue:
        """
        self.processor = processor
        self.items_queue = items_queue

        super(ProcessorThread, self).__init__(
            target=self.run,
            name="saver-thread"
        )
        self.daemon = True
        self._exiting = threading.Event()

    @property
    def exiting(self):
        return self._exiting.is_set()

    def run(self):
        logger.info("Scheduling process")
        schedule.every(3).seconds.do(self.process)
        while True:
            if self.exiting:
                logger.info("Exiting...")
                break
            else:
                logger.debug("Run pending schedule")
                schedule.run_pending()
                time.sleep(1)

    def process(self):
        logger.info("Getting all items")
        list_of_items = self.items_queue.get_all()
        logger.info("Process list of items")
        self.processor.process(list_of_items)
        logger.info("Done processing list of items")

    def start(self):
        self._exiting.clear()
        super(ProcessorThread, self).start()

    def stop(self):
        self._exiting.set()
