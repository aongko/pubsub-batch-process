import json
from datetime import datetime

import os

from pubsub_batch_process import utc, local_tz


class Processor:
    @staticmethod
    def process(list_of_items):
        """
        Process list of items by saving it into file

        Args:
            list_of_items (list[pubsub_batch_process.item.Item]):
        """
        if not os.path.isdir("data"):
            os.makedirs("data", exist_ok=True)
        now = utc.localize(datetime.utcnow()).astimezone(local_tz)
        filename = "data/{}.json".format(now.strftime("%Y%m%d-%H%M%S-%f"))

        with open(filename, "w") as f:
            json.dump(
                list(map(lambda x: x.to_str(), list_of_items)),
                f,
                indent=4
            )
