from google.cloud.pubsub_v1 import PublisherClient


class Publisher:
    def __init__(self, project_id, topic_name):
        self.project_id = project_id
        self.topic_name = topic_name

    @property
    def publisher_client(self):
        if not hasattr(self, "_publisher_client"):
            setattr(self, "_publisher_client", PublisherClient())
        return getattr(self, "_publisher_client")

    @property
    def topic_path(self):
        return self.publisher_client.topic_path(self.project_id,
                                                self.topic_name)

    def publish(self, message):
        """
        Publish a message to a topic on Google Pub/Sub

        Args:
            message (str): The message

        Returns:
            str: message_id

        Raises:
            google.api_core.exceptions.GoogleAPIError

        """
        if not isinstance(message, str):
            raise ValueError("message must be a `str`")

        f = self.publisher_client.publish(
            self.topic_path, message.encode("utf-8")
        )
        message_id = f.result()
        return message_id
