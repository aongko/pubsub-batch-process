from google.cloud.pubsub_v1 import SubscriberClient
from google.cloud.pubsub_v1.types import FlowControl


class Subscriber:
    def __init__(self, project_id, subscription_name):
        self.project_id = project_id
        self.subscription_name = subscription_name

    @property
    def subscriber_client(self):
        """
        Returns:
            google.cloud.pubsub_v1.SubscriberClient
        """
        if not hasattr(self, "_subscriber_client"):
            setattr(self, "_subscriber_client", SubscriberClient())
        return getattr(self, "_subscriber_client")

    @property
    def subscription_path(self):
        if not hasattr(self, "_subscription_path"):
            setattr(self, "_subscription_path",
                    self.subscriber_client.subscription_path(
                        self.project_id, self.subscription_name
                    ))
        return getattr(self, "_subscription_path")

    def subscribe(self, callback=None, max_flow=100):
        """
        Subscribe to a subscription on Google Pub/Sub

        Args:
            callback (function|None): The callback function
            max_flow (int): The maximum outstanding message

        Returns:
            google.cloud.pubsub_v1.subscriber.policy.base.BasePolicy

        """
        # noinspection PyArgumentList
        flow_control = FlowControl(max_messages=max_flow)
        subscription = self.subscriber_client.subscribe(
            self.subscription_path,
            callback=callback,
            flow_control=flow_control
        )
        return subscription
