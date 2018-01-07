import json


class Item:
    def __init__(self, item_id):
        self.item_id = item_id

    @classmethod
    def from_str(cls, s):
        item = json.loads(s)
        if "item_id" not in item:
            raise ValueError("no item_id")
        return cls(item["item_id"])

    def to_str(self):
        return json.dumps({"item_id": self.item_id})

    def __repr__(self):
        return "<Item: {}>".format(self.item_id)
