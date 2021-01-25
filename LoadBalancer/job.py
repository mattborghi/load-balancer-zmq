import uuid

class Job(object):
    """
    Define an object to be sent to the workers to be processed.

    Job sent are in the form of:

    {
        "id": uuid4,
        "name: str,
        "number1": number,
        "number2": number,
    }

    """

    def __init__(self, payload: dict, name: str, id=None) -> dict:
        self.id = id if id else uuid.uuid4().hex[:4]
        self.name = name
        self.payload = payload
        self.result = None

    def get_job(self):
        return {
            "id": self.id,
            "name": self.name,
            "number1": self.payload["number1"],
            "number2": self.payload["number2"],
            "result": self.result
        }

    @staticmethod
    def get_result(data: dict, result) -> dict:
        data["result"] = result
        return data
