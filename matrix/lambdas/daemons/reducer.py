from matrix.common.dynamo_handler import DynamoHandler


class Reducer:
    def __init__(self, request_id: str, format: str):
        print(f"Reducer created: {request_id}, {bundle_fqid}, {format}")
        self.request_id = request_id
        self.format = format

        self.dynamo_handler = DynamoHandler()

    def run(self):
        print("Running reducer")
