class Mapper:
    def __init__(self):
        pass

    def run(self, request_id: str, bundle_fqid: str, format: str):
        print(f"received mapper input: {request_id}, {bundle_fqid}, {format}")
