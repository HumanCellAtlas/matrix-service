import logging
import uuid

formatter = logging.Formatter("%(levelname)s:%(name)s:%(message)s")

stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)

logger = logging.getLogger("hca-matrix-service")
logger.addHandler(stream_handler)


def rand_uuid():
    return str(uuid.uuid4())
