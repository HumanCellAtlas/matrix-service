import traceback

from multiprocessing import Process, Pipe
from chalicelib.config import hca_client


class HcaDownloadWorker(Process):
    """
    Worker takes charge of downloading entities from the DSS bundle.
    """
    def __init__(self, **kwargs):
        super().__init__()
        self._kwargs = kwargs
        self._pconn, self._cconn = Pipe(duplex=False)
        self._exception = None

    def run(self):
        try:
            hca_client.download(**self._kwargs)
            self._cconn.send(None)
        except:
            self._cconn.send(Exception(traceback.format_exc()))
        finally:
            self._cconn.close()

    @property
    def exception(self):
        if self._pconn.poll():
            self._exception = self._pconn.recv()
        return self._exception

    def __del__(self):
        self._pconn.close()
        self._cconn.close()
