class MatrixException(Exception):
    def __init__(self, status: int, title: str, detail: str = None, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.status = status
        self.title = title
        self.detail = detail
