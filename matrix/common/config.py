from dcplib.config import Config


class MatrixInfraConfig(Config):

    def __init__(self, *args, **kwargs):
        super().__init__(component_name='matrix', secret_name='infra', **kwargs)


class MatrixRedshiftConfig(Config):

    def __init__(self, *args, **kwargs):
        super().__init__(component_name='matrix', secret_name='database', **kwargs)
