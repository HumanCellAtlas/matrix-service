#!/usr/bin/env python
"""
This script creates a readonly user in the redshift db
"""

from matrix.common.aws.redshift_handler import RedshiftHandler
from matrix.common.config import MatrixRedshiftConfig


def retrieve_redshift_config():  # pragma: no cover
    return MatrixRedshiftConfig()


def handler():
    matrix_config = retrieve_redshift_config()
    redshift_handler = RedshiftHandler()
    readonly_username = matrix_config.readonly_username
    readonly_password = matrix_config.readonly_password
    drop_user_query = f"DROP USER IF EXISTS {readonly_username};"
    add_user_query = f"CREATE USER {readonly_username} WITH PASSWORD '{readonly_password}';"
    grant_public_query = f"grant select on all tables in schema public to {readonly_username};"
    grant_information_schema_query = f"grant select on all tables in schema information_schema to {readonly_username};"
    try:
        redshift_handler.transaction([drop_user_query, add_user_query])
    except Exception as e:
        print(e)
    redshift_handler.transaction([grant_public_query, grant_information_schema_query])
    print("permissions applied to readonly user")


if __name__ == '__main__':  # pragma: no cover
    handler()
