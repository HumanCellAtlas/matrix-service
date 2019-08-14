import concurrent.futures
import os
import traceback
import typing
import uuid

import boto3
import hca
import psycopg2
from dcplib.etl import DSSExtractor

from matrix.common import date
from matrix.common.aws.redshift_handler import RedshiftHandler, TableName
from matrix.common.constants import CREATE_QUERY_TEMPLATE, MATRIX_ENV_TO_DSS_ENV
from matrix.common.logging import Logging
from .transformers import MetadataToPsvTransformer
from .transformers.analysis import AnalysisTransformer
from .transformers.cell_expression import CellExpressionTransformer
from .transformers.feature import FeatureTransformer
from .transformers.project_publication_contributor import ProjectPublicationContributorTransformer
from .transformers.specimen_library import SpecimenLibraryTransformer

logger = Logging.get_logger(__name__)


def etl_dss_bundles(query: dict,
                    content_type_patterns: typing.List[str],
                    filename_patterns: typing.List[str],
                    transformer_cb,
                    finalizer_cb,
                    staging_directory,
                    deployment_stage: str,
                    max_workers: int = 256,
                    max_dispatchers: int = 1,
                    dispatcher_executor_class: concurrent.futures.Executor = concurrent.futures.ProcessPoolExecutor):
    """
    Extracts specified DSS bundles and files to local disk, transforms to PSV, uploads to S3, loads into Redshift.
    :param query: ES query to match bundles
    :param content_type_patterns: List of content-type patterns to match files to download
    :param filename_patterns: List of filename patterns to match files to download
    :param transformer_cb: Callback to run per downloaded bundle
    :param finalizer_cb: Callback to run after all bundles downloaded
    :param staging_directory: Local directory to stage extracted data
    :param deployment_stage: Matrix Service deployment stage
    :param max_workers: Max number of parallel executions during extraction
    :param max_dispatchers: Max number of parallel exeuctions during transformation
    :param dispatcher_executor_class: The concurrent.futures.Executor class to manage dispatch executors
    """
    logger.info(f"Loading {deployment_stage} redshift cluster via ETL")

    os.makedirs(os.path.join(staging_directory,
                             MetadataToPsvTransformer.OUTPUT_DIRNAME,
                             TableName.CELL.value),
                exist_ok=True)
    os.makedirs(os.path.join(staging_directory,
                             MetadataToPsvTransformer.OUTPUT_DIRNAME,
                             TableName.EXPRESSION.value),
                exist_ok=True)
    os.makedirs(os.path.join(staging_directory,
                             MetadataToPsvTransformer.LOG_DIRNAME),
                exist_ok=True)

    extractor = DSSExtractor(staging_directory=staging_directory,
                             content_type_patterns=content_type_patterns,
                             filename_patterns=filename_patterns,
                             dss_client=get_dss_client(deployment_stage))
    extractor.extract(
        query=query,
        transformer=transformer_cb,
        finalizer=finalizer_cb,
        max_workers=max_workers,
        max_dispatchers=max_dispatchers,
        dispatch_executor_class=dispatcher_executor_class
    )


def transform_bundle(bundle_uuid: str,
                     bundle_version: str,
                     bundle_path: str,
                     bundle_manifest_path: str,
                     extractor: DSSExtractor):
    """
    Per bundle callback passed to DSSExtractor.extract.
    Generates cell and expression table rows from a downloaded DSS bundle.
    :param bundle_uuid: Downloaded bundle UUID
    :param bundle_version: Downloaded bundle version
    :param bundle_path: Local path to downloaded bundle dir
    :param extractor: ETL extractor object
    """
    logger.info(f"ETL: Downloaded bundle {bundle_uuid}.{bundle_version}. Transforming to PSV.")
    transformer = CellExpressionTransformer(extractor.sd)
    try:
        transformer.transform_bundle(bundle_path, bundle_manifest_path)
    except Exception as ex:
        _log_error(f"{bundle_uuid}.{bundle_version}", ex, traceback.format_exc(), extractor)


def _run_transformers(extractor: DSSExtractor, transformers: list):
    """
    Executes a provided list of transformers.
    :param extractor: DSSExtractor
    :param transformers: List of MetadataToPsvTransformers to execute
    """
    logger.info(f"ETL: All bundles downloaded. Performing final PSV transformations.")

    for transformer in transformers:
        try:
            transformer.transform(os.path.join(extractor.sd, 'bundles'))
        except Exception as ex:
            _log_error(transformer.__class__.__name__, ex, traceback.format_exc(), extractor)

    logger.info(f"ETL: All transformations complete.")


def finalizer_reload(extractor: DSSExtractor):
    """
    Final callback during ETL for loading an empty database. Invokes Loader.
    """
    _run_transformers(extractor, [
        FeatureTransformer(extractor.sd),
        AnalysisTransformer(extractor.sd),
        SpecimenLibraryTransformer(extractor.sd),
        ProjectPublicationContributorTransformer(extractor.sd)
    ])
    upload_and_load(extractor.sd, is_update=False)


def finalizer_update(extractor: DSSExtractor):
    """
    Final callback during ETL to perform database updates. Invokes Loader with update flag.
    :return:
    """
    _run_transformers(extractor, [
        FeatureTransformer(extractor.sd),
        AnalysisTransformer(extractor.sd),
        SpecimenLibraryTransformer(extractor.sd),
        ProjectPublicationContributorTransformer(extractor.sd)
    ])
    upload_and_load(extractor.sd, is_update=True)


def finalizer_notification(extractor: DSSExtractor):
    """
    Final callback during ETL to handle DSS notifications. Invokes Loader with update flag.
    :return:
    """
    _run_transformers(extractor, [
        AnalysisTransformer(extractor.sd),
        SpecimenLibraryTransformer(extractor.sd),
        ProjectPublicationContributorTransformer(extractor.sd)
    ])
    upload_and_load(extractor.sd, is_update=True)


def _log_error(entity: str, exception: Exception, trace: str, extractor: DSSExtractor):
    """
    Logs an ETL error and exception stack trace to a file.
    Error messages and exceptions are written to 'errors.txt'
    A list of failed entities are written to 'failed_transforms.txt'
    :param entity: A MetadataToPsvTransformer, or a bundle FQID for CellExpressionTransformer errors
    :param exception: Thrown exception string
    :param trace: Exception stack trace
    :param extractor: DSSExtractor
    """
    logger.error(f"Failed to transform {entity}.", exception)

    timestamp = date.get_datetime_now(as_string=True)
    log_file_path = os.path.join(extractor.sd, MetadataToPsvTransformer.LOG_DIRNAME, 'errors.txt')
    with open(log_file_path, 'a+') as fh:
        fh.write(f"[{timestamp}] {entity} failed with exception: {exception}\n{trace}\n")

    ft_file_path = os.path.join(extractor.sd, MetadataToPsvTransformer.LOG_DIRNAME, 'failed_transforms.txt')
    with open(ft_file_path, 'a+') as fh:
        fh.write(f"{entity}\n")


def upload_and_load(staging_dir: str, is_update: bool = False):
    """
    Uploads generated PSV files to S3 and loads Redshift tables from S3.
    :param staging_dir: DSSExtractor staging directory
    :param is_update: True if existing rows are being updated, else False
    """
    job_id = str(uuid.uuid4())

    _upload_dir_to_s3(job_id=job_id,
                      local_dir=os.path.join(staging_dir, MetadataToPsvTransformer.OUTPUT_DIRNAME))
    load_tables(job_id=job_id,
                is_update=is_update)


def _upload_dir_to_s3(job_id: str, local_dir: str):
    """
    Uploads all files in local_dir to S3.
    :param job_id: UUID used as S3 prefix during upload
    :param local_dir: Local directory to upload files from
    """
    for name in os.listdir(local_dir):
        logger.info(f"ETL: Uploading {name} to {os.environ['MATRIX_PRELOAD_BUCKET']}.")
        path = os.path.join(local_dir, name)
        if os.path.isdir(path):
            for filename in os.listdir(path):
                _upload_file_to_s3(os.path.join(path, filename), f"{job_id}/{name}/{filename}")
        else:
            _upload_file_to_s3(path, f"{job_id}/{name}")


def _upload_file_to_s3(path_to_file: str, s3_prefix: str):
    """
    Uploads a file to S3 preload bucket.
    :param path_to_file: Local path to file to upload
    :param s3_prefix: S3 prefix to upload to
    """
    try:
        s3 = boto3.client('s3', region_name='us-east-1')
        s3.upload_file(path_to_file, os.environ['MATRIX_PRELOAD_BUCKET'], s3_prefix)
    except Exception as e:
        print(e)


def load_tables(job_id: str, is_update: bool = False):
    """
    Creates tables and loads PSVs from S3 into Redshift via SQL COPY.
    :param job_id: UUID of the S3 prefix containinng the data to load in
    :param is_update: True if existing database rows will be updated, else False
    """
    _create_tables()
    lock_query = """LOCK TABLE write_lock"""
    delete_query_template = """DELETE FROM {0} USING {0}_temp WHERE {0}.{1} = {0}_temp.{1};"""
    insert_query_template = """INSERT INTO {0} SELECT * FROM {0}_temp;"""

    redshift = RedshiftHandler()
    transaction = [lock_query]
    for table in TableName:
        if (is_update and table == TableName.FEATURE) or table == TableName.WRITE_LOCK:
            continue
        s3_prefix = f"s3://{os.environ['MATRIX_PRELOAD_BUCKET']}/{job_id}/{table.value}"
        iam = os.environ['MATRIX_REDSHIFT_IAM_ROLE_ARN']

        table_name = table.value if not is_update else f"{table.value}_temp"

        if table == TableName.FEATURE:
            copy_stmt = f"COPY {table_name} FROM '{s3_prefix}' iam_role '{iam}' COMPUPDATE ON;"
        elif table == TableName.CELL:
            copy_stmt = f"COPY {table_name} FROM '{s3_prefix}' iam_role '{iam}' GZIP COMPUPDATE ON;"
        elif table == TableName.EXPRESSION:
            copy_stmt = f"COPY {table_name} FROM '{s3_prefix}' iam_role '{iam}' GZIP COMPUPDATE ON COMPROWS 10000000;"
        else:
            copy_stmt = f"COPY {table_name} FROM '{s3_prefix}' iam_role '{iam}';"

        if is_update:
            logger.info(f"ETL: Building queries to update {table.value} table")
            transaction.extend([
                CREATE_QUERY_TEMPLATE[table.value].format("TEMP ", "_temp", table_name),
                copy_stmt,
                delete_query_template.format(table.value, RedshiftHandler.PRIMARY_KEY[table]),
                insert_query_template.format(table.value)
            ])
        else:
            logger.info(f"ETL: Building queries to load {table_name} table")
            transaction.append(copy_stmt)

    logger.info(f"ETL: Populating Redshift tables. Committing transaction.")
    try:
        redshift.transaction(transaction)
    except psycopg2.Error as e:
        logger.error("Failed to populate Redshift tables. Rolling back.", e)


def _create_tables():
    redshift = RedshiftHandler()
    transaction = []
    for table in TableName:
        transaction.append(CREATE_QUERY_TEMPLATE[table.value].format("", "", table.value))

    redshift.transaction(transaction)


def get_dss_client(deployment_stage: str):
    """
    Returns appropriate DSSClient for deployment_stage.
    """
    dss_env = MATRIX_ENV_TO_DSS_ENV[deployment_stage]
    if dss_env == "prod":
        swagger_url = "https://dss.data.humancellatlas.org/v1/swagger.json"
    else:
        swagger_url = f"https://dss.{dss_env}.data.humancellatlas.org/v1/swagger.json"

    logger.info(f"ETL: Hitting DSS with Swagger URL: {swagger_url}")

    dss_config = hca.HCAConfig()
    dss_config['DSSClient'] = {}
    dss_config['DSSClient']['swagger_url'] = swagger_url

    client = hca.dss.DSSClient(config=dss_config)
    return client
