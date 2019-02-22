import boto3
import os
import io
import typing
from enum import Enum

import hca
import psycopg2
from dcplib.etl import DSSExtractor

from .transformers import MetadataToPsvTransformer
from .transformers.cell_expression import CellExpressionTransformer
from .transformers.analysis import AnalysisTransformer
from .transformers.feature import FeatureTransformer
from .transformers.donor_organism import DonorTransformer
from .transformers.library_preparation import LibraryTransformer
from .transformers.project_publication_contributor import ProjectPublicationContributorTransformer

DEPLOYMENT_STAGE = os.environ['DEPLOYMENT_STAGE']
STAGING_DIRECTORY = os.path.join(os.path.dirname(__file__), 'data')
S3_BUCKET = f"dcp-matrix-service-redshift-preload-{DEPLOYMENT_STAGE}"


class TableName(Enum):
    """
    Redshift table names.
    """
    CELL = "cell"
    EXPRESSION = "expression"
    FEATURE = "feature"
    ANALYSIS = "analysis"
    DONOR_ORGANISM = "donor_organism"
    LIBRARY_PREPARATION = "library_preparation"
    PROJECT = "project"
    PUBLICATION = "publication"
    CONTRIBUTOR = "contributor"


def run_etl(query: dict, content_type_patterns: typing.List[str], filename_patterns: typing.List[str]):
    """
    Extracts specified DSS bundles and files to local disk, transforms to PSV, uploads to S3, loads into Redshift.
    :param query: ES query to match bundles
    :param content_type_patterns: List of content-type patterns to match files to download
    :param filename_patterns: List of filename patterns to match files to download
    """
    extractor = DSSExtractor(staging_directory=STAGING_DIRECTORY,
                             content_type_patterns=content_type_patterns,
                             filename_patterns=filename_patterns,
                             dss_client=get_dss_client())
    extractor.extract(
        query=query,
        transformer=bundle_to_psvs,
        finalizer=upload_to_s3,
        max_workers=256
    )


def bundle_to_psvs(bundle_uuid: str, bundle_version: str, bundle_path: str, extractor: DSSExtractor):
    """
    Transforms an input bundle into PSV rows for each Redshift table.
    :param bundle_uuid: UUID of input bundle
    :param bundle_version: Version of input bundle
    :param bundle_path: Local path to downloaded bundle
    :param extractor: Extractor object
    """
    transformers = [
        CellExpressionTransformer(),
        FeatureTransformer(),
        AnalysisTransformer(),
        LibraryTransformer(),
        DonorTransformer(),
        ProjectPublicationContributorTransformer()
    ]

    for transformer in transformers:
        transformer.transform(bundle_path)


def upload_to_s3():
    """
    Upload to S3 all PSV files in output/ dir.
    """
    for filename in os.listdir(MetadataToPsvTransformer.OUTPUT_DIR):
        filepath = os.path.join(MetadataToPsvTransformer.OUTPUT_DIR, filename)
        try:
            s3 = boto3.client('s3')
            s3.upload_file(filepath, S3_BUCKET, filename)
        except Exception:
            print(f"Unable to find {filename}")

    load_redshift()


def load_redshift():
    """
    Loads PSVs into Redshift via COPY commands.
    """
    dbname = f"matrix_service_{DEPLOYMENT_STAGE}"
    port = 5439
    user = "root"
    password = boto3.client('secretsmanager').get_secret_value('matrix-service/redshift/password')
    host = f"dcp-matrix-service-cluster-{DEPLOYMENT_STAGE}.cjcuhlgpha1p.us-east-1.redshift.amazonaws.com"

    conn_string = f"dbname='{dbname}' port='{port}' user='{user}' password='{password}' host='{host}'"
    conn = psycopg2.connect(conn_string)

    for table in TableName:
        s3_prefix = f"{S3_BUCKET}/{table.value}"
        iam = f"matrix-service-etl-ec2-{DEPLOYMENT_STAGE}"

        buf = io.StringIO(f"COPY {table.value} FROM '{s3_prefix}' iam_role {iam}")
        if table == TableName.CELL or table == TableName.FEATURE:
            buf.write(" COMPUPDATE ON")
        if table == TableName.EXPRESSION:
            buf.write(" GZIP COMPUPDATE ON COMPROWS 10000000")
        buf.write(";")

        copy_stmt = buf.getvalue()
        buf.close()

        cur = conn.cursor()
        cur.execute(copy_stmt)

    conn.close()


def get_dss_client():
    """
    Returns appropriate DSSClient for DEPLOYMENT_STAGE.
    """
    if DEPLOYMENT_STAGE == "prod":
        swagger_url = "https://dss.data.humancellatlas.org/v1/swagger.json"
    elif DEPLOYMENT_STAGE == "staging":
        swagger_url = f"https://dss.staging.data.humancellatlas.org/v1/swagger.json"
    else:
        swagger_url = f"https://dss.integration.data.humancellatlas.org/v1/swagger.json"

    client = hca.dss.DSSClient(swagger_url=swagger_url)
    return client


if __name__ == '__main__':
    # TODO: parameterize inputs
    query = { # TODO: include 10X bundles
        "query": {
            "bool": {
                "must": [
                    {
                        "match": {
                            "files.analysis_protocol_json.protocol_type.text": "analysis"
                        }
                    },
                    {
                        "term": {
                            "files.analysis_process_json.tasks.task_name": "SmartSeq2ZarrConversion"
                        }
                    }
                ]
            }
        }
    }
    content_type_patterns = ['application/json; dcp-type="metadata*"'] # match metadata
    filename_patterns = ["*zarr*", # match expression data
                         "rsem\.*\.results", "qc\.*\.txt", # match SS2 raw count files
                         "*\.mtx", "genes*\.csv", "barcodes*\.csv"] # match 10X raw count files TODO: test 10X

    run_etl(query=query,
            content_type_patterns=content_type_patterns,
            filename_patterns=filename_patterns)
