import boto3
import json
import os
import typing
from urllib.parse import urlparse

import hca
import psycopg2
from dcplib.etl import DSSExtractor

from . import CREATE_TABLE_QUERY
from transformers import MetadataToPsvTransformer, TableName
from transformers.cell_expression import CellExpressionTransformer
from transformers.analysis import AnalysisTransformer
from transformers.feature import FeatureTransformer
from transformers.donor_library import DonorLibraryTransformer
from transformers.project_publication_contributor import ProjectPublicationContributorTransformer

DEPLOYMENT_STAGE = os.environ['DEPLOYMENT_STAGE']
S3_BUCKET = f"dcp-matrix-service-redshift-preload-{DEPLOYMENT_STAGE}"


def run_etl(query: dict, content_type_patterns: typing.List[str], filename_patterns: typing.List[str]):
    """
    Extracts specified DSS bundles and files to local disk, transforms to PSV, uploads to S3, loads into Redshift.
    :param query: ES query to match bundles
    :param content_type_patterns: List of content-type patterns to match files to download
    :param filename_patterns: List of filename patterns to match files to download
    """
    os.makedirs(MetadataToPsvTransformer.OUTPUT_DIR, exist_ok=True)
    extractor = DSSExtractor(staging_directory=MetadataToPsvTransformer.STAGING_DIRECTORY,
                             content_type_patterns=content_type_patterns,
                             filename_patterns=filename_patterns,
                             dss_client=get_dss_client())
    extractor.extract(
        query=query,
        transformer=transform_bundle,
        finalizer=transform_and_load,
        max_workers=256
    )


def transform_bundle(bundle_uuid: str, bundle_version: str, bundle_path: str, extractor: DSSExtractor):
    """
    Transforms a downloaded bundle into PSV rows - per bundle transformer passed to ETL.
    :param bundle_uuid: Downloaded bundle UUID
    :param bundle_version: Downloaded bundle version
    :param bundle_path: Local path to downloaded bundle dir
    :param extractor: ETL extractor object
    """
    transformers = [
        CellExpressionTransformer()
    ]

    for transformer in transformers:
        transformer.transform(bundle_path)


def transform_and_load(extractor: DSSExtractor):
    """
    Final transformer during ETL, invokes loader - finalizer passed to ETL.
    :param extractor: ETL extractor object
    """
    transformers = [
        FeatureTransformer(),
        AnalysisTransformer(),
        DonorLibraryTransformer(),
        ProjectPublicationContributorTransformer()
    ]

    for transformer in transformers:
        transformer.transform(os.path.join(extractor.sd, 'bundles'))

    upload_to_s3()


def upload_to_s3():
    """
    Uploads to S3 all files in OUTPUT_DIR.
    """
    for name in os.listdir(MetadataToPsvTransformer.OUTPUT_DIR):
        path = os.path.join(MetadataToPsvTransformer.OUTPUT_DIR, name)
        if os.path.isdir(path):
            for filename in os.listdir(path):
                _upload_file_to_s3(os.path.join(path, filename), f"{name}/{filename}")
        else:
            _upload_file_to_s3(path, name)

    load_redshift()


def _upload_file_to_s3(path_to_file, s3_prefix):
    """
    Uploads a file to S3 preload bucket.
    :param path_to_file: Local path to file to upload
    :param s3_prefix: S3 prefix to upload to
    """
    try:
        s3 = boto3.client('s3', region_name='us-east-1')
        s3.upload_file(path_to_file, S3_BUCKET, s3_prefix)
    except Exception as e:
        print(e)


def load_redshift():
    """
    Creates tables and loads PSVs in S3 into Redshift via SQL COPY.
    """
    conn_url = json.loads(boto3.client('secretsmanager', region_name='us-east-1').
                          get_secret_value(SecretId='dcp/matrix/dev/database')['SecretString'])['database_uri']
    url_parts = urlparse(conn_url)
    dbname = f"matrix_service_{DEPLOYMENT_STAGE}"

    conn_string = f"dbname={dbname} user={url_parts.username} password={url_parts.password} " \
                  f"host={url_parts.hostname} port={url_parts.port}"
    conn = psycopg2.connect(conn_string)

    for table in TableName:
        create_query = CREATE_TABLE_QUERY[table.value]

        s3_prefix = f"s3://{S3_BUCKET}/{table.value}"
        iam = f"arn:aws:iam::861229788715:role/matrix-service-redshift-{DEPLOYMENT_STAGE}"

        if table == TableName.FEATURE:
            copy_stmt = f"COPY {table.value} FROM '{s3_prefix}' iam_role '{iam}' COMPUPDATE ON;"
        elif table == TableName.CELL:
            copy_stmt = f"COPY {table.value} FROM '{s3_prefix}' iam_role '{iam}' GZIP COMPUPDATE ON;"
        elif table == TableName.EXPRESSION or table == TableName.CELL:
            copy_stmt = f"COPY {table.value} FROM '{s3_prefix}' iam_role '{iam}' GZIP COMPUPDATE ON COMPROWS 10000000;"
        else:
            copy_stmt = f"COPY {table.value} FROM '{s3_prefix}' iam_role '{iam}';"

        print(table.value, copy_stmt)
        cur = conn.cursor()
        cur.execute(create_query)
        cur.execute(copy_stmt)

    conn.commit()
    conn.close()


def get_dss_client():
    """
    Returns appropriate DSSClient for DEPLOYMENT_STAGE.
    """
    if DEPLOYMENT_STAGE == "prod":
        swagger_url = "https://dss.data.humancellatlas.org/v1/swagger.json"
    elif DEPLOYMENT_STAGE == "staging":
        swagger_url = "https://dss.staging.data.humancellatlas.org/v1/swagger.json"
    else:
        swagger_url = "https://dss.integration.data.humancellatlas.org/v1/swagger.json"

    client = hca.dss.DSSClient(swagger_url=swagger_url)
    return client


if __name__ == '__main__':
    # All SS2 analysis bundles
    query = {
        "query": {
            "bool": {
                "must": [
                    {
                        "match": {
                            "files.library_preparation_protocol_json.library_construction_approach.ontology": "EFO:0008931"
                        }
                    },
                    {
                        "match": {
                            "files.sequencing_protocol_json.paired_end": True
                        }
                    },
                    {
                        "match": {
                            "files.donor_organism_json.biomaterial_core.ncbi_taxon_id": 9606
                        }
                    },
                    {
                        "match": {
                            "files.analysis_process_json.process_type.text": "analysis"
                        }
                    }
                ],
                "should": [
                    {
                        "match": {
                            "files.dissociation_protocol_json.dissociation_method.ontology": "EFO:0009108"
                        }
                    },
                    {
                        "match": {
                            "files.dissociation_protocol_json.dissociation_method.text": "mouth pipette"
                        }
                    }
                ],
                "must_not": [
                    {
                        "range": {
                            "files.donor_organism_json.biomaterial_core.ncbi_taxon_id": {
                                "lt": 9606
                            }
                        }
                    },
                    {
                        "range": {
                            "files.donor_organism_json.biomaterial_core.ncbi_taxon_id": {
                                "gt": 9606
                            }
                        }
                    }
                ]
            }
        }
    }

    content_type_patterns = ['application/json; dcp-type="metadata*"'] # match metadata
    filename_patterns = ["*zarr*", # match expression data
                         "*.results", # match SS2 raw count files
                         "*.mtx", "genes.tsv", "barcodes.tsv"] # match 10X raw count files

    run_etl(query=query,
            content_type_patterns=content_type_patterns,
            filename_patterns=filename_patterns)
