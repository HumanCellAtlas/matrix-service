import argparse
import concurrent.futures
import multiprocessing
import os

from matrix.common import etl
from matrix.common.aws.redshift_handler import RedshiftHandler
from matrix.common.query_constructor import format_str_list

# Match all SS2 and 10X analysis bundles
DSS_SEARCH_QUERY_TEMPLATE = {
    "query": {
        "bool": {
            "must": [
                {
                    "bool": {
                        "should": [],
                        "minimum_number_should_match": 1
                    }
                },
                {
                    "match": {
                        "files.donor_organism_json.biomaterial_core.ncbi_taxon_id": 9606
                    }
                },
                {
                    "bool": {
                        "should": [
                            {
                                "match": {
                                    "files.analysis_process_json.type.text": "analysis"
                                }
                            },
                            {
                                "match": {
                                    "files.analysis_process_json.process_type.text": "analysis"
                                }
                            }
                        ],
                        "minimum_number_should_match": 1
                    }
                }
            ]
        }
    }
}

ALL_10X_SS2_MATCH_TERMS = [
    {
        "match": {
            "files.library_preparation_protocol_json.library_construction_approach.ontology": "EFO:0008931"
        }
    },
    {
        "match": {
            "files.library_preparation_protocol_json.library_construction_approach.ontology_label": "10X v2 sequencing"
        }
    },
    {
        "match": {
            "files.library_preparation_protocol_json.library_construction_method.ontology": "EFO:0008931"
        }
    },
    {
        "match": {
            "files.library_preparation_protocol_json.library_construction_method.ontology_label": "10X v2 sequencing"
        }
    }
]


def load(args):
    dss_query = _build_dss_query(project_uuids=args.project_uuids, bundle_uuids=args.bundle_uuids)
    staging_dir = os.path.abspath('/mnt')
    content_type_patterns = ['application/json; dcp-type="metadata*"'] # match metadata
    filename_patterns = ["*zarr*", # match expression data
                         "*.results", # match SS2 raw count files
                         "*.mtx", "genes.tsv", "barcodes.tsv"] # match 10X raw count files

    is_update = True if args.project_uuids or args.bundle_uuids else False
    if args.state == 0 or args.state == 1:
        etl.etl_dss_bundles(query=dss_query,
                            content_type_patterns=content_type_patterns,
                            filename_patterns=filename_patterns,
                            transformer_cb=etl.transform_bundle,
                            finalizer_cb=etl.finalizer_update if is_update else etl.finalizer_reload,
                            staging_directory=staging_dir,
                            deployment_stage=os.environ['DEPLOYMENT_STAGE'],
                            max_workers=args.max_workers,
                            max_dispatchers=multiprocessing.cpu_count(),
                            dispatcher_executor_class=concurrent.futures.ProcessPoolExecutor)
    elif args.state == 2:
        etl.upload_and_load(staging_dir, is_update=is_update)
    elif args.state == 3:
        etl.load_tables(args.s3_upload_id, is_update=is_update)
    _verify_load(es_query=dss_query)


def _build_dss_query(project_uuids=None, bundle_uuids=None):
    q = DSS_SEARCH_QUERY_TEMPLATE

    if not project_uuids and not bundle_uuids:
        q['query']['bool']['must'][0]['bool']['should'] = ALL_10X_SS2_MATCH_TERMS

    if project_uuids:
        for uuid in project_uuids:
            q['query']['bool']['must'][0]['bool']['should'].append({
                "match": {
                    "files.project_json.provenance.document_id": uuid
                }
            })

    if bundle_uuids:
        for uuid in bundle_uuids:
            q['query']['bool']['must'][0]['bool']['should'].append({
                "match": {
                    "uuid": uuid
                }
            })

    return q


def _verify_load(es_query):
    dss_client = etl.get_dss_client(deployment_stage=os.environ['DEPLOYMENT_STAGE'])
    response = dss_client.post_search.iterate(es_query=es_query,
                                              replica='aws',
                                              per_page=500)
    expected_bundles = list(result['bundle_fqid'] for result in response)

    print(f"Loading {len(expected_bundles)} bundles to {os.environ['DEPLOYMENT_STAGE']} complete.\n"
          f"Verifying row counts in Redshift...")
    redshift = RedshiftHandler()
    count_bundles_query = f"SELECT COUNT(*) FROM analysis WHERE bundle_fqid IN {format_str_list(expected_bundles)}"
    results = redshift.transaction(queries=[count_bundles_query],
                                   return_results=True)
    print(f"Found {results[0][0]} analysis rows for {len(expected_bundles)} expected bundles.")
    assert(results[0][0] == len(expected_bundles))


if __name__ == '__main__':  # pragma: no cover
    parser = argparse.ArgumentParser()
    parser.add_argument("--max-workers",
                        help="Maximum number of concurrent threads during extraction.",
                        type=int,
                        default=512)
    parser.add_argument("--state",
                        help="ETL state (0=pre-ETL, 1=post-E, 2=post-ET, 3=post-ET and upload)",
                        type=int,
                        default=0)
    parser.add_argument("--s3-upload-id",
                        help="Upload UUID in dcp-matrix-service-preload-* S3 bucket "
                             "to load Redshift from (required for state==3).",
                        type=str)
    parser.add_argument("--project-uuids",
                        help="DCP Project UUIDs to perform ETL on.",
                        type=str,
                        nargs="*",
                        default="")
    parser.add_argument("--bundle-uuids",
                        help="DCP Bundle UUIDs to perform ETL on.",
                        type=str,
                        nargs="*",
                        default="")
    _args = parser.parse_args()

    load(_args)
