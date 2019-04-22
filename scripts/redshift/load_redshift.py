import concurrent.futures
import multiprocessing
import os
import argparse

from matrix.common.etl import run_etl, transform_bundle, finalizer_reload, load_from_local_files, load_from_s3


if __name__ == '__main__':
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
                             "to load Redshift from (required for state 3)",
                        type=str)
    args = parser.parse_args()

    # Match all SS2 and 10X analysis bundles
    query = {
        "query": {
            "bool": {
                "must": [
                    {
                        "bool": {
                            "should": [
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
                            ],
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

    staging_dir = os.path.abspath('/mnt')
    content_type_patterns = ['application/json; dcp-type="metadata*"'] # match metadata
    filename_patterns = ["*zarr*", # match expression data
                         "*.results", # match SS2 raw count files
                         "*.mtx", "genes.tsv", "barcodes.tsv"] # match 10X raw count files

    if args.state == 0 or args.state == 1:
        run_etl(query=query,
                content_type_patterns=content_type_patterns,
                filename_patterns=filename_patterns,
                transformer_cb=transform_bundle,
                finalizer_cb=finalizer_reload,
                staging_directory=staging_dir,
                deployment_stage=os.environ['DEPLOYMENT_STAGE'],
                max_workers=args.max_workers,
                max_dispatchers=multiprocessing.cpu_count(),
                dispatcher_executor_class=concurrent.futures.ProcessPoolExecutor)
    elif args.state == 2:
        load_from_local_files(staging_dir)
    elif args.state == 3:
        load_from_s3(args.s3_upload_id)
