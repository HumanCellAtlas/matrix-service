import concurrent.futures
import multiprocessing
import os

from matrix.common.etl import run_etl, transform_bundle, finalizer_reload


if __name__ == '__main__':
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
                        "match": {
                            "files.analysis_process_json.process_type.text": "analysis"
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
            filename_patterns=filename_patterns,
            transformer_cb=transform_bundle,
            finalizer_cb=finalizer_reload,
            staging_directory=os.path.abspath('/mnt'),
            deployment_stage=os.environ['DEPLOYMENT_STAGE'],
            max_workers=512,
            max_dispatchers=multiprocessing.cpu_count(),
            dispatcher_executor_class=concurrent.futures.ProcessPoolExecutor)
