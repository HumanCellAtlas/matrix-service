"""Flatten DSS bundle metadata into a csv.

Uses queues a bunch to deal with DSS latency.
"""

import csv
import multiprocessing
import re
import time

import hca.dss

# This is the query that gets the list of bundles whose metadata will be placed
# into a csv.
ES_QUERY = {
    "query": {
        "bool": {
            "must": [
                {
                    "match": {
                        "files.project_json.project_core.project_short_name":
                            "Single cell transcriptome analysis of human pancreas"
                    }
                }
            ],
            "must_not": [
                {
                    "match": {
                        "files.analysis_protocol_json.protocol_type.text": "analysis"
                    }
                }
            ]
        }
    }
}

STOP_SENTINEL = "STOP"


def get_metadata_files_specs(bundle_queue, file_queue):
    """Pull bundle fqids off the queue, and get metadata file information them."""

    while True:
        bundle_spec = bundle_queue.get()
        if bundle_spec == STOP_SENTINEL:
            return
        bundle_uuid, bundle_version = bundle_spec["bundle_fqid"].split('.', 1)
        client = hca.dss.DSSClient()
        while True:
            try:
                # Get all the indexed files except links.json. What is that
                # even
                output = [f for f in client.get_bundle(uuid=bundle_uuid,
                                                       version=bundle_version,
                                                       replica="aws")["bundle"]["files"]
                          if f["indexed"] and f["name"] != "links.json"]
                break
            except:
                # You build on failure. You use it as a stepping stone. Close
                # the door on the past.
                time.sleep(1)
                continue

        for file_spec in output:
            file_queue.put((bundle_uuid, bundle_version, file_spec))


def _walk_dss_dict(d, prefix):
    """Walk through a metadata dictionary, and collapse it to elasticsearch terms
    and values.
    """

    outputs = []
    for key, value in d.items():
        new_prefix = '.'.join([prefix, key])
        if isinstance(value, dict):
            outputs.extend(_walk_dss_dict(value, new_prefix))
        elif isinstance(value, list):
            for el in value:
                if isinstance(el, dict):
                    outputs.extend(_walk_dss_dict(el, new_prefix))
                else:
                    outputs.append((new_prefix, el))
        else:
            outputs.append((new_prefix, value))
    return outputs


def build_metadata_kvs(file_queue, kv_queue):
    """Pull file info off the queue and parse it into metadata key, value pairs."""
    while True:
        kv_job_spec = file_queue.get()
        if kv_job_spec == STOP_SENTINEL:
            return

        bundle_uuid, bundle_version, file_spec = kv_job_spec

        try:
            json_key = "files." + '_'.join(re.search("(\w+)_\d+\.(json)",
                                                     file_spec["name"]).groups())
        except:
            # If this fails, I think it means that it's a bundle with old-style
            # metadata. So whatever.
            continue

        while True:
            try:
                client = hca.dss.DSSClient()
                metadata_dict = client.get_file(uuid=file_spec["uuid"],
                                                version=file_spec["version"],
                                                replica="aws")
                break
            except:
                # Think like a queen. A queen is not afraid to fail. Failure is
                # another stepping-stone to greatness.
                time.sleep(1)
                continue

        metadata_kvs = _walk_dss_dict(metadata_dict, json_key)

        kv_queue.put((bundle_uuid, bundle_version, metadata_kvs))


def coalesce_kvs(kv_queue):
    """Combine all the metadata into a csv."""

    def _prepare_metadata_dict(bundle_uuid, bundle_version, metadata_kvs):
        d = {}
        for metadata_kv in metadata_kvs:
            key, value = metadata_kv

            if key in d:
                if isinstance(d[key], list):
                    d[key].append(value)
                else:
                    d[key] = [d[key], value]
            else:
                d[key] = value
        d["bundle_uuid"] = bundle_uuid
        d["bundle_version"] = bundle_version

        for key in d:
            if isinstance(d[key], list):
                d[key] = ';'.join([str(v) for v in sorted(set(d[key]))])

        return d

    all_kvs = {}
    while True:
        bundle_info = kv_queue.get()
        if bundle_info == STOP_SENTINEL:
            break
        bundle_uuid, bundle_version, metadata_kvs = bundle_info
        key = (bundle_uuid, bundle_version)
        all_kvs.setdefault(key, []).extend(metadata_kvs)

    prepped_dicts = []
    for key in all_kvs:
        bundle_uuid, bundle_version = key
        prepped_dicts.append(_prepare_metadata_dict(bundle_uuid, bundle_version, all_kvs[key]))

    fieldnames = set()
    for kvs in prepped_dicts:
        for k in kvs:
            fieldnames.add(k)
    with open("output.csv", "w") as outf:
        writer = csv.DictWriter(outf, fieldnames, dialect="excel")
        writer.writeheader()
        writer.writerows(prepped_dicts)


def main():

    bundle_queue = multiprocessing.Queue()
    file_queue = multiprocessing.Queue()
    kv_queue = multiprocessing.Queue()

    # Set these very high to spice up the grafana dashboard
    num_bundle_processes = 3
    num_file_processes = 12

    bundle_processes = []
    for _ in range(num_bundle_processes):
        bundle_process = multiprocessing.Process(target=get_metadata_files_specs,
                                                 args=(bundle_queue, file_queue))
        bundle_process.start()
        bundle_processes.append(bundle_process)

    client = hca.dss.DSSClient()
    for bundle_spec in client.post_search.iterate(es_query=ES_QUERY, replica="aws"):
        bundle_queue.put(bundle_spec)

    for _ in range(num_bundle_processes):
        bundle_queue.put(STOP_SENTINEL)

    file_processes = []
    for _ in range(num_file_processes):
        file_process = multiprocessing.Process(target=build_metadata_kvs,
                                               args=(file_queue, kv_queue))
        file_process.start()
        file_processes.append(file_process)

    coalesce_process = multiprocessing.Process(target=coalesce_kvs, args=(kv_queue,))
    coalesce_process.start()

    for bundle_process in bundle_processes:
        bundle_process.join()

    for _ in range(num_file_processes):
        file_queue.put(STOP_SENTINEL)

    for file_process in file_processes:
        file_process.join()

    kv_queue.put(STOP_SENTINEL)
    coalesce_process.join()


main()
