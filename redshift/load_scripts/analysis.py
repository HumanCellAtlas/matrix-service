import json
import os
import pathlib

def parse_ap(ap_path):

    ap_dict = json.load(open(ap_path))
    key = ap_dict["provenance"]["document_id"]
    bundle_uuid = ap_path.parts[-2]
    protocol = ap_dict["protocol_core"]["protocol_id"]
    awg_disposition = "blessed" if protocol.startswith("smartseq2") else "community"


    return {
        "key": key,
        "bundle_uuid": bundle_uuid,
        "protocol": protocol,
        "awg_disposition": awg_disposition
    }

def main():
    p = pathlib.Path(".")

    ap_infos = []
    for ap_path in p.glob("**/analysis_protocol_*.json"):
        ap_info = parse_ap(ap_path)
        ap_infos.append(ap_info)

    ap_data = set()
    for ap_info in ap_infos:
        ap_data.add(
            '|'.join([ap_info["key"],
                      ap_info["bundle_uuid"],
                      ap_info["protocol"],
                      ap_info["aws_disposition"]]))

    with open("analysis.data", "w") as ap_file:
        for ap_line in ap_data:
            ap_file.write(ap_line + '\n')

main()
