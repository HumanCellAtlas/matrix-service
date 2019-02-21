"""Parse the gencode annotation gtf into a psv that can be used to populate the
feature table.

The gtf is gencode v27:
ftp://ftp.ebi.ac.uk/pub/databases/gencode/Gencode_human/release_27/gencode.v27.chr_patch_hapl_scaff.annotation.gtf.gz
"""
import sys

def parse_line(line):
    """Parse a GTF line into the fields we want."""
    p = line.strip().split("\t")
    type_ = p[2]

    if type_ not in ("gene", "transcript"):
        return ''
    chrom = p[0]
    start = p[3]
    end = p[4]
    attrs = p[8]

    for attr in attrs.split(";"):
        if not attr:
            continue
        label, value = attr.strip().split(" ")
        value = eval(value)
        label = label.strip()

        if label == type_ + "_id":
            id_ = value
        elif label == type_ + "_type":
            feature_type = value
        elif label == type_ + "_name":
            name = value
    shortened_id = id_.split(".", 1)[0]
    if id_.endswith("_PAR_Y"):
        shortened_id += "_PAR_Y"

    return '|'.join([
        shortened_id, name, feature_type, chrom, start, end,
        str(type_ == "gene")])

def main(filename):
    for line in open(filename):
        # Skip comments
        if line.startswith("#"):
            continue
        parsed = parse_line(line)
        if parsed:
            print(parsed)

if __name__ == '__main__':
    main(sys.argv[1])
