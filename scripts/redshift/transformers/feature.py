import gzip
import shutil
import typing
import urllib.request

from threading import Lock

from . import MetadataToPsvTransformer
from ..init_cluster import TableName


class FeatureTransformer(MetadataToPsvTransformer):
    WRITE_LOCK = Lock()
    ANNOTATION_FTP_URL = "ftp://ftp.ebi.ac.uk/pub/databases/gencode/Gencode_human/release_27/" \
                         "gencode.v27.chr_patch_hapl_scaff.annotation.gtf.gz"
    FILENAME = "gencode_annotation.gtf"
    GZIP_FILENAME = f"{FILENAME}.gz"

    def __init__(self):
        urllib.request.urlretrieve(self.ANNOTATION_FTP_URL, self.GZIP_FILENAME)
        with gzip.open(self.GZIP_FILENAME, 'rb') as f_in:
            with open(self.FILENAME, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)

    def _write_rows_to_psvs(self, *args: typing.Tuple):
        with FeatureTransformer.WRITE_LOCK:
            super(FeatureTransformer, self)._write_rows_to_psvs(*args)

    def _parse_from_metadatas(self, filename):
        features = set()

        for line in open(filename):
            # Skip comments
            if line.startswith("#"):
                continue
            parsed = self.parse_line(line)
            if parsed:
                features.add(parsed)

        return (TableName.FEATURE, features),

    def parse_line(self, line):
        """Parse a GTF line into the fields we want."""
        p = line.strip().split("\t")
        type_ = p[2]

        if type_ not in ("gene", "transcript"):
            return ''
        chrom = p[0]
        start = p[3]
        end = p[4]
        attrs = p[8]

        id_ = ""
        name = ""
        feature_type = ""
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

        self._generate_psv_row(shortened_id, name, feature_type, chrom, start, end, str(type_ == "gene"))
