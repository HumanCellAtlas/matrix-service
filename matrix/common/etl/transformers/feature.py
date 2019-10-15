import gzip
import os
import shutil
import typing
import urllib.request

from threading import Lock

from . import MetadataToPsvTransformer
from matrix.common.aws.redshift_handler import TableName
from matrix.common.constants import GenusSpecies


class FeatureTransformer(MetadataToPsvTransformer):
    """Reads gencode annotation reference and writes out rows for feature table in PSV format."""
    WRITE_LOCK = Lock()
    ANNOTATION_FTP_URLS = {
        GenusSpecies.HUMAN.value: ("ftp://ftp.ebi.ac.uk/pub/databases/gencode/Gencode_human/release_27/"
                                   "gencode.v27.primary_assembly.annotation.gtf.gz"),
        GenusSpecies.MOUSE.value: ("ftp://ftp.ebi.ac.uk/pub/databases/gencode/Gencode_mouse/release_M21/"
                                   "gencode.vM21.annotation.gtf.gz")
    }

    def __init__(self, staging_dir):
        super(FeatureTransformer, self).__init__(staging_dir)

        self.annotation_files = {}

        self._fetch_annotations()

    def _fetch_annotations(self):

        for genus_species, url in self.ANNOTATION_FTP_URLS.items():
            os.makedirs(os.path.join(self.staging_dir, genus_species), exist_ok=True)
            annotation_file_gz = os.path.join(self.staging_dir, genus_species, "gencode_annotation.gtf.gz")
            annotation_file = os.path.join(self.staging_dir, genus_species, "gencode_annotation.gtf")
            urllib.request.urlretrieve(url, annotation_file_gz)
            with gzip.open(annotation_file_gz, 'rb') as f_in:
                with open(annotation_file, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            self.annotation_files[genus_species] = annotation_file

    def _write_rows_to_psvs(self, *args: typing.Tuple):
        with FeatureTransformer.WRITE_LOCK:
            super(FeatureTransformer, self)._write_rows_to_psvs(*args)

    def _parse_from_metadatas(self, filename):
        features = set()

        for genus_species, annotation_file in self.annotation_files.items():
            for line in open(annotation_file):
                # Skip comments
                if line.startswith("#"):
                    continue
                parsed = self.parse_line(line, genus_species)
                if parsed:
                    features.add(parsed)

        return (TableName.FEATURE, features),

    def parse_line(self, line, genus_species):
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

        return self._generate_psv_row(shortened_id, name, feature_type, chrom, start,
                                      end, str(type_ == "gene"), genus_species)
