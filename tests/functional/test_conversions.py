"""Test the output of matrix format conversions.

The general strategy is to look at the results of each format and compare them
to reading the redshift PSV without any conversion. That "direct" reading is
done differently than it's done in the converter code, using csv and boto3
instead of pandas.
"""

import argparse
import collections
import csv
import gzip
import io
import json
import os
import shutil
import urllib.parse
import unittest
from unittest import mock
import zipfile

from botocore import UNSIGNED
from botocore.config import Config

import boto3
import loompy
import s3fs
import scipy

from matrix.docker.matrix_converter import MatrixConverter

# This is the 2544 pancreas data in a public bucket.
CELL_MANIFEST = "s3://hca-matrix-conversion-test-data/pancreas/cell_metadata_manifest"
GENE_MANIFEST = "s3://hca-matrix-conversion-test-data/pancreas/gene_metadata_manifest"
EXPRESSION_MANIFEST = "s3://hca-matrix-conversion-test-data/pancreas/expression_manifest"


class TestConversions(unittest.TestCase):
    """Tests for the csv, mtx, and loom matrix conversion outputs.

    Each test relies on a big dict of expected values read from the psv files once for the
    whole set of tests..
    """

    @classmethod
    def setUpClass(cls):
        """Read the psv cell and expression data into dicts only once."""

        cls.s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
        cls.direct_expression = cls._read_expression_direct()
        cls.direct_cell = cls._read_cell_direct()

    @classmethod
    def tearDownClass(cls):
        """Clean up the files created by the tests."""

        to_delete = ["test.mtx.zip", "test.csv.zip", "test.loom",
                     "test.mtx", "test.csv", ".loom_parts"]

        for path in to_delete:
            if os.path.isdir(path):
                shutil.rmtree(path)
            elif os.path.isfile(path):
                os.remove(path)

    @classmethod
    def _read_expression_direct(cls):
        """Read expression data directly from the redshift-generated psv."""

        expression_data = {}
        expression_columns = cls._get_columns(EXPRESSION_MANIFEST)
        expression_psvs = cls._get_component_psvs(EXPRESSION_MANIFEST)

        for expression_psv in expression_psvs:
            for row in gzip.GzipFile(fileobj=io.BytesIO(cls._read_s3_url(expression_psv))):
                row_dict = dict(zip(expression_columns, row.strip().split(b'|')))
                expression_data.setdefault(
                    row_dict["cellkey"].decode(), {})[row_dict["featurekey"].decode()] = \
                    float(row_dict["exrpvalue"])

        return expression_data

    @classmethod
    def _read_cell_direct(cls):
        """Read expression data directly from the redshift-generated psv."""

        cell_data = {}
        cell_columns = cls._get_columns(CELL_MANIFEST)
        cell_psvs = cls._get_component_psvs(CELL_MANIFEST)

        for cell_psv in cell_psvs:
            for row in gzip.GzipFile(fileobj=io.BytesIO(cls._read_s3_url(cell_psv))):
                row_dict = dict(zip(cell_columns, row.strip().split(b'|')))
                cell_data[row_dict["cellkey"].decode()] = {k: v.decode() for
                                                           k, v in row_dict.items()}

        return cell_data

    @classmethod
    def _read_s3_url(cls, s3_url):
        """Completely read an S3 url using boto3."""

        parsed_url = urllib.parse.urlparse(s3_url)
        return cls.s3.get_object(Bucket=parsed_url.netloc,
                                 Key=parsed_url.path.lstrip("/"))["Body"].read()

    @classmethod
    def _get_component_psvs(cls, manifest_url):
        """Get a list of s3 urls to result parts."""
        return [k["url"] for k in json.loads(cls._read_s3_url(manifest_url))["entries"]]

    @classmethod
    def _get_columns(cls, manifest_url):
        """Get a list of column headers for an output table."""
        return [k["name"] for k in
                json.loads(cls._read_s3_url(manifest_url))["schema"]["elements"]]

    @mock.patch("matrix.docker.matrix_converter.MatrixConverter._upload_converted_matrix")
    def test_csv(self, mock_upload_method):
        """Test the csv output."""

        args = argparse.Namespace(
            request_id="test_id",
            expression_manifest_key=EXPRESSION_MANIFEST,
            cell_metadata_manifest_key=CELL_MANIFEST,
            gene_metadata_manifest_key=GENE_MANIFEST,
            target_path="test.csv.zip",
            format="csv",
            working_dir=".")

        with mock.patch("matrix.docker.matrix_converter.RequestTracker") as mock_request_tracker, \
                mock.patch("os.remove"):
            matrix_converter = MatrixConverter(args)
            matrix_converter.FS = s3fs.S3FileSystem(anon=True)

            mock_request_tracker.return_value.creation_date = "1983-10-11T000000.00Z"

            matrix_converter.run()

        with zipfile.ZipFile("test.csv.zip") as csv_output:

            # Check the components of the zip file
            members = csv_output.namelist()
            self.assertIn("test.csv/expression.csv", members)
            self.assertIn("test.csv/genes.csv", members)
            self.assertIn("test.csv/cells.csv", members)
            self.assertEqual(len(members), 4)

            # Read in the expression data
            csv_expression = {}
            for row in csv.DictReader(io.StringIO(
                    csv_output.read("test.csv/expression.csv").decode())):

                csv_expression[row["cellkey"]] = {}
                for gene, exprvalue in row.items():
                    if gene == "cellkey" or exprvalue == '0':
                        continue
                    csv_expression[row["cellkey"]][gene] = float(exprvalue)

            # Check it against the direct expression data
            self.assertListEqual(list(csv_expression.keys()),
                                 list(self.direct_expression.keys()))

            for cellkey in csv_expression:
                csv_dict = csv_expression[cellkey]
                direct_dict = self.direct_expression[cellkey]
                self.assertListEqual(list(csv_dict.keys()), list(direct_dict.keys()))

                for gene in csv_dict:
                    self.assertAlmostEqual(csv_dict[gene], direct_dict[gene], places=0)

            del csv_expression

            csv_cells = {}
            for row in csv.DictReader(io.StringIO(csv_output.read("test.csv/cells.csv").decode())):
                csv_cells[row["cellkey"]] = row
            self.assertListEqual(list(csv_cells.keys()), list(self.direct_cell.keys()))
            for cellkey in csv_cells:
                self.assertListEqual(list(csv_cells[cellkey].values()),
                                     list(self.direct_cell[cellkey].values()))

    @mock.patch("matrix.docker.matrix_converter.MatrixConverter._upload_converted_matrix")
    def test_mtx(self, mock_upload_method):
        """Test the mtx output."""

        args = argparse.Namespace(
            request_id="test_id",
            expression_manifest_key=EXPRESSION_MANIFEST,
            cell_metadata_manifest_key=CELL_MANIFEST,
            gene_metadata_manifest_key=GENE_MANIFEST,
            target_path="test.mtx.zip",
            format="mtx",
            working_dir=".")

        with mock.patch("matrix.docker.matrix_converter.RequestTracker") as mock_request_tracker, \
                mock.patch("os.remove"):
            matrix_converter = MatrixConverter(args)
            matrix_converter.FS = s3fs.S3FileSystem(anon=True)

            mock_request_tracker.return_value.creation_date = "1983-10-11T000000.00Z"

            matrix_converter.run()

        with zipfile.ZipFile("test.mtx.zip") as mtx_output:

            # Check the components of the zip file
            members = mtx_output.namelist()
            self.assertIn("test.mtx/matrix.mtx.gz", members)
            self.assertIn("test.mtx/genes.tsv.gz", members)
            self.assertIn("test.mtx/cells.tsv.gz", members)
            self.assertEqual(len(members), 4)

            # Read in the cell and gene tables. We need both for mtx files
            # since the mtx itself is just numbers and indices.
            mtx_cells = collections.OrderedDict()
            for row in csv.DictReader(io.StringIO(gzip.GzipFile(fileobj=io.BytesIO(
                    mtx_output.read("test.mtx/cells.tsv.gz"))).read().decode()), delimiter='\t'):
                mtx_cells[row["cellkey"]] = row

            mtx_genes = collections.OrderedDict()
            for row in csv.DictReader(io.StringIO(gzip.GzipFile(fileobj=io.BytesIO(
                    mtx_output.read("test.mtx/genes.tsv.gz"))).read().decode()), delimiter='\t'):
                mtx_genes[row["featurekey"]] = row

            # Read the expression values. This is supposed to be aligned with
            # the gene and cell tables
            mtx_expression = scipy.io.mmread(
                gzip.GzipFile(
                    fileobj=io.BytesIO(
                        mtx_output.read("test.mtx/matrix.mtx.gz")))).tocsc()

            self.assertEqual(mtx_expression.shape[1], len(mtx_cells))
            self.assertEqual(len(mtx_cells), len(self.direct_cell))

            col = 0
            for cellkey in mtx_cells:
                mtx_cell_expr = {k: float(v) for k, v in
                                 zip(mtx_genes, mtx_expression[:, col].toarray().ravel()) if v != 0}
                direct_cell_expr = self.direct_expression[cellkey]
                self.assertListEqual(list(mtx_cell_expr.keys()), list(direct_cell_expr.keys()))

                for gene in mtx_cell_expr:
                    self.assertAlmostEqual(mtx_cell_expr[gene], direct_cell_expr[gene], places=2)
                col += 1

            for cellkey in mtx_cells:
                self.assertListEqual(list(mtx_cells[cellkey].values()),
                                     list(self.direct_cell[cellkey].values()))

    @mock.patch("matrix.docker.matrix_converter.MatrixConverter._upload_converted_matrix")
    def test_loom(self, mock_upload_method):
        """Test the loom output."""

        args = argparse.Namespace(
            request_id="test_id",
            expression_manifest_key=EXPRESSION_MANIFEST,
            cell_metadata_manifest_key=CELL_MANIFEST,
            gene_metadata_manifest_key=GENE_MANIFEST,
            target_path="test.loom",
            format="loom",
            working_dir=".")

        with mock.patch("matrix.docker.matrix_converter.RequestTracker") as mock_request_tracker, \
                mock.patch("os.remove"):
            matrix_converter = MatrixConverter(args)
            matrix_converter.FS = s3fs.S3FileSystem(anon=True)

            mock_request_tracker.return_value.creation_date = "1983-10-11T000000.00Z"

            matrix_converter.run()

        test_loom = loompy.connect("test.loom")

        self.assertListEqual(test_loom.ca["CellID"].tolist(),
                             list(self.direct_expression.keys()))

        col = 0
        for cellkey in test_loom.ca["CellID"]:
            loom_cell_expr = {k: v for k, v in
                              zip(test_loom.ra["Accession"], test_loom[:, col]) if v != 0}
            direct_cell_expr = self.direct_expression[cellkey]

            self.assertListEqual(list(loom_cell_expr.keys()), list(direct_cell_expr.keys()))

            for gene in loom_cell_expr:
                self.assertAlmostEqual(loom_cell_expr[gene], direct_cell_expr[gene], places=2)
            col += 1
