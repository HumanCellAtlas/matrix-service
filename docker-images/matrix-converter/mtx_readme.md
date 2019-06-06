## HCA Matrix Service MTX Output

The mtx-formatted output from the matrix service is a zip archive that contains four files:

| Filename                         | Description                   |
|--------------------------------- |-------------------------------|
| mtx_readme.txt                   | This readme                   |
| <directory_name>/matrix.mtx.gz   | Expression values             |
| <directory_name>/cells.tsv.gz    | Cell metadata                 |
| <directory_name>/genes.tsv.gz    | Gene (or transcript) metadata |

### `matrix.mtx.gz`

This file contains expression values in the [matrix market exchange
format](https://math.nist.gov/MatrixMarket/formats.html). This is a sparse
format where only the non-zero expression values are recorded. The columns in
this file correspond to cells, and the rows correspond to genes or transcripts.
The column and row indices are aligned with the rows of the cell and gene metadata
TSVs, respectively.

The expression values are meant to be a "raw" count, so for SmartSeq2
experiments, this is the `expected_count` field from
[RSEM output](http://deweylab.biostat.wisc.edu/rsem/rsem-calculate-expression.html#output).
For 10x experiments analyzed with Cell Ranger, this is read from the `matrix.mtx` file that Cell
Ranger produces as its filtered feature-barcode matrix.

### `cells.tsv.gz`

Each row of the cell metadata table represents a cell, and each column is a different metadata
field. Descriptions of some of the metadata fields can be found at the
[HCA Data Browser](https://prod.data.humancellatlas.org/metadata).
Additional fields, `genes_detected` for example, are calculated during secondary analysis. Full
descriptions of those fields are forthcoming.

### `genes.tsv.gz`

The gene metadata contains basic information about the genes in the count matrix. Each row is a
gene, and each row corresponds to the same row in the expression mtx file. Note that
`featurename` is not unique.
