## HCA Matrix Service CSV Output

The csv-formatted output from the matrix service is a zip archive that contains four files:

| Filename                         | Description                   |
|--------------------------------- |-------------------------------|
| csv_readme.txt                   | This readme                   |
| <directory_name>/expression.csv  | Expression values             |
| <directory_name>/cells.csv       | Cell metadata                 |
| <directory_name>/genes.csv       | Gene (or transcript) metadata |

### `expression.csv`

The first row is the header, and the first entry in the header is `cellkey`. This is a unique
identifier for the cell and is present in both the expression csv and cell metadata csv. The
remaining header are Ensembl IDs for the genes (or depending on the request, transcripts).

The remaining rows each contain all the expression values for a cell, so cells are rows and
genes are columns. The expression values are meant to a "raw" count, so for SmartSeq2
experiments, this is the `expected_count` field from
[RSEM output](http://deweylab.biostat.wisc.edu/rsem/rsem-calculate-expression.html#output).
For 10x experiments analyzed with Cell Ranger, this is read from the `matrix.mtx` file.

### `cells.csv`

The cell metadata table is oriented like the expression table, where each row represents a cell.
Each column is a different metadata field. Descriptions of some of the metadata fields can be
found at the [HCA Data Browser](https://prod.data.humancellatlas.org/explore/projects).
Additional fields, `genes_detected` for example, are calculated during secondary analysis. Full
descriptions of those fields are forthcoming.

### `genes.csv`
The gene metadata contains basic information about the genes in the count matrix. Each row is a
gene, and each row corresponds to a column in the expression csv. Note that `featurename` is not
unique. 
