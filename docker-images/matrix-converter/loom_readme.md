## HCA Matrix Service Loom Output

The loom-formatted output from the matrix service is a zip archive that contains two files:

| Filename                         | Description                   |
|--------------------------------- |-------------------------------|
| loom_readme.txt                  | This readme                   |
| <file_name>.loom                 | Loom file with requested data |

The Loom format is documented more fully, along with code samples,
[here](https://linnarssonlab.org/loompy/index.html).

Per Loom
[conventions](https://linnarssonlab.org/loompy/conventions/index.html), columns
in the loom-formatted expression matrix represent cells, and rows represent
genes. The column and row attributes follow Loom conventions where applicable
as well: `CellID` uniquely identifies a cell, `Gene` is a gene name, and
`Accession` is an ensembl gene id.

Descriptions of the remaining metadata fields are available at the 
[HCA Data Browser](https://prod.data.humancellatlas.org/metadata).
