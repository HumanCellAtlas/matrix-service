# Motivation

The Human Cell Atlas (HCA) Data Coordination Platform (DCP) ingests,
processes, and stores single-cell data, making that data easily
available to a wide audience of researchers.
The single-cell data type of greatest interest to most researchers is the expression
matrix, the cell-by-gene expression values that are the starting point for many
downstream analyses.
To support this most commonly requested data type, the DCP offers a specialized
interface, the DCP matrix service, which enables investigators to easily access
and analyze HCA expression data.

# Expression Matrix Interfaces

The native interface exposed by the DCP storage system, along with the quirks of
the DCP's data organization, would make working with the matrices inconvenient.
In contrast, the matrix service enables researchers to seamlessly incorporate HCA
expression data into their existing analysis practices.

As an illustration, those familiar with dataframe manipulation will be able
to work with HCA expression data as they might work with their own local data:

```python
>>> import hca.expression
>>> data = hca.expression.data
>>> data
<hca.expression.Matrix (123456989 cells, 34183 genes) int32>
>>> filtered_data = data[(data["percent_mito"] < .05) & (data["organ"] == "kidney")]
>>> filtered_data
<hca.expression.Matrix (22134 cells, 34183 genes) int32>
>>> import scanpy.api as sc
>>> adata = filtered_data.as_anndata()
>>> sc.pp.normalize_per_cell(adata, counts_per_cell_after=1e4)
...
```

Analysis package developers can integrate their existing work with HCA data:

```R
library(HumanCellAtlas)
library(SingleCellExpression)

filtered.data <- hca %>%
    filter(percent_mito < 0.05) %>%
    filter(organ == "kidney")

sce <- as(filtered.data, "SingleCellExperiment")
...
```

And portal and other interactive tool developers can have their applications
make requests for HCA data with minimal overhead using web best practices.

# Challenges

A number of engineering challenges must be addressed before the interactions
sketched above can be realized.

## Scientific Utility

#### Normalization and batch effect correction.

The DCP combines single-cell data from many different sources. But, batch
effects in single-cell data are particularly strong. Correct analysis of
combined data from the HCA requires careful handling of batch and other
normalization issues. The matrix service could prepare a matrix from multiple
experiments that is simple and easy to use, but without adjustment for
confounding effects such a matrix may only serve to enable false discoveries.

#### Metadata availability.

While much of the engineering work for expression matrices is focused on
storing and delivering the expression values, practical analysis of expression
data depends on metadata.

#### Data formats and schemata

Multiple single-cell analysis tools are available that depend on different
representations of expression matrix data and metadata. These differences limit
interoperability with the matrix service. A small set of community dataframe
standards are required to allow seamless integration in the future.

## Scale

The number of cells with expression data in the HCA DCP will grow by several
orders of magnitude over time. The matrix service must handle this
growth in data volume.

#### Query responsiveness

The matrix service delivers subsets of HCA expression data in response to
queries from users. As the HCA and the complexity of supported queries grows,
the time needed to deliver a matrix could become intolerably long. The matrix
service must implement a scalable, burstable architecture.

#### Distributed computation

Most current tertiary analyses of single-cell expression data can be reasonably
performed on a single computer, but this will change soon. The matrix service
should enable analyses distributed computation over expression data.

#### Data representation and localization

As data volume grows, when and how to localize expression data to a client will
become important considerations. For example, a client should be able to refer
to subsets or operations on HCA data without unexpectedly localizing a huge
amount expression data.

## Expressing Queries

#### Separation of query and matrix interfaces

The DCP data storage system has its own evolving query interface that may or
may not align well with the sorts of query expression matrix consumers want to
make. The matrix service needs to manage the interaction between the two query
interfaces.

#### Query execution ordering

When a client makes a query, the most efficient action may not be to execute
the query immediately but instead batch or delay operations until the client
needs to realize the matrix.

#### Indicating progress and expected running time

Matrix service clients should be aware of the expected resource consumption and
waiting time for a particular request.

# Existing art

Other projects have addressed similar issues.


### [PanGEO](http://pangeo-data.org)

### [Quilt](https://quiltdata.com)
