# Alternative Matrix Service Backends: <br> _The Sum of All Tickets_

## Motivation

The current design of the matrix service is illustrated in this [attractive
chart](https://github.com/HumanCellAtlas/matrix-service/blob/master/matrix_architecture.svg).
To summarize, queries are processed via a map-reduce-like architecture using
AWS Lambda Functions that read and process data from the HCA DSS. The results
of those queries are placed in S3 and optionally converted to a requested file
format using AWS Batch.

This architecture successfully delivered initial milestones for the Matrix
Service, and it is currently processing requests from the HCA Data Browser.
There are a number of benefits to the current design:

1. *Data management.* All data is in the DSS, so the matrix service has no
   concerns with deletions, versioning, etc. Moreover its additional storage
   costs are limited to cached results.

2. *Burstable.* Infrastructure is spun up in response to requests, so periods
   of inactivity do not incur any infrastructure cost.

3. *Flexible.* Since the matrix service is just running its own python code in
   lambdas, there are few constraints on the kinds of filters it can support.
   If something can be expressed as python code, then the matrix service can
   execute it.


However, a number of drawbacks to the current design have become apparent:

1. *Speed and scaling.* Uncached requests for the ~4500 cell pancreas dataset
   currently take 3-10 minutes to complete. This is driven by DSS latency and
   lambda overhead. Both of those issues will likely worsen as data volume
   increases. This means that clients may face long waits while a matrix is
   prepared, but the long-term vision for the matrix service is to enable
   a nearly interactive UX.

2. *Access patterns.* Since the source of the matrix data is the DSS, access is
   "bundle-major" in that first bundles must be resolved and then contents of
   those bundles must be filtered and transformed. But bundles are
   heterogeneous and rarely map cleanly to the kinds of acceses patterns that
   the matrix service needs to support. So this imposes limitations both in UX
   and efficiency.

3. *Querying.* Initial UX research has revealed that users will want queries
   that do not fit will with the map-reduce architecture. And implementing the
   API and the lambdas ourselves will require us to implement our own query
   language and try to optimize its execution.

4. *Fault tolerance.* Lambda-based architectures have to deal with transient
   lambda failures and retries. Currently a failure and successful retry yields
   corrupted result.

5. *Existing solutions.* The matrix service, at its core, queries tabular data.
   This is not an unusual task, and there are existing tools and services that
   likely can handle the scale of HCA expression data.

6. *DSS contents.* By relying on only data stored within the DSS, many
   improvements to the matrix service UX require reingestion of data or
   reexecution of pipelines. For example, adding a gene annotation could
   require that every single analysis bundle be recomputed.


## Alternatives

One property that distinguishes expression data from the rest of the data in
the DSS is that the expression data is quite small. Some back-of-the-envelope
math suggests that 1 billion cells is ~4.5TB when compressed, excluding
metadata. This is a tiny fraction of the read, alignment, and image data.
The cost of storing that separately is not too high, so matrix service backends
that subscribe to the DSS and store a separate copy of the expression data are
feasible.

### Benchmarking Queries

There are a few queries that can be used to evaluate the performance and
suitability of different backends:

A. Fetch the expression values for all cells from the "1M Immune Cells" project.

This is a large amount of data, ~550,000 cells from a 10x project. This tests
how a backend handles a large data request. Notably, this is ~100x larger
than all the data currently served by the matrix service.

B. Fetch expression values from cells from diseased donors and filter by an
   additional cell property.

This is a filter applied to all cells in the DCP, and it filters by ingest
metadata as well as a per-cell computed metadata.

C. Calculate the average expression of CD24 in non-diseased caucasians.

This is an aggregation over the expression of one gene from a filtered set of
cells.

D. Fetch the titles of publications that include cells expressing CD24.

This query looks for a non-cell-based value using a filter specified by
properties of a cell.

### AWS Redshift

Redshift is AWS's managed data warehouse product. You can load data from S3
and then write SQL queries against it. Redshift handles things like
compression, sorting, and distribution of queries across multiple nodes.

#### Data organization

With Redshift, the expression data and metadata can be loaded into multiple
tables, and joins across those tables enable metadata-based queries. There are
many ways to organize the data, but one possible pattern has a cell table that
links the notion of cell to metadata about cells:

```
CREATE TABLE cell (
    cellkey          VARCHAR(60) NOT NULL,
    projectkey       VARCHAR(60) NOT NULL,
    donorkey         VARCHAR(60) NOT NULL,
    librarykey       VARCHAR(60) NOT NULL,
    barcode          VARCHAR(32),
    is_lucky         BOOLEAN,
    PRIMARY KEY(cellkey),
    FOREIGN KEY(projectkey) REFERENCES project(projectkey),
    FOREIGN KEY(donorkey) REFERENCES donor_organism(donorkey),
    FOREIGN KEY(librarykey) REFERENCES library_preparation(librarykey))
    DISTKEY(cellkey)
    SORTKEY(cellkey, projectkey)
;
```

There currently is no coherent computed cell metadata across assay types, so
this table includes a random `is_lucky` field to test filtering against derived
metadata.

Then much of the metadata can be organized into various other tables:

```
CREATE TABLE donor_organism (
    donorkey           VARCHAR(40) NOT NULL,
    genus_species      VARCHAR(40),
    ethnicity          VARCHAR(40),
    disease            VARCHAR(40),
    development_stage  VARCHAR(40),
    PRIMARY KEY(donorkey))
    DISTSTYLE ALL
    SORTKEY(donorkey)
;

CREATE TABLE library_preparation (
    librarykey                    VARCHAR(40) NOT NULL,
    input_nucleic_acid            VARCHAR(40),
    construction_approach         VARCHAR(40),
    end_bias                      VARCHAR(20),
    strand                        VARCHAR(20),
    PRIMARY KEY(librarykey))
    DISTSTYLE ALL
    SORTKEY(librarykey)
;

CREATE TABLE project (
    projectkey       VARCHAR(60) NOT NULL,
    short_name       VARCHAR(100) NOT NULL,
    title            VARCHAR(300) NOT NULL,
    PRIMARY KEY(projectkey))
    DISTSTYLE ALL
    SORTKEY(projectkey)
;

CREATE TABLE publication (
    projectkey              VARCHAR(60) NOT NULL,
    pub_title               VARCHAR(200) NOT NULL,
    pub_doi                 VARCHAR(40),
    FOREIGN KEY(projectkey) REFERENCES project(projectkey))
    DISTSTYLE ALL
    SORTKEY(projectkey)
;

CREATE TABLE contributor (
    projectkey             VARCHAR(60) NOT NULL,
    cont_name              VARCHAR(150) NOT NULL,
    cont_institution       VARCHAR(150),
    FOREIGN KEY(projectkey) REFERENCES project(projectkey))
    DISTSTYLE ALL
    SORTKEY(projectkey)
;
```

The expression data itself can also be placed into a single table that is
distributed by cell across all the slices of the Redshift cluster:

```
CREATE TABLE expression (
    cellkey          VARCHAR(60) NOT NULL,
    featurekey       VARCHAR(20) NOT NULL,
    exprtype         VARCHAR(10) NOT NULL,
    exrpvalue        REAL NOT NULL,
    FOREIGN KEY(cellkey) REFERENCES cell(cellkey))
    DISTKEY(cellkey)
    COMPOUND SORTKEY(cellkey, featurekey)
;
```

#### Test queries

For query (A), we can find the cells associated with the particular project
shortname and write out all expression values for them. Using `UNLOAD` allows
Redshift to write results in parallel to S3, and `GZIP` compresses the rather
repetitive text file results.

```
UNLOAD ('SELECT c.cellkey, e.featurekey, e.exprtype, e.exrpvalue
FROM expression as e,
     project as p,
     cell as c
WHERE c.projectkey = p.projectkey
  AND c.cellkey = e.cellkey
  AND p.short_name = \'1M Immune Cells\'')
TO 's3://hca-matrix-redshift-results/test1'
iam_role '...'
GZIP;
```

Query (B) is similar, the joins and conditions are just different. Note that
'PATO:0000461' is the HCA's ontology term for "normal".

```
UNLOAD ('SELECT c.cellkey, e.featurekey, e.exrpvalue
FROM expression as e,
     cell as c,
     donor_organism as d
WHERE e.cellkey = c.cellkey
  AND c.donorkey = d.donorkey
  AND c.is_lucky IS TRUE
  AND d.disease <> \'PATO:0000461\'')
TO 's3://hca-matrix-redshift-results/test2'
iam_role '...'
GZIP;
```

For query (C), we can use sql's `AVG` and additional conditions on the cells.
Note that `ENSG00000272398` is the ensembl gene ID for CD24.

```
SELECT AVG(e.exrpvalue)
FROM expression as e,
     cell as c,
     donor_organism as d
WHERE d.ethnicity = 'HANCESTRO:0005'
  AND d.disease = 'PATO:0000461'
  AND e.featurekey = 'ENSG00000272398'
  AND e.exprtype = 'Count'
  AND e.cellkey = c.cellkey
  AND c.donorkey = d.donorkey
;
```

And finally, query (D) can be implemented by a primarily looking at the
`publication` table:

```
SELECT DISTINCT(pub.pub_title)
FROM expression as e,
     cell as c,
     project as p,
     publication as pub
WHERE c.projectkey = p.projectkey
  AND c.cellkey = e.cellkey
  AND pub.projectkey = p.projectkey
  AND e.featurekey = 'ENSG00000272398'
```

Each query was run on a Redshift cluster with 4 `dc2.large` nodes, which costs
$1/hour to operate.

| Query | Time (s) |
|-------|----------|
| A     | 41       |
| B     | 21       |
| C     | 17       |
| D     | 15       |

This is the time to complete the query. The TTFB can be tuned to be quite fast
by setting the size of files that each slice should write to S3.

### Apache Drill (Single Instance)

Given the modest size of expression data (~4.5TB), it may be sufficient to opt
for simpler infrastructure and store all data on a single instance. Apache Drill
is a distributed query engine that offers columnar query execution on schema-free
data models. Drill also supports querying from multiple, aggregate datasources
such as Hadoop, S3 and local files via standard APIs including ANSI SQL. Though
Drill is capable of scaling to a cluster of nodes, we are interested in Drill on
a single instance as an architecturally lightweight solution.

#### Data Organization

For consistency, the same data schemas as [above](#aws-redshift) will be used 
to benchmark query performance. Since the design uses a single node, the same
considerations addressing data distribution do not apply.

Drill supports querying various file formats including plaintext files (CSV,
TSV, PSV) as well as Hadoop file formats such as `parquet`. To leverage
Drill's columnar query execution, expression data will be stored in `parquet`
files on disk - one file per table.

#### EC2 Instance

Amazon offers a class of storage-optimized EC2 instances (I3) that provide low
latency I/O to large NVMe SSDs. The following table describes the instance used
to perform test queries and benchmarking.

| EC2 AMI | vCPUs | Memory | Storage |  Cost |
|---------|-------|--------|---------|-------|
|[i3.4xlarge](https://www.ec2instances.info/?selected=i3.4xlarge)|  16   |122GiB  |3.8TB NVMe SSD|$1.248/hr (OD) $0.857/hr (R)|

The query performance of a single instance is largely dependent on the level of
parallelization it can achieve. This is bounded by the number of cores
(translated from vCPUs). This test uses the 3rd largest I3 instance with 16
cores, following 32 (i3.8xlarge) and 64 cores (i3.16xlarge) that may be an option
if necessary.

For the anticipated size of data and query loads, memory and storage are
manageable given the two larger I3 instances.

#### Test Queries

For reasonable results, the same queries as [above](#aws-redshift) will be used
to perform benchmarking. They are rewritten here as SQL queries against Drill.

A) Find full expression matrix from the "1M Immune Cells" project:
```
SELECT c.cellkey, e.featurekey, e.exprtype, e.exprvalue
FROM dfs.`/mnt/data/expression_100m.parquet` AS e
    JOIN dfs.`/mnt/data/cell.parquet` AS c ON e.cellkey=c.cellkey
    JOIN dfs.`/mnt/data/project.parquet` AS p ON e.projectkey=p.projectkey
WHERE p.short_name='1M Immune Cells';
```

B) Find the full expression matrix from lucky cells from diseased donors:
```
SELECT c.cellkey, e.featurekey, e.exprvalue
FROM dfs.`/mnt/data/expression_100m.parquet` AS e
    JOIN dfs.`/mnt/data/cell.parquet` AS c ON e.cellkey=c.cellkey
    JOIN dfs.`/mnt/data/donor.parquet` AS d ON e.donorkey=d.donorkey
WHERE c.cell_is_lucky IS TRUE
    AND d.donor_disease='PATO:0000461';
```

C) Find the average expression of CD24 in non-diseased caucasians:
```
SELECT AVG(e.exprvalue)
FROM dfs.`/mnt/data/expression_100m.parquet` AS e
    JOIN dfs.`/mnt/data/donor.parquet` AS d ON e.donorkey=d.donorkey
WHERE d.ethnicity='HANCESTRO:0005'
    AND d.disease='PATO:0000461'
    AND e.expr_featurekey='ENSG00000272398'
    AND e.expr_exprtype='Count';
```

D) Find the paper title for all cells expressing CD24:
```
SELECT DISTINCT(pub.pub_title)
FROM dfs.`/mnt/data/expression_100m.parquet` AS e
    JOIN dfs.`/mnt/data/cell.parquet` AS c ON e.cellkey=c.cellkey
    JOIN dfs.`/mnt/data/project.parquet` AS p ON e.projectkey=p.projectkey
    JOIN dfs.`/mnt/data/publication.parquet` AS pub ON p.key=pub.projectkey
WHERE e.featurekey='ENSG00000272398';
```

#### Results

**Important note**: These results do not reflect the full dataset used in the
[Redshift experiment](#aws-redshift). A parquet file for the full expression
table (~400 million rows) was not generated. Instead, these queries used half
(~200 million rows) of the expression table. Otherwise, the data are the same.

| Query | Time |
|-------|------|
| A |  2min  |
| B |  5min |
| C |  30s |
| D |  15s |

These results suggest Drill's performance on a single node may be passable given
optimizations. Query A and B are unacceptable, however by increasing
parallelization using the larger two i3 instances, they may improve to an acceptable
range. These instances cost $3.97/hr and $7.93/hr respectively (on demand),
significantly more expensive than alternatives.
