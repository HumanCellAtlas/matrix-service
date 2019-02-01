# Redshift Architecture Design

## Motivation

An experiment designed to explore the shortcomings of the Matrix Service's current design
and the advantages of alternate designs chose Redshift as a suitable alternative current
architecture. The experiment and an analysis of the factors considered are captured in this
[doc](https://github.com/HumanCellAtlas/matrix-service/blob/mckinsel-redshift/redshift/alternative_backend_design.md#apache-drill-single-instance).

## Design

The new Redshift architecture will extend the current API to enable users to specify desired
expression data via an SQL metadata query. The API will fire a Lambda to execute the query
against Redshift to retrieve matched expression data. On query completion, the Lambda will kick
off a Batch job to generate the expression matrix in the client's specified format and upload it
to S3. The prepared expression matrices will continue to be made available to the client via a
GET endpoint on the API.

The following subsections propose a design of the new architecture organized by component.

[insert LucidChart architecture diagram]

### Redshift

#### Schema

#### Data Initailization

This section outlines the first-time initialization process for loading expression data and relevant metadata from the
DSS into Redshift. The process will be implemented as a Python script running on an EC2 instance that will be kicked off
and provisioned by a local bash script. In Python, [dcplib/ETL]() will be implemented to download all data required in
Redshift from the DSS. The ETL library allows the client to supply two callbacks during extraction: 1) on download of a
single bundle and 2) on completion of all requested data. The first callback will be responsible for downloading all
auxiliary data related to the bundle (expression data). The second callback will be responsible for transforming all
downloaded data to PSV files representing Redshift tables. Once transformed, these files will be uploaded to an S3
bucket to be consumed by Redshift. Once uploaded, a [COPY query](https://docs.aws.amazon.com/redshift/latest/dg/t_Loading-data-from-S3.html)
will be issued to Redshift to load all data from the S3 bucket into Redshift tables.

#### DSS Updates

Data in Redshift will stay up-to-date with the DSS via DSS subscriptions. A new endpoint on the Matrix Service API will
subscribe to create and delete events in the DSS. These events will be processed in a Lambda invoked by the endpoint to
bypass API Gateway's 30s timeout. For create events, the Lambda will download the new bundle via ETL, transform relevant
data into PSV format and INSERT the new rows into their tables in Redshift. For delete events, a DELETE will be issued
to all tables and rows relating to the deleted bundle. Handling delete events will require [the schema](#schema) to
be such that all entries to be tied to a bundle UUID.

_Caveat:_ This design does not maintain data integrity in the S3 staging bucket used during
[Data Initialization](#data-initailization). The rationale for this is the design and time complexity to handle
single bundle deletes across raw text files stored in S3. Supporting this would require scanning all files in the S3
staging bucket which is unreasonable.
        
### POST /matrix Lambda

### File Format Conversion Batch Job

The current file format conversion batch job will be updated to transform Redshift Query Results in S3 out
to an expression matrix of the requested file format.

#### Zarr
#### Loom
#### CSV
#### MTX
