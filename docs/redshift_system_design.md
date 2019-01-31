# Redshift Architecture Design

## Motivation

In an experiment designed to explore the shortcomings of the current design of the Matrix Service
and the advantages of alternate designs, Redshift was determined to be a desired alternative to 
the current architecture. The experiment and an analysis of the factors considered in this
decision can be found in this [doc](https://github.com/HumanCellAtlas/matrix-service/blob/mckinsel-redshift/redshift/alternative_backend_design.md#apache-drill-single-instance).

## Design

The new Redshift architecture will extend the current API to enable users to specify desired
expression data via an SQL metadata query. The API will fire a Lambda to execute the query
against Redshift to retrieve matched expression data. On query completion, the Lambda will kick
off a Batch job to generate the expression matrix in the client's specified format and upload it
to S3. The prepared expression matrices will continue to be made available to the client via a
GET endpoint on the API.

The following subsections propose a design of the new architecture organized by component.

[insert LucidChart architecture diagram]

### ETL (move to dedicated ETL design doc)

The query service and matrix service have identified a common requirement to consume data in the
DSS and represent it in various representations. This process does not only benefit these services,
but is essential to any process consuming data from the DSS. This is the motivation behind the new
ETL library.

The new architecture will implement the ETL library to implement the initialization and syncing of
data between Redshift and the DSS.

### Redshift

#### Schema

#### Query execution

#### Data Initailization

- Initialization script: 
    - Bash script
    - Spins up an EC2 instance
    - Stages data on local filesystem using ETL library functions
        - issues ES queries to ETL to stage all relevant data locally (expression data, relevant metadata)
    - [ETL] Per Bundle Callback: Hit DSS, stage expression data related to bundle
    - [ETL] Final Callback: all files staged -> write PSVs -> upload to staging bucket in S3 -> load S3>Redshift
    
#### DSS Updates

- DSS notification endpoint on Matrix API:
    - create DSS subscriptions for bundle creates and deletes routed to this endpoint
    - on Create event:
        - invoke Lambda (15min timeout)
        - in Lambda, locally stage new bundle via ETL library, transform to PSV, upload to S3 staging bucket -> load S3>Redshift
            - alternatively, ignore S3 staging bucket for notifications and simply update tables in Redshift
    - on Delete event:
        - invoke Lambda (15min timeout)
        - in Lambda, delete all references to deleted bundle in Redshift tables
        
### POST /matrix Lambda

### File Format Conversion Batch Job

The current file format conversion batch job will be updated to transform Redshift Query Results in S3 out
to an expression matrix of the requested file format.

#### Zarr
#### Loom
#### CSV
#### MTX
