# Expression Matrix Service

[![Production Health Check](https://status.data.humancellatlas.org/service/matrix-health-check-prod.svg)](https://matrix.data.humancellatlas.org/)
[![Test Coverage](https://codecov.io/gh/HumanCellAtlas/matrix-service/branch/master/graph/badge.svg)](https://codecov.io/gh/HumanCellAtlas/matrix-service)

## Overview

The Matrix Service consumes data from the [HCA](https://prod.data.humancellatlas.org/)
[Data Store](https://github.com/HumanCellAtlas/data-store) to dynamically generate cell by gene expression matrices.
Users can select cells to include in their matrix by specifying metadata and expression value filters via the API.
Matrices also include metadata per cell for which fields to include can be specified in the POST request. For a quick
example to get started, try this
[Jupytner Notebook vignette](https://github.com/HumanCellAtlas/matrix-service/blob/master/docs/HCA%20Matrix%20Service%20to%20Scanpy.ipynb).

For information on the technical architecture of the service, please see
[Matrix Service Technical Architecture](https://allspark.dev.data.humancellatlas.org/HumanCellAtlas/matrix-service/wikis/Technical-Architecture).

## API: https://matrix.data.humancellatlas.org

The complete API documentation is available [here](https://matrix.data.humancellatlas.org).

### Requesting a matrix
Expression matrices are generated asynchronously for which results are retrieved via a polling architecture.
To request the generation of a matrix, submit a POST request to `/v1/matrix` and receive a job ID. Use this ID to poll
`/v1/matrix/<ID>` to retrieve the status and results of your request. 

When requesting a matrix, users are required to select cells by specifying [metadata/expression data filters](#Filter).
Optionally, they may also specify which [metadata fields](#Fields) to include in the matrix, the
[output format](#Format) and the [feature type](#Feature) to describe. These 4 fields are supplied in the body of the
POST request:
```json
{
  "filter": {},
  "fields": [
    "string"
  ],
  "format": "string",
  "feature": "string"
}
```
#### Filter

To select cells, the API supports a simple yet expressive language for specifying complex metadata and expression data
filters capable of representing nested AND/OR structures as a JSON object. There are two types of filter objects to
achieve this:

*Comparison filter*
```
{
  "op": one of [ =, !=, >, <, >=, <=, in ],
  "field": a metadata filter,
  "value": string or int or list
}
```

*Logical filter*
```
{
  "op": one of [ and, or, not ],
  "value": array of 2 filter objects if op==and|or, filter object if op==not
}
```

These filter types can be recursively nested via the `value` field of a logical filter.

*Filter object examples*

Select all full length cells:
```
...
  "filter": {
    "op": ">=",
    "value": "full length",
    "field": "library_preparation_protocol.end_bias"
  }
...
```

Select all cells from the "Single cell transcriptome analysis of human pancreas" project with at least 3000 genes
detected:
```
...
  "filter": {
    "op": "and",
    "value": [
      {
        "op": "=",
        "value": "Single cell transcriptome analysis of human pancreas",
        "field": "project.project_core.project_short_name"},
      {
        "op": ">=",
        "value": 3000,
        "field": "genes_detected"
      }
    ]
  }
...
```

The list of available filter names is available at `/v1/filters`. To retrieve more information about a specific filter,
GET `/v1/filters/<filter>`.

#### Fields

Users can specify a list of metadata fields to be exported with an expression matrix. The list of available metadata
fields is available at `/v1/fields`. More information about a specific field is available at
`/v1/fields/<field>`.

#### Format

The Matrix Service supports generating matrices in the following 3 formats:

- [.loom](http://loompy.org/) (default)
- [.csv](https://en.wikipedia.org/wiki/Comma-separated_values)
- [.mtx](https://math.nist.gov/MatrixMarket/formats.html)

This list is also available at `/v1/formats` with additional information for a specific format available at
`/v1/formats/<format>`.

#### Feature

The Matrix Service also supports generating cell by transcript matrices in addition to cell by gene matrices. To select
the feature type, specify either `gene` (default) or `transcript` in the POST request. The list of available features is
available at `/v1/features` with additional information for a specific feature available at `/v1/features/<feature>`.

## Developer Getting Started

### Requirements

- Python >= 3.6
- Terraform == 0.11.10

### Developer Environment Setup

1. Clone the ``matrix-service`` repo
1. Create a virtualenv _(recommended)_ 
1. Install requirements
1. Run tests

```bash
git clone git@github.com:HumanCellAtlas/matrix-service.git && cd matrix-service
virtualenv -p python3 venv
. venv/bin/activate
pip install -r requirements-dev.txt --upgrade
make test
```

### Testing

#### Unit tests

To run unit tests, in the top-level directory, run `make test`.

#### Functional tests

Functional tests test the end-to-end functionality of a deployed environment of the service. To set the deployment
environment for which the tests will run against, set the ``DEPLOYMENT_STAGE`` environment variable to an existing
deployment name (``predev`` | ``dev`` | ``integration`` | ``staging`` | ``prod``).

To run functional tests, in the top level directory, run `make functional-test`.

### Debugging

#### Local API server

To deploy the Matrix API/Chalice app from your local machine for development purposes:

```bash
cd chalice
make build && cd ..
./scripts/matrix-service-api.py
```
