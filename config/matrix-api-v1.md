# Matrix Service API - Version 1

## Background

The HCA matrix service allows users to request expression data from the HCA and receive an
expression matrix along with associated metadata in the format of their choice.

The first version of the matrix service's API focused on integration with the HCA Data Browser. So,
it required that requests for expression data be specified using "bundle ids". "Bundles" are a
concept internal to the HCA's data storage system. Since the data browser is closely integrated with
the storage system, bundle ids were a natural way to specify requests. However, bundles ids make
less sense for external clients. The focus of this API release is to enable requests for expression
data specified in terms preferred by a scientist who is unfamiliar with the internals of the HCA's
data platform.

In practice, this means that this API allows requests based on facts about _cells_ rather
than any storage system details. For example, a user may want cells from a particular tissue or
cells that meet some quality threshold or some conjunction or disjunction of conditions like that.
In addition, as the number of available metadata fields grows, users may want to limit the metadata
in their prepared matrix to a subset of interest.

Finally, the previous version of the API did not allow users to summarize either the data or the
available metadata. This version of the API provides endpoints for users to investigate which
metadata fields and values are present and how many cells are associated with them.

## Requesting a Matrix

Matrix requests are made to the `/matrix` endpoint. The details of the request are contained in a
JSON object in the request body. That object permits four properties: `filters`, `fields`, `format`,
and `feature`. Only `filter` is required, as it is needed to describe the subset of HCA expression
that should be returned.

### Specifying a `filter`

The `filter` property must have a valid _filter object_. Filter objects can take two forms and are
defined recursively.

The first form of filter object compares a metadata value for a cell to a user-supplied value using
an allowed comparison operator:

```json
{
  "op": <comparison operator>,
  "field": <cell metadata field>,
  "value": <user-supplied value>
}
```
For example, to specify a filter for cells with more than 2,000 detected genes, the object would be

```json
{
  "op": ">",
  "field": "genes_detected",
  "value": 2000
}
```

The permitted comparison operators are `==`, `>=`, `<=`, `>`, `<`, and `in`. When `in` is used, the
`value` should be an array.

The second form of filter object is a logical combination of other filter objects:

```json
{
  "op": <logical operator>,
  "value": [<filter objects>, ...]
}
```

For example, to combine the `genes_detected` filter above with a filter on organ, the object would
be

```json
{
  "op": "and",
  "value": [
    {
    "op": ">",
    "field": "genes_detected",
    "value": 2000
    },
    {
    "op": "==",
    "field": "derived_organ_label",
    "value": "pancreas"
    },
  ]
}
```

Permitted logical operators are `and`, `or`, and `not`. If `not` is used, the `value` should have a
single element.

### Specifying a limited set of metadata `fields`

By default, the matrix service will include all available metadata fields in the matrix it prepares
for a request. However, there may be scores of such fields, so a user can request a subset in the
`fields` property of the request body:

```json
{
  "filter": {...},
  "fields": ["derived_organ_label", "project.project_core.project_short_name",
             "library_preparation_protocol.library_construction_method.ontology_label"]
}
```

### Specifying a result `format`

By default, the matrix service will create a [loom](http://linnarssonlab.org/loompy/) file that
contains both expression values and metadata. Users can also request results as zipped `csv` or
`mtx` files via the `format` property.

### Specifying the reference `feature`

Expression matrices can contain counts of different biological entities. By default, the matrix
service will return counts of `genes`, but for some assay types, users can specify `transcripts`
instead.
