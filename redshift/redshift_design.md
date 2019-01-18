# Alternative Matrix Service Backends

## Motivation

The long-term vision for the matrix service is to enable fast and seamless
integration of HCA expression data into tertiary analysis workflows and client
portals. Accomplishing that probably means that requests for expression data
need to return quickly, on the order of seconds of TTFB. Currently, uncached
requests take ~10 minutes to complete. And since there are only now a small
number of cells available in the DSS, these requests are rather small.

This means that request latency for the matrix service is 2-4 orders of
magnitude too high. There are improvements to DSS response times planned, but
it is likely that those will not be enough achieve the matrix service's
response time goals.

## Current Design

The matrix service currently serves requests by launching cascading AWS Lambda
functions that read data from the DSS. Then optionally, a batch job is launched
that handles file format conversion.

This design has a few nice properties:

1. The matrix service has no independent copy of the data. So it doesn't need
   to handle anything like propagating deletions, versioning, etc.
2. Infrastructure is only spun up in response to requests, which matches the
   bursty pattern of requests the matrix service will likely get.

However, there are some serious issues:

1. Requires lots of requests to the DSS. The distribution of DSS request
   latency has a very long tail.
2. Requires launching many lambda functions. The DCP AWS account is often
   subject to throttling.
3. Fault tolerance is poor. A sporadic error in a lambda function can
   corrupt the result of an entire query.
4. Heavy dependence of secondary analysis pipelines to produce data in a
   particular format and schema.


## Alternatives

One property that distinguished expression data from the rest of the data in
the DSS is that the expression data is quite small. Some back-of-the-envelope
math suggests that 1 billion cells is ~4.5TB. The cost of storing that
separately is not too high, so some of the advantages of the current approach
are diminished.

Thus, it's worth investigating some approaches that are more directly tailored
to the kinds of requests that the matrix service is going to get.

### Redshift

Redshift is AWS's data warehouse product. You can load data from S3 and then
write SQL queries against it. Redshift handles things like compression and
distributing queries across multiple nodes.

The expression values themselves can be organized into one big fact table:

```
expression (
    expr_projectkey       VARCHAR(60) NOT NULL SORTKEY,
    expr_cellkey          VARCHAR(60) NOT NULL DISTKEY,
    expr_donorkey         VARCHAR(60),
    expr_librarykey       VARCHAR(60),
    expr_featurekey       VARCHAR(20) NOT NULL,
    expr_exprtype         VARCHAR(10) NOT NULL,
    expr_value            NUMERIC(12, 2) NOT NULL
)
```

And then much of the metadata can be organized into various dimension tables:

```
cell (
    cell_key              VARCHAR(60) NOT NULL SORTKEY,
    cell_barcode          VARCHAR(32),
    cell_is_lucky         BOOLEAN
)

donor_organism (
    donor_key                VARCHAR(40) NOT NULL SORTKEY,
    donor_genus_species      VARCHAR(40),
    donor_ethnicity          VARCHAR(40),
    donor_disease            VARCHAR(40),
    donor_development_stage  VARCHAR(40)
)

library_preparation (
    library_key                    VARCHAR(40) NOT NULL SORTKEY,
    library_input_nucleic_acid     VARCHAR(40),
    library_construction_approach  VARCHAR(40),
    library_end_bias               VARCHAR(20),
    library_strand                 VARCHAR(20)
)

project (
    proj_key              VARCHAR(60) NOT NULL SORTKEY,
    proj_short_name       VARCHAR(100) NOT NULL,
    proj_title            VARCHAR(300) NOT NULL
)

publication (
    proj_key              VARCHAR(60) NOT NULL SORTKEY,
    pub_title             VARCHAR(200) NOT NULL,
    pub_doi               VARCHAR(40)
)

contributor (
    proj_key              VARCHAR(60) NOT NULL SORTKEY,
    cont_name             VARCHAR(150) NOT NULL,
    cont_institution      VARCHAR(150)
)
```

The tables above are just illustrative examples, and this overlaps a great
deal with the work being done for the query service.

### Big EC2 Instance


### Test queries


Find full expression matrix from the "1M Immune Cells" project.
```
SELECT e.expr_cellkey, e.expr_featurekey, e.expr_value
FROM expression as e,
     project as p
WHERE e.expr_projectkey = p.proj_key
  AND p.proj_short_title = '1M Immune Cells'
```

Find the full expression matrix from lucky cells from diseased donors.
```
SELECT e.expr_cellkey, e.expr_featurekey, e.expr_value
FROM expression as e,
     cell as c,
     donor_organism as d
WHERE e.expr_cellkey = c.cellkey
  AND c.is_lucky IS TRUE
  AND d.donor_disease <> 'PATO:0000461'
```
Find the average expression of CD24 in non-diseased caucasians.
```
SELECT AVG(e.expr_value)
FROM expression as e,
     donor_organism
WHERE donor_organism.donor_ethnicity = 'HANCESTRO:0005'
  AND donor_organism.donor_disease = 'PATO:0000461'
  AND e.expr_featurekey = 'ENSG00000272398'
  AND e.expr_exprtype = 'Count'
  AND e.expr_donorkey = donor_organism.donor_key
```

Find the paper title for all cells expressing CD24.
``` 
SELECT DISTINCT(pub.pub_title)
FROM expression as e,
     project as p,
     publication as pub
WHERE e.expr_projectkey = p.proj_key
  AND p.proj_key = pub.proj_key
  AND e.expr_featurekey = 'ENSG00000272398'
```
