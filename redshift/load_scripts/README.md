## Redshift load scripts

These are the scripts used to prepare data for loading into redshift. They're
pretty clunky/experimental right now.

The first step is to download all the bundles you want from the DSS. Everything
it super slow if you try to do it remotely. Then, run each of the adjacent
python scripts to create a set of pipe-separated values files. `expression.py`
needs to be run once per bundle, and the results concanated.

Once the PSVs are created, copy them to S3 and run some SQL statements to
populate the tables:

```
COPY project FROM 's3://hca-matrix-redshift-staging/project' iam_role '...';
COPY donor_organism FROM 's3://hca-matrix-redshift-staging/donor' iam_role '...';
COPY library_preparation FROM 's3://hca-matrix-redshift-staging/library' iam_role '...';
COPY publication FROM 's3://hca-matrix-redshift-staging/publication' iam_role '...';
COPY contributor FROM 's3://hca-matrix-redshift-staging/contributor' iam_role '...';
COPY analysis FROM 's3://hca-matrix-redshift-staging/analysis' iam_role '...';
COPY cell FROM 's3://hca-matrix-redshift-staging/cell' iam_role '...' COMPUPDATE ON;
COPY expression FROM 's3://hca-matrix-redshift-staging/expression' iam_role '...' GZIP COMPUPDATE ON COMPROWS 10000000;
COPY feature FROM 's3://hca-matrix-redshift-staging/feature' iam_role '...' COMPUPDATE ON;
```
