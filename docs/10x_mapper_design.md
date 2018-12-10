# Adjusting mappers and workers to handle 10x bundles

Recall that in the current driver/mapper/worker/reducer scheme, we have a few
design properties that we want to optimize:

1. *Minimize number of lambda executions.* Each lambda has small chance of
   failing non-deterministically, and we don't handle that very well. Also each
   lambda has some overhead.
2. *Minimize number of parallel writers.* When many workers are writing in
   parallel to a zarr store, there's a lot of lock contention that can
   eventually overwhelm the lock table
3. *Minimize DSS calls.* API calls to the DSS are slow and the distribution of
   response times has a very long, fat tail. We also have to carefully wrap
   every interaction with retry logic.
4. *Never overload a lambda.* If a lambda OOMs or times out, presently our
   whole query breaks. Moreover, even if a lambda completes in, say, 14
   minutes, that's a long time for a user to be waiting.

Once we start handling 10x analysis bundles, we'll have two very different
bundle types:

1. SmartSeq2 - Contains a tiny amount of data. One worker lambda can handle
   lots of bundles. Currently each one handles 100.
2. 10x - Contains lots of data. One worker lambda can handle one "chunk" from a
   bundle, and a bundles may have tens to hundreds of chunks.

## Current Implementation

1. Driver lambda receives a request and resolves a url to a list of bundle
   fqids. It then submit groups of size `bundles_per_worker` to each mapper
   lambda.
2. Mapper lambda receives a list of `bundles_per_worker` fqids. It does nothing
   except submit that list to a worker lambda.
3. Worker lambda receives the fqids, reads them, filters them, and writes them
   to S3.

In the current implementation, we'd violate goal (4) because a worker lambda
might be expected to handle many 10x bundles, which wouldn't work.

## Proposed Implementations

### Maintain current work distribution scheme

The mapper needs to examine the bundles it receives and based on their size,
distribute them to one or more workers. The pseudocode would look like this:

```
bundle_cell_counts = {}
for fqid in bundle_fqids:
   bundle_cell_counts[fqid] = get_bundle_cell_count(fqid)

for bundle_group in group_for_workers(bundle_cell_counts, cells_per_worker):
    lambda.invoke(bundle_group, ...)
```

The risk of this approach is that `get_bundle_cell_count` probably requires a
`GET /bundle` and a `GET /file`, which means a lot of DSS interaction gets put
into the mapper. To address this, we may need to reduce the size of fqid groups
that get passed to the mapper from the driver. This sets us back on goal (1).
And in this set up, each worker cannot receive more bundles than a mapper
receives, so we also lose on (1) and (2). This may not end up being a big deal,
but will require some profiling.

### Store DSS state separately

One option to avoid the many new calls to the DSS would be to store some facts
about bundles outside of the DSS. In the above example, it could mean an
alternative implementation of `get_bundle_cell_count`:

```
def get_bundle_cell_count(fqid):
    try:
        bundle_cell_count = bundle_cell_count_table.lookup(fqid)
    except BundleNotFound:
        bundle_cell_count = ask_dss_cell_count(fqid)
        bundle_cell_count_table.update(fqid, bundle_cell_count)
    return bundle_cell_count
```

where `bundle_cell_count_table` is something like dynamo. This doesn't help the
worst-case performance, but it would probably make average-case much better.
This could also be augmented with a subscription to the DSS that updates
`bundle_cell_count_table`, meaning that cache misses would occur very rarely.
