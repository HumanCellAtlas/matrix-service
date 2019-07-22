import pandas

from matrix.common.query.query_results_reader import QueryResultsReader


class ExpressionQueryResultsReader(QueryResultsReader):
    def load_results(self):
        raise NotImplementedError()

    def load_slice(self, slice_idx):
        """Load expression query results from a slice, yielding the data by a fixed number
        of rows.

        Args:
            slice_idx: Index of the slice to get data for

        Yields:
            DataFrame of expression results slice
        """

        chunksize = 1000000
        part_url = self.manifest["part_urls"][slice_idx]
        expression_table_columns = ["cellkey", "featurekey", "exprvalue"]
        expression_dtype = {"cellkey": "object", "featurekey": "object", "exprvalue": "float32"}

        # Iterate over chunks of the remote file. We have to set a fixed set
        # number of rows to read, but we also want to make sure that all the
        # rows from a given cell are yielded with each chunk. So we are going
        # to keep track of the "remainder", rows from the end of a chunk for a
        # cell the spans a chunk boundary.
        remainder = None
        for chunk in pandas.read_csv(
                part_url, sep="|", names=expression_table_columns,
                dtype=expression_dtype, header=None, chunksize=chunksize):

            # If we have some rows from the previous chunk, prepend them to
            # this one
            if remainder is not None:
                adjusted_chunk = pandas.concat([remainder, chunk], axis=0, copy=False)
            else:
                adjusted_chunk = chunk

            # Now get the rows for the cell at the end of this chunk that spans
            # the boundary. Remove them from the chunk we yield, but keep them
            # in the remainder.
            last_cellkey = adjusted_chunk.tail(1).cellkey.values[0]
            remainder = adjusted_chunk.loc[adjusted_chunk['cellkey'] == last_cellkey]
            adjusted_chunk = adjusted_chunk[adjusted_chunk.cellkey != last_cellkey]

            yield adjusted_chunk

        if remainder is not None:
            yield remainder
