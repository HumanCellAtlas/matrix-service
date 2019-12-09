import pandas

from matrix.common.query.query_results_reader import QueryResultsReader


class CellQueryResultsReader(QueryResultsReader):
    def load_results(self):

        if self.is_empty:
            return pandas.DataFrame()

        return pandas.concat([self.load_slice(s) for s in range(len(self.manifest['part_urls']))], copy=False)

    def load_slice(self, slice_idx):
        """Load the cell metadata table from a particular result slice.

        Args:
            slice_idx: Index of the slice to get cell metadata for

        Returns:
            dataframe of cell metadata. Index is "cellkey" and other columns are metadata
            fields.
        """
        cell_table_columns = self._map_columns(self.manifest["columns"])
        cell_table_dtype = {c: "category" for c in cell_table_columns}
        cell_table_dtype["genes_detected"] = "uint32"
        cell_table_dtype["total_umis"] = "float64"
        cell_table_dtype["emptydrops_is_cell"] = "object"
        cell_table_dtype["cellkey"] = "object"

        part_url = self.manifest["part_urls"][slice_idx]
        df = pandas.read_csv(
            part_url, sep='|', header=None, names=cell_table_columns,
            dtype=cell_table_dtype, true_values=["t"], false_values=["f"],
            index_col="cellkey")

        return df
