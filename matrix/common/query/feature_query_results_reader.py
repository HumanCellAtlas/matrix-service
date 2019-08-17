import pandas

from matrix.common.query.query_results_reader import QueryResultsReader


class FeatureQueryResultsReader(QueryResultsReader):
    def load_results(self):
        """Load the feature metadata table.

        Returns:
            DataFrame of feature metadata. Index is "featurekey"
        """

        gene_table_columns = self._map_columns(self.manifest["columns"])

        dfs = []
        for part_url in self.manifest["part_urls"]:
            df = pandas.read_csv(part_url, sep='|', header=None, names=gene_table_columns,
                                 true_values=["t"], false_values=["f"],
                                 index_col="featurekey")

            dfs.append(df)
        return pandas.concat(dfs)

    def load_slice(self, slice_idx):
        raise NotImplementedError()
