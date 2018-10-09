import pandas
import zarr


def convert_zarr_store_to_pandas_df(zarr_store, concat=False):
        """Get a pandas dataframe of the expression matrix
        Parameters
        ----------
        zarr_store : instance of DSSZarrStore
        concat : bool, optional
            Concatenate the expression and cell metadata into a single dataframe
            (default is False)
        Returns
        -------
        pandas.DataFrame or list of pandas.DataFrame
            DataFrame(s) with expression and metadata values
        """

        matrix_root = zarr.group(store=zarr_store)
        exp_df = pandas.DataFrame(
            data=matrix_root.expression[:],
            index=matrix_root.cell_id[:],
            columns=matrix_root.gene_id[:])
        cell_metadata_df = pandas.DataFrame(
            data=matrix_root.cell_metadata[:],
            index=matrix_root.cell_id[:],
            columns=matrix_root.cell_metadata_name[:])

        if concat:
            return pandas.concat([exp_df, cell_metadata_df], axis=1, copy=False)

        return exp_df, cell_metadata_df
