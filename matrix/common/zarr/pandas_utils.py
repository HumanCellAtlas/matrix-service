import pandas
from pandas.core.frame import DataFrame
from zarr import Group


def convert_dss_zarr_root_to_subset_pandas_dfs(zarr_root: Group, start_row: int, end_row: int):
    """
    Params:
        zarr_root: instance of Zarr.group
        start_row: beginning row index to filter
        end_row: end row index to filter
    Returns:
        Tuple of two pandas.DataFrames:
            expression_df: subset filtered expression counts
            qc_df: subset filtered qc values
    """
    expression_df = pandas.DataFrame(data=zarr_root.expression[start_row:end_row],
                                     index=zarr_root.cell_id[start_row:end_row],
                                     columns=zarr_root.gene_id[:])
    numeric_qc_df = pandas.DataFrame(data=zarr_root.cell_metadata_numeric[start_row:end_row],
                                     index=zarr_root.cell_id[start_row:end_row],
                                     columns=zarr_root.cell_metadata_numeric_name[:],
                                     dtype="float32")
    string_qc_df = pandas.DataFrame(data=zarr_root.cell_metadata_string[start_row:end_row],
                                    index=zarr_root.cell_id[start_row:end_row],
                                    columns=zarr_root.cell_metadata_string_name[:],
                                    dtype="<U40")
    qc_df = pandas.concat((numeric_qc_df, string_qc_df), axis=1, copy=False)

    return expression_df, qc_df


def apply_filter_to_matrix_pandas_dfs(filter_query: str, exp_df: DataFrame, qc_df: DataFrame):
    """
    Params:
        filter_query: query filter string in pandas notation
        exp_df: unfiltered expression pandas DataFrame
        qc_df: unfiltered qc pandas DataFrame
    Returns:
        Tuple of two pandas.DataFrames:
            filtered_data: the filtered expression counts
            filtered_qcs: the filtered qc values
    """
    matrix = pandas.concat([exp_df, qc_df], axis=1, copy=False)
    filtered_matrix = matrix.query(filter_query)
    filtered_data = filtered_matrix.iloc[:, :exp_df.shape[1]]
    filtered_qcs = filtered_matrix.iloc[:, exp_df.shape[1]:]
    return filtered_data, filtered_qcs
