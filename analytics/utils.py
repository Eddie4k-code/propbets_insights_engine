def map_data_to_columns(data, columns):
    """
    Map a list of data values to their corresponding column names.
    
    Args:
        data (list): A list of data values.
        columns (list): A list of column names corresponding to the data values.
    
    Returns:
        dict: A dictionary mapping column names to their respective data values.
    """
    if len(data) != len(columns):
        raise ValueError("Data length does not match columns length.")
    return {col: val for col, val in zip(columns, data)}