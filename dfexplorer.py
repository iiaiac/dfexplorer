import sys
import subprocess

def install_and_import(package):
    """
    Install and import the specified package if it is not already installed.

    Parameters:
    ----------
    package : str
        The name of the package to install and import.
    """
    try:
        __import__(package)
    except ImportError:
        subprocess.check_call([sys.executable, "-m", "pip", "install", package])
        __import__(package)

# Check and install necessary packages
install_and_import("pandas")
install_and_import("numpy")
install_and_import("openpyxl")

import pandas as pd
import numpy as np

def dfexplorer(df: pd.DataFrame, output_file: str = None, export: bool = False) -> dict:
    """
    Provides an in-depth exploratory analysis of a DataFrame's character, numeric, and date fields.
    This function calculates summary statistics, missing values, frequency tables, and 
    optional exports to an Excel file with separate sheets for each summary type.

    Parameters:
    ----------
    df : pd.DataFrame
        The input DataFrame to analyze.

    output_file : str, optional
        The path to the Excel file where analysis results will be saved if `export` is True.

    export : bool, default False
        If True, the function exports the analysis results to an Excel file at `output_file`.

    Returns:
    -------
    dict
        A dictionary containing summary DataFrames for character, numeric, and date fields. 
        Each summary includes details on missing values and relevant statistics.

    Notes:
    ------
    - Character Fields: Provides missing value counts and frequency distribution.
    - Numeric Fields: Provides descriptive statistics (min, max, mean, percentiles).
    - Date Fields: Provides range and monthly counts.
    - Optionally saves results to an Excel file with each summary on a separate sheet.

    Example:
    --------
    >>> df = pd.DataFrame({
            'Name': ['Alice', 'Bob', 'Charlie', 'David', None],
            'Age': [24, 27, 35, None, 29],
            'JoinDate': pd.to_datetime(['2020-05-01', '2019-03-20', '2021-01-15', '2022-02-11', None])
        })
    >>> summary = dfexplorer(df, export=False)
    >>> print(summary['Character Summary'])
    """
    # Initialize the dictionary to hold summaries for character, numeric, and date fields
    summary_dict = {}

    # Character Fields Analysis
    # -------------------------
    char_fields = df.select_dtypes(include="object")
    if not char_fields.empty:
        char_summary = {
            "Total Records": df.shape[0],
            "Missing Values": char_fields.isnull().sum(),
            "Percentage Missing": (char_fields.isnull().sum() / df.shape[0]) * 100,
        }
        summary_dict["Character Summary"] = pd.DataFrame(char_summary)

        # Frequency tables for each character field
        freq_tables = {
            col: df[col].value_counts().to_frame("Frequency") for col in char_fields
        }
        summary_dict.update(freq_tables)
    
    # Numeric Fields Analysis
    # -----------------------
    num_fields = df.select_dtypes(include=np.number)
    if not num_fields.empty:
        num_summary = {
            "Total Records": df.shape[0],
            "Missing Values": num_fields.isnull().sum(),
            "Percentage Missing": (num_fields.isnull().sum() / df.shape[0]) * 100,
            "Min": num_fields.min(),
            "Max": num_fields.max(),
            "Mean": num_fields.mean(),
            "Median": num_fields.median(),
            "Mode": num_fields.mode().iloc[0],
            "Percentile 25": num_fields.quantile(0.25),
            "Percentile 50": num_fields.quantile(0.50),
            "Percentile 75": num_fields.quantile(0.75),
            "Percentile 95": num_fields.quantile(0.95),
            "Percentile 99": num_fields.quantile(0.99),
        }
        summary_dict["Numeric Summary"] = pd.DataFrame(num_summary)

    # Date Fields Analysis
    # --------------------
    date_fields = df.select_dtypes(include="datetime")
    if not date_fields.empty:
        date_summary = {
            "Total Records": df.shape[0],
            "Missing Values": date_fields.isnull().sum(),
            "Percentage Missing": (date_fields.isnull().sum() / df.shape[0]) * 100,
            "Min Date": date_fields.min(),
            "Max Date": date_fields.max(),
        }
        summary_dict["Date Summary"] = pd.DataFrame(date_summary)

        # Monthly counts for each date field
        for col in date_fields:
            monthly_counts = (
                date_fields[col].dt.to_period("M").value_counts().sort_index()
            )
            summary_dict[f"Monthly Count - {col}"] = monthly_counts.to_frame("Count")

    # Exporting to Excel
    # ------------------
    # If export is True, save all summaries into separate sheets within an Excel file.
    if export and output_file:
        with pd.ExcelWriter(output_file) as writer:
            for sheet_name, data in summary_dict.items():
                data.to_excel(writer, sheet_name=sheet_name)

    # Return the summary dictionary for further inspection or use in other code
    return summary_dict
