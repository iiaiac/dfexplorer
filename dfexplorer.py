import pandas as pd
import numpy as np

def analyze_character_numeric_and_date_fields(df: pd.DataFrame, output_file: str, export: bool = False) -> None:
    """
    Analyzes character, numeric, and date fields in a DataFrame and optionally exports the calculated metrics 
    to an Excel file if `export` is set to True. The function exports individual sheets only if that specific 
    type of field (character, numeric, or date) is present in the DataFrame.

    Parameters:
    ----------
    df : pd.DataFrame
        The input DataFrame to analyze.
        
    output_file : str
        The path to the output Excel file where the analysis results will be saved (only if export=True).

    export : bool, default False
        If True, saves the analysis results to an Excel file at `output_file`.
    
    Analysis Details:
    -----------------
    1. Character Fields:
        - Number of Records: Total count of records in the DataFrame for each field.
        - Number of Missing Values: Count of missing values (NaN) for each character field.
        - Percentage of Missing Values: Percentage of missing values relative to the total records.
        - Frequency Table: Frequency count of each unique value in the character field.

    2. Numeric Fields:
        - Number of Records: Total count of records in the DataFrame for each field.
        - Number of Missing Values: Count of missing values (NaN) for each numeric field.
        - Percentage of Missing Values: Percentage of missing values relative to the total records.
        - Minimum: Minimum value in the numeric field.
        - Percentiles (1st, 5th, 10th, 20th, 25th, 50th, 75th, 80th, 90th, 95th, 99th): Key percentiles of the values.
        - Mean: Average value.
        - Mode: Most frequent value.
        - Maximum: Maximum value.

    3. Date Fields:
        - Number of Records: Total count of records in the DataFrame for each field.
        - Number of Missing Values: Count of missing values (NaN) for each date field.
        - Percentage of Missing Values: Percentage of missing values relative to the total records.
        - Minimum Date: Earliest date.
        - Maximum Date: Latest date.
        - Count by YYYY-MM: Aggregation of record counts by year-month for each date field.

    Exported Excel Sheets (if export=True):
    ---------------------------------------
    - "Character Summary": Summary of metrics for character fields (only if character fields are present).
    - "Freq_<Field Name>": Individual sheet for each character field showing the frequency table.
    - "Numeric Summary": Summary of metrics for numeric fields (only if numeric fields are present).
    - "Date Summary": Summary of metrics for date fields (only if date fields are present).
    - "Count_<Field Name>": Individual sheet for each date field with record counts aggregated by year-month.

    Example Usage:
    --------------
    >>> df = pd.DataFrame({
            'Name': ['Alice', 'Bob', 'Charlie', 'Alice', 'David'],
            'Age': [23, 35, 45, None, 29],
            'Birthdate': pd.to_datetime(['1998-05-10', '1985-10-15', None, '1998-05-10', '1992-07-22'])
        })
    >>> analyze_character_numeric_and_date_fields(df, 'output_analysis.xlsx', export=True)
    """
    # Filter character fields
    char_fields = df.select_dtypes(include='object')
    
    # Prepare lists to store character field metrics
    char_metrics = {
        "Field": [],
        "Number of Records": [],
        "Number of Missing Values": [],
        "Percentage of Missing Values": [],
        "Frequency Table": []
    }
    
    # Calculate metrics for each character field if there are any
    if not char_fields.empty:
        for field in char_fields.columns:
            num_records = len(df)
            num_missing = df[field].isna().sum()
            pct_missing = round((num_missing / num_records) * 100, 2)
            freq_table = df[field].value_counts()
            
            # Append metrics to lists
            char_metrics["Field"].append(field)
            char_metrics["Number of Records"].append(num_records)
            char_metrics["Number of Missing Values"].append(num_missing)
            char_metrics["Percentage of Missing Values"].append(f"{pct_missing}%")
            char_metrics["Frequency Table"].append(freq_table)
        
        # Create a DataFrame from the character field metrics
        char_metrics_df = pd.DataFrame(char_metrics)

    # Filter numeric fields
    numeric_fields = df.select_dtypes(include=[np.number])
    
    # Prepare lists to store numeric field metrics
    numeric_metrics = {
        "Field": [],
        "Number of Records": [],
        "Number of Missing Values": [],
        "Percentage of Missing Values": [],
        "Minimum": [],
        "Percentile 1": [],
        "Percentile 5": [],
        "Percentile 10": [],
        "Percentile 20": [],
        "Percentile 25": [],
        "Percentile 50": [],
        "Mean": [],
        "Mode": [],
        "Percentile 75": [],
        "Percentile 80": [],
        "Percentile 90": [],
        "Percentile 95": [],
        "Percentile 99": [],
        "Maximum": []
    }
    
    # Calculate metrics for each numeric field if there are any
    if not numeric_fields.empty:
        for field in numeric_fields.columns:
            num_records = len(df)
            num_missing = df[field].isna().sum()
            pct_missing = round((num_missing / num_records) * 100, 2)
            minimum = df[field].min()
            percentiles = np.percentile(df[field].dropna(), [1, 5, 10, 20, 25, 50, 75, 80, 90, 95, 99])
            mean = df[field].mean()
            mode = df[field].mode()[0] if not df[field].mode().empty else np.nan
            maximum = df[field].max()
            
            # Append metrics to lists
            numeric_metrics["Field"].append(field)
            numeric_metrics["Number of Records"].append(num_records)
            numeric_metrics["Number of Missing Values"].append(num_missing)
            numeric_metrics["Percentage of Missing Values"].append(f"{pct_missing}%")
            numeric_metrics["Minimum"].append(minimum)
            numeric_metrics["Percentile 1"].append(percentiles[0])
            numeric_metrics["Percentile 5"].append(percentiles[1])
            numeric_metrics["Percentile 10"].append(percentiles[2])
            numeric_metrics["Percentile 20"].append(percentiles[3])
            numeric_metrics["Percentile 25"].append(percentiles[4])
            numeric_metrics["Percentile 50"].append(percentiles[5])
            numeric_metrics["Mean"].append(mean)
            numeric_metrics["Mode"].append(mode)
            numeric_metrics["Percentile 75"].append(percentiles[6])
            numeric_metrics["Percentile 80"].append(percentiles[7])
            numeric_metrics["Percentile 90"].append(percentiles[8])
            numeric_metrics["Percentile 95"].append(percentiles[9])
            numeric_metrics["Percentile 99"].append(percentiles[10])
            numeric_metrics["Maximum"].append(maximum)
        
        # Create a DataFrame from the numeric field metrics
        numeric_metrics_df = pd.DataFrame(numeric_metrics)
    
    # Filter date fields (simplified filter for datetime columns)
    date_fields = df.select_dtypes(include=['datetime64[ns]'])
    
    # Prepare lists to store date field metrics
    date_metrics = {
        "Field": [],
        "Number of Records": [],
        "Number of Missing Values": [],
        "Percentage of Missing Values": [],
        "Minimum Date": [],
        "Maximum Date": [],
        "Count by YYYY-MM": []
    }
    
    # Calculate metrics for each date field if there are any
    if not date_fields.empty:
        for field in date_fields.columns:
            num_records = len(df)
            num_missing = df[field].isna().sum()
            pct_missing = round((num_missing / num_records) * 100, 2)
            min_date = df[field].min()
            max_date = df[field].max()
            
            # Calculate count by YYYY-MM and convert to character format
            count_by_month = df[field].dropna().dt.to_period("M").value_counts().sort_index()
            count_by_month.index = count_by_month.index.astype(str)  # Convert index to string format
            count_by_month.index.name = "Year-Month"  # Rename index to "Year-Month"
            
            # Append metrics to lists
            date_metrics["Field"].append(field)
            date_metrics["Number of Records"].append(num_records)
            date_metrics["Number of Missing Values"].append(num_missing)
            date_metrics["Percentage of Missing Values"].append(f"{pct_missing}%")
            date_metrics["Minimum Date"].append(min_date)
            date_metrics["Maximum Date"].append(max_date)
            date_metrics["Count by YYYY-MM"].append(count_by_month)
        
        # Create a DataFrame from the date field metrics
        date_metrics_df = pd.DataFrame(date_metrics)
    
    # Export all metrics to an Excel file only if export is True and the type exists
    if export:
        with pd.ExcelWriter(output_file) as writer:
            # Write character metrics summary and frequency tables if character fields exist
            if not char_fields.empty:
                char_metrics_df.drop(columns=["Frequency Table"]).to_excel(writer, sheet_name="Character Summary", index=False)
                # Write each frequency table to separate sheets with field name as the first column name
                for i, field in enumerate(char_metrics["Field"]):
                    freq_table = char_metrics["Frequency Table"][i].to_frame(name="Frequency")
                    freq_table.index.name = field  # Set the index name to the field name
                    freq_table.to_excel(writer, sheet_name=f"Freq_{field}")
            
            # Write numeric metrics summary if numeric fields exist
            if not numeric_fields.empty:
                numeric_metrics_df.to_excel(writer, sheet_name="Numeric Summary", index=False)
            
            # Write date metrics summary and counts if date fields exist
            if not date_fields.empty:
                date_metrics_df.drop(columns=["Count by YYYY-MM"]).to_excel(writer, sheet_name="Date Summary", index=False)
                for i, field in enumerate(date_metrics["Field"]):
                    count_by_month = date_metrics["Count by YYYY-MM"][i].to_frame(name="Record Count")
                    count_by_month.to_excel(writer, sheet_name=f"Count_{field}")
