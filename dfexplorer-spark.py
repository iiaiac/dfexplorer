from pyspark.sql import functions as F
from pyspark.sql import Window
import pandas as pd
import openpyxl

def dfexplorer(spark_df, output_file=None, export=False):
    """
    Analyzes character, numeric, and date fields in a PySpark DataFrame and optionally exports the metrics 
    to an Excel file. Provides summaries for missing data, frequency tables, descriptive statistics, and 
    date aggregations.

    Parameters:
    ----------
    spark_df : pyspark.sql.DataFrame
        The input PySpark DataFrame to analyze.
        
    output_file : str, optional
        Path to the Excel file for saving the analysis results. Required if export=True.
        
    export : bool, default False
        If True, exports the analysis results to an Excel file at `output_file`.
        
    Analysis Details:
    -----------------
    - Character Fields:
        - Record count, missing values, and percentage of missing values.
        - Frequency table of unique values.
        
    - Numeric Fields:
        - Record count, missing values, and percentage of missing values.
        - Descriptive statistics: min, max, mean, mode, and key percentiles (1st, 5th, 10th, 25th, 50th, 75th, 95th, 99th).
        
    - Date Fields:
        - Record count, missing values, and percentage of missing values.
        - Minimum and maximum dates.
        - Monthly record counts (Year-Month).

    Returns:
    -------
    None
        If export=True, saves an Excel file with separate sheets for each summary.
        If export=False, prints summaries to the console.

    Example Usage:
    --------------
    >>> spark_df = spark.createDataFrame([
            ("Alice", 24, "2020-05-01"),
            ("Bob", 27, "2019-03-20"),
            ("Charlie", 35, "2021-01-15"),
            ("David", None, "2022-02-11")
        ], ["Name", "Age", "JoinDate"])

    >>> dfexplorer(spark_df, output_file="output_analysis.xlsx", export=True)
    """
    # Initialize dictionaries for summaries
    char_summary = {}
    num_summary = {}
    date_summary = {}
    freq_tables = {}
    
    # Character Fields Analysis
    char_fields = [field.name for field in spark_df.schema.fields if isinstance(field.dataType, StringType)]
    for field in char_fields:
        num_records = spark_df.count()
        num_missing = spark_df.filter(F.col(field).isNull()).count()
        pct_missing = (num_missing / num_records) * 100
        freq_table = spark_df.groupBy(field).count().orderBy(F.desc("count")).toPandas()
        
        char_summary[field] = {
            "Total Records": num_records,
            "Missing Values": num_missing,
            "Percentage Missing": pct_missing
        }
        freq_tables[field] = freq_table
    
    # Numeric Fields Analysis
    num_fields = [field.name for field in spark_df.schema.fields if isinstance(field.dataType, NumericType)]
    for field in num_fields:
        stats = spark_df.agg(
            F.count(F.col(field)).alias("Total Records"),
            F.count(F.when(F.col(field).isNull(), 1)).alias("Missing Values"),
            (F.count(F.when(F.col(field).isNull(), 1)) / F.count(F.col(field)) * 100).alias("Percentage Missing"),
            F.min(field).alias("Min"),
            F.expr("percentile_approx({}, 0.01)".format(field)).alias("Percentile 1"),
            F.expr("percentile_approx({}, 0.05)".format(field)).alias("Percentile 5"),
            F.expr("percentile_approx({}, 0.10)".format(field)).alias("Percentile 10"),
            F.expr("percentile_approx({}, 0.25)".format(field)).alias("Percentile 25"),
            F.expr("percentile_approx({}, 0.50)".format(field)).alias("Median"),
            F.avg(field).alias("Mean"),
            F.expr("percentile_approx({}, 0.75)".format(field)).alias("Percentile 75"),
            F.expr("percentile_approx({}, 0.90)".format(field)).alias("Percentile 90"),
            F.expr("percentile_approx({}, 0.95)".format(field)).alias("Percentile 95"),
            F.expr("percentile_approx({}, 0.99)".format(field)).alias("Percentile 99"),
            F.max(field).alias("Max")
        ).collect()[0].asDict()
        
        num_summary[field] = stats
    
    # Date Fields Analysis
    date_fields = [field.name for field in spark_df.schema.fields if isinstance(field.dataType, DateType)]
    for field in date_fields:
        num_records = spark_df.count()
        num_missing = spark_df.filter(F.col(field).isNull()).count()
        pct_missing = (num_missing / num_records) * 100
        min_date, max_date = spark_df.select(F.min(field), F.max(field)).first()
        
        date_summary[field] = {
            "Total Records": num_records,
            "Missing Values": num_missing,
            "Percentage Missing": pct_missing,
            "Min Date": min_date,
            "Max Date": max_date
        }
        
        # Monthly counts for each date field
        monthly_counts = spark_df.groupBy(F.date_format(F.col(field), "yyyy-MM").alias("Year-Month")).count().orderBy("Year-Month").toPandas()
        date_summary[f"Monthly Count - {field}"] = monthly_counts

    # Export to Excel if specified
    if export and output_file:
        with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
            # Character summary
            if char_summary:
                pd.DataFrame(char_summary).T.to_excel(writer, sheet_name="Character Summary")
                for field, table in freq_tables.items():
                    table.to_excel(writer, sheet_name=f"Freq_{field}", index=False)
            
            # Numeric summary
            if num_summary:
                pd.DataFrame(num_summary).T.to_excel(writer, sheet_name="Numeric Summary")
            
            # Date summary and monthly counts
            if date_summary:
                date_main_summary = {k: v for k, v in date_summary.items() if not k.startswith("Monthly Count")}
                pd.DataFrame(date_main_summary).T.to_excel(writer, sheet_name="Date Summary")
                for field, table in date_summary.items():
                    if "Monthly Count" in field:
                        table.to_excel(writer, sheet_name=field.replace("Monthly Count - ", ""), index=False)

    else:
        print("Character Summary:", pd.DataFrame(char_summary).T)
        print("Numeric Summary:", pd.DataFrame(num_summary).T)
        print("Date Summary:", pd.DataFrame(date_summary).T)
