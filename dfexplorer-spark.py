from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, NumericType, DateType
import pandas as pd


def install_and_import(package):
    """Install and import the specified package."""
    try:
        __import__(package)
    except ImportError:
        subprocess.check_call([sys.executable, "-m", "pip", "install", package])
        __import__(package)


# Check and install necessary packages
install_and_import("pyspark")
install_and_import("pandas")
install_and_import("openpyxl")


def dfexplorer(spark_df, output_file=None, export=False):
    """
    Analyzes character, numeric, and date fields in a PySpark DataFrame and optionally exports the metrics
    to an Excel file. This function provides detailed summaries of the dataset, including missing values,
    frequency distributions, and statistical metrics.

    Parameters:
    ----------
    spark_df : pyspark.sql.DataFrame
        The input PySpark DataFrame containing the dataset to analyze.

    output_file : str, optional
        Path to the Excel file for saving the analysis results. Required if export=True.

    export : bool, default False
        If True, the function saves the analysis results to an Excel file at output_file.
        If False, the results are returned in a structured format without exporting.

    Analysis Details:
    -----------------
    - **Character Fields**: For each character field, the function computes:
        - Total Records: Count of all records.
        - Missing Values: Count of missing (null) values.
        - Percentage of Missing Values: Proportion of missing values relative to total records.
        - Frequency Table: Table of unique values and their counts.

    - **Numeric Fields**: For each numeric field, the function calculates:
        - Total Records
        - Missing Values
        - Percentage of Missing Values
        - Descriptive statistics: min, max, mean, mode, and key percentiles.

    - **Date Fields**: For each date field, the function provides:
        - Total Records
        - Missing Values
        - Percentage of Missing Values
        - Minimum Date: The earliest date in the field.
        - Maximum Date: The latest date in the field.
        - Monthly Counts: The count of records grouped by month.

    Returns:
    -------
    dict
        A dictionary containing summary tables for character, numeric, and date fields.
        Each key corresponds to a summary type and can include:
        - "Character Summary": A DataFrame summarizing character field statistics.
        - "Numeric Summary": A DataFrame summarizing numeric field statistics.
        - "Date Summary": A DataFrame summarizing date field statistics.
        - Frequency tables for each character field.

    Example Usage:
    --------------
    >>> spark = SparkSession.builder.appName("dfexplorer").getOrCreate()
    >>> data = [("Alice", 24, "2020-05-01"), ("Bob", 27, "2019-03-20")]
    >>> df = spark.createDataFrame(data, ["Name", "Age", "JoinDate"])
    >>> summary = dfexplorer(df, export=False)
    """

    summary_dict = {}

    # Character Fields Analysis
    char_fields = [
        field.name
        for field in spark_df.schema.fields
        if isinstance(field.dataType, StringType)
    ]
    for field in char_fields:
        num_records = spark_df.count()
        num_missing = spark_df.filter(F.col(field).isNull()).count()
        pct_missing = (num_missing / num_records) * 100
        freq_table = spark_df.groupBy(field).count().orderBy(F.desc("count")).toPandas()

        summary_dict[field] = {
            "Total Records": num_records,
            "Missing Values": num_missing,
            "Percentage Missing": pct_missing,
            "Frequency Table": freq_table,
        }

    # Numeric Fields Analysis
    num_fields = [
        field.name
        for field in spark_df.schema.fields
        if isinstance(field.dataType, NumericType)
    ]
    num_summary = {}
    for field in num_fields:
        stats = spark_df.agg(
            F.count(F.col(field)).alias("Total Records"),
            F.count(F.when(F.col(field).isNull(), 1)).alias("Missing Values"),
            (
                F.count(F.when(F.col(field).isNull(), 1)) / F.count(F.col(field)) * 100
            ).alias("Percentage Missing"),
            F.min(field).alias("Min"),
            F.max(field).alias("Max"),
            F.avg(field).alias("Mean"),
            F.expr("percentile_approx({}, 0.50)".format(field)).alias("Median"),
            F.expr("percentile_approx({}, 0.01)".format(field)).alias("Percentile 1"),
            F.expr("percentile_approx({}, 0.05)".format(field)).alias("Percentile 5"),
            F.expr("percentile_approx({}, 0.10)".format(field)).alias("Percentile 10"),
            F.expr("percentile_approx({}, 0.25)".format(field)).alias("Percentile 25"),
            F.expr("percentile_approx({}, 0.75)".format(field)).alias("Percentile 75"),
            F.expr("percentile_approx({}, 0.95)".format(field)).alias("Percentile 95"),
            F.expr("percentile_approx({}, 0.99)".format(field)).alias("Percentile 99"),
        ).first()

        num_summary[field] = stats.asDict()

    summary_dict["Numeric Summary"] = pd.DataFrame(num_summary)

    # Date Fields Analysis
    date_fields = [
        field.name
        for field in spark_df.schema.fields
        if isinstance(field.dataType, DateType)
    ]
    for field in date_fields:
        num_records = spark_df.count()
        num_missing = spark_df.filter(F.col(field).isNull()).count()
        pct_missing = (num_missing / num_records) * 100
        date_summary = spark_df.agg(
            F.count(F.col(field)).alias("Total Records"),
            F.count(F.when(F.col(field).isNull(), 1)).alias("Missing Values"),
            (
                F.count(F.when(F.col(field).isNull(), 1)) / F.count(F.col(field)) * 100
            ).alias("Percentage Missing"),
            F.min(field).alias("Min Date"),
            F.max(field).alias("Max Date"),
        ).first()

        summary_dict[field] = date_summary.asDict()

        # Monthly counts
        monthly_counts = (
            spark_df.groupBy(F.date_format(field, "yyyy-MM")).count().toPandas()
        )
        summary_dict[f"Monthly Count - {field}"] = monthly_counts

    # Export to Excel if specified
    if export and output_file:
        with pd.ExcelWriter(output_file) as writer:
            for name, data in summary_dict.items():
                if isinstance(data, pd.DataFrame):
                    data.to_excel(writer, sheet_name=name)
                else:
                    pd.DataFrame([data]).to_excel(writer, sheet_name=name)

    return summary_dict
