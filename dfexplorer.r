# Function to install and import required packages
install_and_import <- function(package) {
  if (!require(package, character.only = TRUE)) {
    install.packages(package, dependencies = TRUE)
    library(package, character.only = TRUE)
  }
}

# Check and install necessary packages
install_and_import("dplyr")
install_and_import("lubridate")
install_and_import("openxlsx")

#' dfexplorer
#'
#' Analyzes character, numeric, and date fields in a data.frame and optionally exports the metrics 
#' to an Excel file. This function provides detailed summaries of the dataset, including missing values, 
#' frequency distributions, and statistical metrics.
#'
#' @param df A data.frame or tibble containing the dataset to analyze.
#' @param output_file A character string specifying the file path to save the analysis (Excel format). Optional if export = FALSE.
#' @param export Logical, default is FALSE. If TRUE, exports the analysis to an Excel file at `output_file`.
#'
#' @details
#' - **Character Fields**: For each character field, the function computes:
#'   - Total Records: Count of all records.
#'   - Missing Values: Count of missing (NA) values.
#'   - Percentage of Missing Values: Proportion of missing values relative to total records.
#'   - Frequency Table: Table of unique values and their counts.
#'
#' - **Numeric Fields**: For each numeric field, the function calculates:
#'   - Total Records
#'   - Missing Values
#'   - Percentage of Missing Values
#'   - Descriptive statistics: min, max, mean, standard deviation, and mode.
#'   - Percentiles: 1st, 5th, 10th, 25th, 50th, 75th, 95th, and 99th.
#'
#' - **Date Fields**: For each date field, the function provides:
#'   - Total Records
#'   - Missing Values
#'   - Percentage of Missing Values
#'   - Minimum and Maximum Dates.
#'   - Monthly Counts: Count of records grouped by Year-Month.
#'
#' @return If `export = TRUE`, the function saves an Excel file at the specified path and returns NULL.
#' If `export = FALSE`, it returns a list of summary tables for character, numeric, and date fields.
#'
#' @examples
#' # Sample DataFrame
#' df <- data.frame(
#'     Name = c('Alice', 'Bob', 'Charlie', 'David', NA),
#'     Age = c(24, 27, 35, NA, 29),
#'     JoinDate = as.Date(c('2020-05-01', '2019-03-20', '2021-01-15', '2022-02-11', NA))
#' )
#'
#' # Run analysis and export to Excel if needed
#' summary_list <- dfexplorer(df, export = TRUE, output_file = "output_analysis.xlsx")
dfexplorer <- function(df, output_file = NULL, export = FALSE) {
  
  summary_list <- list()
  
  # Character Fields Analysis
  char_fields <- df %>% select_if(is.character)
  if (ncol(char_fields) > 0) {
    char_summary <- char_fields %>%
      summarise(across(everything(), list(total_records = ~n(), 
                                           missing_values = ~sum(is.na(.)), 
                                           pct_missing = ~mean(is.na(.)) * 100)))
    summary_list[["Character Summary"]] <- char_summary
    
    # Frequency tables
    freq_tables <- lapply(char_fields, function(col) as.data.frame(table(col, useNA = "ifany")))
    names(freq_tables) <- names(char_fields)
    summary_list <- c(summary_list, freq_tables)
  }
  
  # Numeric Fields Analysis
  num_fields <- df %>% select_if(is.numeric)
  if (ncol(num_fields) > 0) {
    num_summary <- num_fields %>%
      summarise(across(everything(), list(total_records = ~n(), 
                                           missing_values = ~sum(is.na(.)), 
                                           pct_missing = ~mean(is.na(.)) * 100,
                                           min = ~min(., na.rm = TRUE),
                                           max = ~max(., na.rm = TRUE),
                                           mean = ~mean(., na.rm = TRUE),
                                           median = ~median(., na.rm = TRUE),
                                           mode = ~names(sort(table(.), decreasing = TRUE))[1],
                                           percentile_1 = ~quantile(., 0.01, na.rm = TRUE),
                                           percentile_5 = ~quantile(., 0.05, na.rm = TRUE),
                                           percentile_10 = ~quantile(., 0.10, na.rm = TRUE),
                                           percentile_25 = ~quantile(., 0.25, na.rm = TRUE),
                                           percentile_50 = ~quantile(., 0.50, na.rm = TRUE),
                                           percentile_75 = ~quantile(., 0.75, na.rm = TRUE),
                                           percentile_95 = ~quantile(., 0.95, na.rm = TRUE),
                                           percentile_99 = ~quantile(., 0.99, na.rm = TRUE)
                                           )))
    summary_list[["Numeric Summary"]] <- num_summary
  }
  
  # Date Fields Analysis
  date_fields <- df %>% select_if(lubridate::is.Date)
  if (ncol(date_fields) > 0) {
    date_summary <- date_fields %>%
      summarise(across(everything(), list(total_records = ~n(),
                                           missing_values = ~sum(is.na(.)),
                                           pct_missing = ~mean(is.na(.)) * 100,
                                           min_date = ~min(., na.rm = TRUE),
                                           max_date = ~max(., na.rm = TRUE)))
    summary_list[["Date Summary"]] <- date_summary
    
    # Monthly counts
    date_counts <- lapply(date_fields, function(col) table(format(col, "%Y-%m")))
    names(date_counts) <- names(date_fields)
    summary_list <- c(summary_list, date_counts)
  }
  
  # Export to Excel if specified
  if (export && !is.null(output_file)) {
    wb <- openxlsx::createWorkbook()
    for (name in names(summary_list)) {
      openxlsx::addWorksheet(wb, name)
      openxlsx::writeData(wb, name, summary_list[[name]])
    }
    openxlsx::saveWorkbook(wb, output_file, overwrite = TRUE)
  }
  
  return(summary_list)
}
