library(dplyr)
library(openxlsx)
library(lubridate)

#' dfexplorer
#'
#' Analyzes character, numeric, and date fields in a DataFrame and optionally exports the metrics to an Excel file.
#' Provides summaries for missing data, frequency tables, descriptive statistics, and date aggregations.
#'
#' @param df A data.frame or tibble containing the dataset to analyze.
#' @param output_file A character string specifying the file path to save the analysis (Excel format). Optional if export = FALSE.
#' @param export Logical, default is FALSE. If TRUE, exports the analysis to an Excel file at `output_file`.
#'
#' @details
#' - **Character Fields**: Summarizes record count, missing values, and percentage of missing values. Creates frequency tables for each unique value.
#' - **Numeric Fields**: Provides descriptive statistics including minimum, percentiles (1st, 5th, 10th, 20th, 25th, 50th, 75th, 80th, 90th, 95th, 99th), mean, mode, and maximum.
#' - **Date Fields**: Calculates min and max dates and creates monthly record counts in "YYYY-MM" format.
#'
#' **Exporting**:
#' If `export = TRUE`, the function saves the analysis results to an Excel file with separate sheets for each summary (character, numeric, date).
#'
#' @return If `export = TRUE`, the function saves an Excel file at the specified path and returns NULL.
#' If `export = FALSE`, it prints summaries to the console.
#'
#' @examples
#' # Sample Data
#' df <- data.frame(
#'     Name = c("Alice", "Bob", "Charlie", "David", NA),
#'     Age = c(24, 27, 35, NA, 29),
#'     JoinDate = as.Date(c("2020-05-01", "2019-03-20", "2021-01-15", "2022-02-11", NA))
#' )
#'
#' # Run dfexplorer without exporting
#' dfexplorer(df)
#'
#' # Run dfexplorer with Excel export
#' dfexplorer(df, output_file = "output_analysis.xlsx", export = TRUE)
#'
dfexplorer <- function(df, output_file = NULL, export = FALSE) {
  
  # Initialize empty lists for each summary type
  char_summary <- list()
  num_summary <- list()
  date_summary <- list()
  freq_tables <- list()
  
  # Character Fields Analysis
  char_fields <- df %>% select_if(is.character)
  if (ncol(char_fields) > 0) {
    char_summary <- char_fields %>%
      summarise(across(everything(), list(
        total_records = ~n(),
        missing_values = ~sum(is.na(.)),
        pct_missing = ~mean(is.na(.)) * 100
      ))) %>%
      pivot_longer(everything(), names_to = c("Field", ".value"), names_sep = "_")

    # Frequency tables for each character field
    freq_tables <- lapply(char_fields, function(col) as.data.frame(table(col, useNA = "ifany")))
    names(freq_tables) <- names(char_fields)
  }
  
  # Numeric Fields Analysis
  num_fields <- df %>% select_if(is.numeric)
  if (ncol(num_fields) > 0) {
    num_summary <- num_fields %>%
      summarise(across(everything(), list(
        total_records = ~n(),
        missing_values = ~sum(is.na(.)),
        pct_missing = ~mean(is.na(.)) * 100,
        min = ~min(., na.rm = TRUE),
        percentile_1 = ~quantile(., 0.01, na.rm = TRUE),
        percentile_5 = ~quantile(., 0.05, na.rm = TRUE),
        percentile_10 = ~quantile(., 0.10, na.rm = TRUE),
        percentile_20 = ~quantile(., 0.20, na.rm = TRUE),
        percentile_25 = ~quantile(., 0.25, na.rm = TRUE),
        median = ~median(., na.rm = TRUE),
        mean = ~mean(., na.rm = TRUE),
        mode = ~names(sort(table(.), decreasing = TRUE))[1],
        percentile_75 = ~quantile(., 0.75, na.rm = TRUE),
        percentile_80 = ~quantile(., 0.80, na.rm = TRUE),
        percentile_90 = ~quantile(., 0.90, na.rm = TRUE),
        percentile_95 = ~quantile(., 0.95, na.rm = TRUE),
        percentile_99 = ~quantile(., 0.99, na.rm = TRUE),
        max = ~max(., na.rm = TRUE)
      ))) %>%
      pivot_longer(everything(), names_to = c("Field", ".value"), names_sep = "_")
  }
  
  # Date Fields Analysis
  date_fields <- df %>% select_if(lubridate::is.Date)
  if (ncol(date_fields) > 0) {
    date_summary <- date_fields %>%
      summarise(across(everything(), list(
        total_records = ~n(),
        missing_values = ~sum(is.na(.)),
        pct_missing = ~mean(is.na(.)) * 100,
        min_date = ~min(., na.rm = TRUE),
        max_date = ~max(., na.rm = TRUE)
      ))) %>%
      pivot_longer(everything(), names_to = c("Field", ".value"), names_sep = "_")
    
    # Monthly counts for each date field
    date_counts <- lapply(date_fields, function(col) {
      col <- as.Date(col)
      table(format(col, "%Y-%m"))
    })
    names(date_counts) <- names(date_fields)
  }
  
  # Export to Excel if specified
  if (export && !is.null(output_file)) {
    wb <- createWorkbook()
    
    # Export character summary and frequency tables
    if (length(char_summary) > 0) {
      addWorksheet(wb, "Character Summary")
      writeData(wb, "Character Summary", as.data.frame(char_summary))
      for (field in names(freq_tables)) {
        addWorksheet(wb, paste("Freq", field, sep = "_"))
        writeData(wb, paste("Freq", field, sep = "_"), freq_tables[[field]])
      }
    }
    
    # Export numeric summary
    if (length(num_summary) > 0) {
      addWorksheet(wb, "Numeric Summary")
      writeData(wb, "Numeric Summary", as.data.frame(num_summary))
    }
    
    # Export date summary and counts
    if (length(date_summary) > 0) {
      addWorksheet(wb, "Date Summary")
      writeData(wb, "Date Summary", as.data.frame(date_summary))
      for (field in names(date_counts)) {
        addWorksheet(wb, paste("Count", field, sep = "_"))
        writeData(wb, paste("Count", field, sep = "_"), as.data.frame(date_counts[[field]]))
      }
    }
    
    # Save workbook
    saveWorkbook(wb, output_file, overwrite = TRUE)
  }
}
