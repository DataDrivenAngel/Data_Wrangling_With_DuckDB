# Import required libraries
library(dplyr)
library(readr)
library(microbenchmark)
library(ggplot2)
library(DBI)
library(duckdb)
#library(duckplyr)
library(data.table)

# Import the DC_trees.csv file
dc_trees <- read.csv("data/DC_trees.csv")
for (i in 1:9) {
  dc_trees <- rbind(dc_trees, read.csv("data/DC_trees.csv"))
}

# Convert to data.table
dc_trees_dt <- as.data.table(dc_trees)

# Convert to data.frame
dc_trees_df <- as.data.frame(dc_trees)

# Function to calculate statistics on tree HEIGHT using dplyr
calculate_tree_height_stats_dplyr <- function() {
  dc_trees %>%
    summarize(
      mean_height = mean(HEIGHT, na.rm = TRUE),
      median_height = median(HEIGHT, na.rm = TRUE),
      min_height = min(HEIGHT, na.rm = TRUE),
      max_height = max(HEIGHT, na.rm = TRUE),
      sd_height = sd(HEIGHT, na.rm = TRUE)
    )
}

# Function to get top 10 common names using dplyr
get_top_10_common_names_dplyr <- function() {
  dc_trees %>%
    group_by(COMMON_NAME) %>%
    summarize(count = n()) %>%
    arrange(desc(count)) %>%
    head(10)
}

# Create a DuckDB connection and copy the data
con <- dbConnect(duckdb::duckdb(), ":memory:")
dbWriteTable(con, "dc_trees", dc_trees)

# Function to calculate statistics on tree HEIGHT using DuckDB
calculate_tree_height_stats_duckdb <- function() {
  dbGetQuery(con, "
    SELECT 
      AVG(HEIGHT) AS mean_height,
      MEDIAN(HEIGHT) AS median_height,
      MIN(HEIGHT) AS min_height,
      MAX(HEIGHT) AS max_height,
      STDDEV(HEIGHT) AS sd_height
    FROM dc_trees
  ")
}

# Function to get top 10 common names using DuckDB
get_top_10_common_names_duckdb <- function() {
  dbGetQuery(con, "
    SELECT 
      COMMON_NAME, COUNT(*) AS count
    FROM dc_trees
    GROUP BY COMMON_NAME
    ORDER BY count DESC
    LIMIT 10
  ")
}

# Function to calculate statistics on tree HEIGHT using duckplyr
calculate_tree_height_stats_duckplyr <- function() {
  dc_trees %>% 
    duckplyr::as_duckplyr_df() %>%
    summarize(
      mean_height = mean(HEIGHT, na.rm = TRUE),
      median_height = median(HEIGHT, na.rm = TRUE),
      min_height = min(HEIGHT, na.rm = TRUE),
      max_height = max(HEIGHT, na.rm = TRUE),
      sd_height = sd(HEIGHT, na.rm = TRUE)
    )
}

# Function to get top 10 common names using duckplyr
get_top_10_common_names_duckplyr <- function() {
  dc_trees %>% 
    duckplyr::as_duckplyr_df() %>%
    group_by(COMMON_NAME) %>%
    summarize(count = n()) %>%
    arrange(desc(count)) %>%
    head(10)
}

# Function to calculate statistics on tree HEIGHT using data.table
calculate_tree_height_stats_datatable <- function() {
  dc_trees_dt[, .(
    mean_height = mean(HEIGHT, na.rm = TRUE),
    median_height = median(HEIGHT, na.rm = TRUE),
    min_height = min(HEIGHT, na.rm = TRUE),
    max_height = max(HEIGHT, na.rm = TRUE),
    sd_height = sd(HEIGHT, na.rm = TRUE)
  )]
}

# Function to get top 10 common names using data.table
get_top_10_common_names_datatable <- function() {
  dc_trees_dt[, .(count = .N), by = COMMON_NAME][order(-count)][1:10]
}

# Function to calculate statistics on tree HEIGHT using data.frame
calculate_tree_height_stats_dataframe <- function() {
  data.frame(
    mean_height = mean(dc_trees_df$HEIGHT, na.rm = TRUE),
    median_height = median(dc_trees_df$HEIGHT, na.rm = TRUE),
    min_height = min(dc_trees_df$HEIGHT, na.rm = TRUE),
    max_height = max(dc_trees_df$HEIGHT, na.rm = TRUE),
    sd_height = sd(dc_trees_df$HEIGHT, na.rm = TRUE)
  )
}

# Function to get top 10 common names using data.frame
get_top_10_common_names_dataframe <- function() {
  dc_trees_df %>%
    group_by(COMMON_NAME) %>%
    summarize(count = n()) %>%
    arrange(desc(count)) %>%
    head(10)
}

# Time the summarize functions and run them 20 times
timing_results <- microbenchmark(
  dplyr = calculate_tree_height_stats_dplyr(),
  duckdb = calculate_tree_height_stats_duckdb(),
  duckplyr = calculate_tree_height_stats_duckplyr(),
  datatable = calculate_tree_height_stats_datatable(),
  dataframe = calculate_tree_height_stats_dataframe(),
  dplyr_top10 = get_top_10_common_names_dplyr(),
  duckdb_top10 = get_top_10_common_names_duckdb(),
  duckplyr_top10 = get_top_10_common_names_duckplyr(),
  datatable_top10 = get_top_10_common_names_datatable(),
  dataframe_top10 = get_top_10_common_names_dataframe(),
  times = 10
)

# Print the timing results
print(timing_results)

# Calculate total elapsed time for each method
total_elapsed_time <- aggregate(time ~ expr, data = timing_results, sum)

# Modify the timing_results to include total time in the method names
timing_results$expr <- paste(timing_results$expr, 
                             round(total_elapsed_time$time[match(timing_results$expr, total_elapsed_time$expr)] / 1e9, 2), 
                             "s")

# Add a column for technology
timing_results$technology <- ifelse(grepl("dplyr", timing_results$expr), "dplyr",
                             ifelse(grepl("duckdb", timing_results$expr), "duckdb",
                             ifelse(grepl("duckplyr", timing_results$expr), "duckplyr",
                             ifelse(grepl("datatable", timing_results$expr), "datatable", "dataframe"))))

# Plot the timing results for better visualization using a bee plot and box plot
plot1 <- ggplot2::ggplot(timing_results, ggplot2::aes(x = expr, y = time / 1e6, color = technology)) +
  ggplot2::geom_jitter(width = 0.2, height = 0, alpha = 0.5) +
  ggplot2::geom_boxplot(alpha = 0.3, outlier.shape = NA) +
  ggplot2::labs(title = title_with_time, x = "Method", y = "Time (ms)") +
  ggplot2::theme(axis.text.x = ggplot2::element_text(angle = 45, hjust = 1))


# Print the plots
print(plot1)
