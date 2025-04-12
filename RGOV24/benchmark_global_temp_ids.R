# Read the data from average_temp.csv and count distinct hybas ids
library(readr)
library(dplyr)
library(microbenchmark)
library(duckdb)
library(ggplot2)
library(duckplyr)

# Specify the path to the CSV file
file_path <- "data/average_temp.csv"
trials = 25

# Read the entire CSV file
data <- read_csv(file_path)

# Create a DuckDB connection and table
#con <- dbConnect(duckdb())
#duckdb_register(con, "data", data)

# Benchmark the distinct function for dplyr, DuckDB, and duckplyr
benchmark_result <- microbenchmark(
  dplyr = {
    data %>%
      distinct(HYBAS_ID) %>%
      nrow()
  },
#  duckdb = {
#    dbGetQuery(con, "SELECT COUNT(DISTINCT HYBAS_ID) FROM data")[[1]]
#  },
  duckplyr = {
    data %>%
      duckplyr::as_duckplyr_tibble() %>%
      distinct(HYBAS_ID) %>%
      nrow()
  },
  times = trials
)

# Get the number of distinct hybas ids
distinct_hybas_count <- data %>%
  distinct(HYBAS_ID) %>%
  nrow()

# Print the count of distinct hybas ids
print(paste("Number of distinct HYBAS IDs:", distinct_hybas_count))

# Print the benchmark results
print(benchmark_result)

# Create a data frame for plotting
plot_data <- data.frame(
  method = benchmark_result$expr,
  time = benchmark_result$time / 1e6  # Convert to milliseconds
)

# Calculate mean times per method
mean_times <- aggregate(time ~ method, plot_data, mean)

# Create labels with mean times
method_labels <- paste0(mean_times$method, "\n(mean: ", round(mean_times$time, 2), " ms)")
names(method_labels) <- mean_times$method

# Create a ggplot
ggplot(plot_data, aes(x = method, y = time)) +
  geom_boxplot(alpha = 0.5, outliers =  FALSE) +
  geom_point(position = position_jitter(width = 0.2), alpha = 0.9) +
  labs(title = paste0("Benchmark: dplyr vs duckplyr (", trials, " trials)"),
       subtitle = "Counting distinct IDs in 80 million observations / a 3.9 GB csv.",
       x = "Method", 
       y = "Time (milliseconds)") +
  scale_x_discrete(labels = method_labels) +
  scale_y_continuous(limits = c(0, NA), expand = expansion(mult = c(0, 0.05))) +
  theme_minimal()

# Close the DuckDB connection
#dbDisconnect(con, shutdown = TRUE)

print(benchmark_result)

