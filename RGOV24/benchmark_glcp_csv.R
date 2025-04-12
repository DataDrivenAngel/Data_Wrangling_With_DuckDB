# Read the data from average_temp.csv and calculate average temperatures by year
library(readr)
library(dplyr)
library(microbenchmark)
library(duckdb)
library(ggplot2)
library(data.table)

# Specify the path to the CSV file
file_path <- "data/glcp.csv"
trials = 50

# Read the entire CSV file
data <- read_csv(file_path)

# Create a DuckDB connection and table
con <- dbConnect(duckdb())
duckdb_register(con, "data", data)

# Convert to data.table
dt <- as.data.table(data)

# First benchmark dplyr
dplyr_benchmark <- microbenchmark(
  dplyr = {
    data |>
      summarise(
        .by = country,
        avg_size = mean(total_km2)
        , n = n()
      )
  },
  times = trials
)

# Benchmark DuckDB
duckdb_benchmark <- microbenchmark(
  duckdb = {
    dbGetQuery(con, "
      SELECT 
        country,
        AVG(total_km2) as avg_size,
        COUNT(*) as n
      FROM data 
      GROUP BY country
    ")
  },
  times = trials
)

library(duckplyr)
# Then benchmark duckplyr
duckplyr_benchmark <- microbenchmark(
  duckplyr = {
    data |>  duckplyr::as_duckplyr_tibble() |>
      summarise(
        .by = country,
        avg_size = mean(total_km2)
        , n = n()
      )
  },
  times = trials
)

# Benchmark data.table
datatable_benchmark <- microbenchmark(
  data.table = {
    dt[, .(avg_size = mean(total_km2), n = .N), by = country]
  },
  times = trials
)

# Combine the benchmark results
benchmark_result <- rbind(dplyr_benchmark, duckdb_benchmark, duckplyr_benchmark, datatable_benchmark)

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
ggplot(plot_data, aes(x = factor(method, levels = c("dplyr", "duckplyr", "duckdb", "data.table")), y = time)) +
  #geom_boxplot(alpha = 0.5) +
  geom_point(position = position_jitter(width = 0.2), alpha = 0.9) +
  labs(title = paste0("Benchmark: dplyr vs duckplyr(fallback) vs DuckDB vs data.table (", trials, " trials)"),
       subtitle = "Calculating yearly temperature averages for 30 million observations / 5.5 GB csv.",
       x = "Method", 
       y = "Time (milliseconds)") +
  scale_x_discrete(labels = method_labels) +
  scale_y_continuous(limits = c(0, NA), expand = expansion(mult = c(0, 0.05))) +
  theme_minimal()

# Close the DuckDB connection
dbDisconnect(con, shutdown = TRUE)

print(benchmark_result)
duckplyr::fallback_review()
