# Read the data from average_temp.csv and calculate average temperatures by year
library(readr)
library(dplyr)
library(microbenchmark)
library(duckdb)
library(ggplot2)

# Specify the path to the CSV file
file_path <- "data/average_temp.csv"
trials = 5

# Read the entire CSV file
data <- read_csv(file_path)

# Create a DuckDB connection and table
con <- dbConnect(duckdb())
duckdb_register(con, "data", data)

# First benchmark dplyr
dplyr_benchmark <- microbenchmark(
  dplyr = {
    data |>
      group_by(year) |>
      summarise(
        mean_temp = mean(mean_temp_k, na.rm = TRUE), n = n()
      ) |> arrange(year)
  },
  times = trials
)


library(duckplyr)
# Then benchmark duckplyr
duckplyr_benchmark <- microbenchmark(
  duckplyr = {
      data |>
      #duckplyr::as_duckplyr_tibble() |>
      group_by(year) |>
      summarise(
        mean_temp = mean(mean_temp_k, na.rm = TRUE), n = n()
      ) |> arrange(year)
  },
  times = trials
)

# Combine the benchmark results
benchmark_result <- rbind(dplyr_benchmark, duckplyr_benchmark)

# Calculate yearly averages using dplyr for verification
yearly_averages <- data %>%
  group_by(year) %>%
  summarise(avg_temp = mean(mean_temp_k, na.rm = TRUE))

# Print the yearly averages
print("Yearly Average Temperatures:")
print(yearly_averages)

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
  #geom_boxplot(alpha = 0.5) +
  geom_point(position = position_jitter(width = 0.2), alpha = 0.9) +
  labs(title = paste0("Benchmark: dplyr vs DuckDB vs duckplyr (", trials, " trials)"),
       subtitle = "Calculating yearly temperature averages in 80 million observations / 3.9 GB csv.",
       x = "Method", 
       y = "Time (milliseconds)") +
  scale_x_discrete(labels = method_labels) +
  scale_y_continuous(limits = c(0, NA), expand = expansion(mult = c(0, 0.05))) +
  theme_minimal()

# Close the DuckDB connection
dbDisconnect(con, shutdown = TRUE)

print(benchmark_result)
duckplyr::fallback_review()
