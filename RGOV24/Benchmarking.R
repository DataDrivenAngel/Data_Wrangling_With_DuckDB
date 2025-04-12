library(dplyr)
library(microbenchmark)
library(ggplot2)
library(duckplyr)
library(lubridate)



start_time <- Sys.time()

# Function to format elapsed time
format_elapsed_time <- function(start_time) {
  elapsed <- difftime(Sys.time(), start_time, units = "secs")
  sprintf("Elapsed time: %.2f seconds", as.numeric(elapsed))
}

# Function to create more comprehensive test dataset
create_test_data <- function(n_rows) {
  set.seed(123)
  dates <- seq(as.Date("2020-01-01"), by = "day", length.out = 100)
  
  data.frame(
    id = 1:n_rows,
    group = sample(letters[1:5], n_rows, replace = TRUE),
    subgroup = sample(LETTERS[1:10], n_rows, replace = TRUE),
    value1 = rnorm(n_rows),
    value2 = runif(n_rows),
    value3 = sample(1:100, n_rows, replace = TRUE),
    date = sample(dates, n_rows, replace = TRUE),
    category = sample(c("A", "B", "C", "D"), n_rows, 
                      prob = c(0.4, 0.3, 0.2, 0.1), replace = TRUE),
    quantity = rpois(n_rows, lambda = 5),
    is_active = sample(c(TRUE, FALSE), n_rows, prob = c(0.8, 0.2), replace = TRUE)
  )
}

# Define comprehensive test operations
run_operations <- function(data) {
  list(
    # Basic Operations
    "Simple Filter - dplyr" = . %>% 
      filter(value1 > 0 & is_active == TRUE),
    
    "Simple Filter - duckplyr" = . %>% 
      duckplyr::mutate() %>%
      filter(value1 > 0 & is_active == TRUE),
    
    # Complex Filtering
    "Complex Filter - dplyr" = . %>%
      filter(
        value1 > mean(value1) & 
          value2 < median(value2) & 
          group %in% c("a", "b") &
          date >= as.Date("2020-03-01")
      ),
    
    "Complex Filter - duckplyr" = . %>%
      duckplyr::mutate() %>%
      filter(
        value1 > mean(value1) & 
          value2 < median(value2) & 
          group %in% c("a", "b") &
          date >= as.Date("2020-03-01")
      ),
    
    # Window Functions
    "Window Functions - dplyr" = . %>%
      group_by(group, subgroup) %>%
      mutate(
        row_number = row_number(),
        running_total = cumsum(value3),
        relative_to_first = value1 - first(value1),
        group_rank = min_rank(desc(value2))
      ) %>%
      ungroup(),
    
    "Window Functions - duckplyr" = . %>%
      duckplyr::mutate() %>%
      group_by(group, subgroup) %>%
      mutate(
        row_number = row_number(),
        running_total = cumsum(value3),
        relative_to_first = value1 - first(value1),
        group_rank = min_rank(desc(value2))
      ) %>%
      ungroup(),
    
    # Complex Aggregations
    "Complex Aggregation - dplyr" = . %>%
      group_by(group, category, floor_date = floor_date(date, "month")) %>%
      summarise(
        count = n(),
        mean_val = mean(value1, na.rm = TRUE),
        weighted_avg = weighted.mean(value1, value3, na.rm = TRUE),
        median_val = median(value2, na.rm = TRUE),
        stddev = sd(value1, na.rm = TRUE),
        total_quantity = sum(quantity),
        active_ratio = mean(is_active),
        .groups = "drop"
      ),
    
    "Complex Aggregation - duckplyr" = . %>%
      duckplyr::mutate() %>%
      group_by(group, category, floor_date = floor_date(date, "month")) %>%
      summarise(
        count = n(),
        mean_val = mean(value1, na.rm = TRUE),
        weighted_avg = weighted.mean(value1, value3, na.rm = TRUE),
        median_val = median(value2, na.rm = TRUE),
        stddev = sd(value1, na.rm = TRUE),
        total_quantity = sum(quantity),
        active_ratio = mean(is_active),
        .groups = "drop"
      ),
    
    # Multiple Transformations
    "Multiple Transformations - dplyr" = . %>%
      mutate(
        scaled_value = scale(value1),
        log_value = log1p(value2),
        binned_value = cut(value3, breaks = 10),
        days_since = as.numeric(difftime(max(date), date, units = "days"))
      ) %>%
      group_by(group, binned_value) %>%
      mutate(
        group_z_score = (value1 - mean(value1)) / sd(value1),
        pct_rank = percent_rank(value2)
      ) %>%
      ungroup(),
    
    "Multiple Transformations - duckplyr" = . %>%
      duckplyr::mutate() %>%
      mutate(
        scaled_value = scale(value1),
        log_value = log1p(value2),
        binned_value = cut(value3, breaks = 10),
        days_since = as.numeric(difftime(max(date), date, units = "days"))
      ) %>%
      group_by(group, binned_value) %>%
      mutate(
        group_z_score = (value1 - mean(value1)) / sd(value1),
        pct_rank = percent_rank(value2)
      ) %>%
      ungroup()
  )
}

# Modified plot_scaling function to handle multiple operations
plot_scaling <- function(results, current_size) {
  # Calculate summary statistics
  results_summary <- results %>%
    group_by(dataset_size, expr) %>%
    summarise(
      mean_time = mean(time) / 1e6,  # Convert to milliseconds
      sd_time = sd(time) / 1e6,
      .groups = "drop"
    )
  
  # Extract operation type from expression
  results_summary <- results_summary %>%
    mutate(
      operation_type = sub(" - .*$", "", expr),
      engine = sub("^.* - ", "", expr)
    )
  
  # Create faceted plot
  p <- ggplot(results_summary, 
              aes(x = dataset_size, y = mean_time, color = engine)) +
    geom_line(linewidth = 1) +
    geom_point(size = 3) +
    geom_ribbon(
      aes(
        ymin = mean_time - sd_time,
        ymax = mean_time + sd_time,
        fill = engine
      ),
      alpha = 0.2,
      color = NA
    ) +
    facet_wrap(~operation_type, scales = "free_y") +
    scale_x_log10(
      labels = scales::label_number(),
      name = "Dataset Size (rows)"
    ) +
    scale_y_log10(
      labels = scales::label_number(),
      name = "Execution Time (milliseconds)"
    ) +
    scale_color_manual(
      values = c("dplyr" = "#E69F00", "duckplyr" = "#56B4E9"),
      name = "Engine"
    ) +
    scale_fill_manual(
      values = c("dplyr" = "#E69F00", "duckplyr" = "#56B4E9"),
      name = "Engine"
    ) +
    theme_minimal() +
    theme(
      legend.position = "top",
      panel.grid.minor = element_blank(),
      text = element_text(size = 12),
      legend.title = element_text(face = "bold"),
      axis.title = element_text(face = "bold"),
      strip.text = element_text(size = 10, face = "bold")
    ) +
    labs(
      title = "dplyr vs duckplyr Performance Comparison",
      subtitle = sprintf("Current dataset size: %s rows", format(current_size, big.mark = ","))
    )
  
  print(p)
}


# Add the missing benchmark_size function
benchmark_size <- function(n_rows, accumulated_results = NULL, times = 11) {
  benchmark_start <- Sys.time()
  cat(sprintf("\n[%s] Benchmarking dataset size: %d rows\n", 
              format(benchmark_start, "%Y-%m-%d %H:%M:%S"),
              n_rows))
  cat(format_elapsed_time(start_time), "\n")
  
  # Create dataset
  data_creation_start <- Sys.time()
  test_data <- create_test_data(n_rows)
  cat(sprintf("Dataset creation time: %.2f seconds\n", 
              as.numeric(difftime(Sys.time(), data_creation_start, units = "secs"))))
  
  operations <- run_operations(test_data)
  
  
  # Run benchmarks for each operation
  results <- lapply(names(operations), function(op_name) {
    op_start <- Sys.time()
    cat(sprintf("\n[%s] Running %s\n", 
                format(op_start, "%Y-%m-%d %H:%M:%S"),
                op_name))
    bench_result <- microbenchmark(
      operations[[op_name]](test_data),
      times = times
    )
    op_end <- Sys.time()
    cat(sprintf("Operation completed in %.2f seconds\n", 
                as.numeric(difftime(op_end, op_start, units = "secs"))))
    
    data.frame(
      time = bench_result$time,
      expr = op_name,
      dataset_size = n_rows
    )
  }) %>% bind_rows()
  
  # Combine with accumulated results
  if (!is.null(accumulated_results)) {
    all_results <- bind_rows(accumulated_results, results)
  } else {
    all_results <- results
  }
  
  # Update plot
  plot_scaling(all_results, n_rows)
  
  # Return accumulated results
  return(all_results)
}

# Create summary statistics (remains the same)
create_summary_stats <- function(results) {
  # Calculate basic statistics
  basic_stats <- results %>%
    group_by(dataset_size, expr) %>%
    summarise(
      mean_ms = mean(time) / 1e6,
      median_ms = median(time) / 1e6,
      sd_ms = sd(time) / 1e6,
      .groups = "drop"
    )
  
  # Calculate scaling factors
  scaling_stats <- basic_stats %>%
    group_by(expr) %>%
    arrange(dataset_size) %>%
    mutate(
      size_ratio = dataset_size / lag(dataset_size),
      time_ratio = mean_ms / lag(mean_ms),
      scaling_factor = time_ratio / size_ratio
    ) %>%
    ungroup()
  
  # Return both stats
  list(
    basic_stats = basic_stats %>% arrange(expr, dataset_size),
    scaling_stats = scaling_stats %>% 
      filter(!is.na(scaling_factor)) %>%
      arrange(expr, dataset_size)
  )
}

# Main execution code (remains the same)
dataset_sizes <- 10^seq(0,10)
dataset_sizes <- round(dataset_sizes)  # Round to whole numbers

accumulated_results <- NULL
# Run benchmarks and update plot after each size
for (size in dataset_sizes) {
  accumulated_results <- benchmark_size(size, accumulated_results)
  Sys.sleep(1)
}



# Generate and print final summary statistics
stats <- create_summary_stats(accumulated_results)
cat("\nFinal Basic Statistics:\n")
print(stats$basic_stats)
cat("\nFinal Scaling Statistics:\n")
print(stats$scaling_stats)