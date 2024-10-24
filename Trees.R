# Import required libraries
library(dplyr)
library(readr)

# Import the DC_trees.csv file
dc_trees <- read_csv("data/DC_trees.csv")

# Print the column names
print(colnames(dc_trees))

# Calculate statistics on tree HEIGHT
tree_height_stats <- dc_trees %>%
  summarize(
    mean_height = mean(HEIGHT, na.rm = TRUE),
    median_height = median(HEIGHT, na.rm = TRUE),
    min_height = min(HEIGHT, na.rm = TRUE),
    max_height = max(HEIGHT, na.rm = TRUE),
    sd_height = sd(HEIGHT, na.rm = TRUE)
  )

# Print the results
print(tree_height_stats)
