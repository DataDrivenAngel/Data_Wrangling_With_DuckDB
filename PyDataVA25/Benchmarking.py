import pandas as pd
import numpy as np
import duckdb
import sqlite3
import time
import os
from pathlib import Path
import random
import string
from tqdm import tqdm
import polars as pl
import seaborn as sns
import matplotlib.pyplot as plt
from datetime import datetime

def generate_random_string(length):
    """Generate a random string of specified length."""
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

def generate_test_data(num_rows, num_cols=5):
    """Generate a DataFrame with random data."""
    data = {
        'id': range(num_rows),  # Unique identifier
        'category': np.random.choice(['A', 'B', 'C', 'D', 'E'], num_rows),  # For grouping
        'numeric1': np.random.random(num_rows),  # Numeric values for aggregation
        'numeric2': np.random.randint(1, 1000, num_rows),  # More numeric values
        'text': [generate_random_string(10) for _ in range(num_rows)]  # Text data
    }
    return pd.DataFrame(data)

def save_to_csv(df, filename):
    """Save DataFrame to CSV file."""
    df.to_csv(filename, index=False)

def benchmark_pandas_read(csv_file):
    """Benchmark pandas read operation."""
    start_time = time.time()
    df = pd.read_csv(csv_file)
    end_time = time.time()
    return end_time - start_time

def benchmark_pandas_groupby(csv_file):
    """Benchmark pandas groupby operation."""
    start_time = time.time()
    df = pd.read_csv(csv_file)
    result = df.groupby('category').agg({
        'numeric1': 'mean',
        'numeric2': 'sum'
    })
    end_time = time.time()
    return end_time - start_time

def benchmark_duckdb_read(csv_file):
    """Benchmark DuckDB read operation."""
    start_time = time.time()
    con = duckdb.connect()
    result = con.execute(f"SELECT * FROM read_csv_auto('{csv_file}')").fetchdf()
    end_time = time.time()
    con.close()
    return end_time - start_time

def benchmark_duckdb_groupby(csv_file):
    """Benchmark DuckDB groupby operation."""
    start_time = time.time()
    con = duckdb.connect()
    result = con.execute(f"""
        SELECT category, 
               AVG(numeric1) as avg_numeric1,
               SUM(numeric2) as sum_numeric2
        FROM read_csv_auto('{csv_file}')
        GROUP BY category
    """).fetchdf()
    end_time = time.time()
    con.close()
    return end_time - start_time

def benchmark_polars_read(csv_file):
    """Benchmark Polars read operation."""
    start_time = time.time()
    df = pl.read_csv(csv_file)
    end_time = time.time()
    return end_time - start_time

def benchmark_polars_groupby(csv_file):
    """Benchmark Polars groupby operation."""
    start_time = time.time()
    df = pl.read_csv(csv_file)
    result = df.group_by('category').agg([
        pl.col('numeric1').mean().alias('avg_numeric1'),
        pl.col('numeric2').sum().alias('sum_numeric2')
    ])
    end_time = time.time()
    return end_time - start_time

def benchmark_pandas_filter(csv_file):
    """Benchmark pandas filter operation."""
    start_time = time.time()
    df = pd.read_csv(csv_file)
    result = df[
        (df['numeric1'] > df['numeric1'].mean()) & 
        (df['numeric2'] < df['numeric2'].median()) &
        (df['category'].isin(['A', 'B']))
    ]
    end_time = time.time()
    return end_time - start_time

def benchmark_duckdb_filter(csv_file):
    """Benchmark DuckDB filter operation."""
    start_time = time.time()
    con = duckdb.connect()
    result = con.execute(f"""
        WITH data as (
        SELECT *
        FROM read_csv_auto('{csv_file}'))

        , STATS as (
            FROM data SELECT
            AVG(numeric1) as avg_numeric1
            , PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY numeric2) as median_numeric2
        )
        select * from data, stats
        WHERE numeric1 > avg_numeric1
        AND numeric2 < median_numeric2
        AND category IN ('A', 'B')
    """).fetchdf()
    end_time = time.time()
    con.close()
    return end_time - start_time

def benchmark_polars_filter(csv_file):
    """Benchmark Polars filter operation."""
    start_time = time.time()
    df = pl.read_csv(csv_file)
    stats = df.select([
        pl.col('numeric1').mean().alias('avg_numeric1'),
        pl.col('numeric2').median().alias('median_numeric2')
    ])
    result = df.filter(
        (pl.col('numeric1') > stats['avg_numeric1'][0]) &
        (pl.col('numeric2') < stats['median_numeric2'][0]) &
        pl.col('category').is_in(['A', 'B'])
    )
    end_time = time.time()
    return end_time - start_time

def setup_sqlite_db():
    """Setup SQLite database for storing benchmark results."""
    conn = sqlite3.connect('benchmark_results.db')
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS benchmark_results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_size INTEGER,
            file_size_mb REAL,
            operation TEXT,
            tool TEXT,
            execution_time REAL,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    conn.commit()

    return conn

def store_benchmark_result(conn, file_size, file_size_mb, operation, tool, execution_time):
    """Store benchmark result in SQLite database."""
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO benchmark_results (file_size, file_size_mb, operation, tool, execution_time)
        VALUES (?, ?, ?, ?, ?)
    ''', (file_size, file_size_mb, operation, tool, execution_time))
    conn.commit()


def generate_benchmark_image():
    duckdb.sql("INSTALL sqlite; LOAD sqlite;")
    duckdb.sql("Create table if not exists benchmarks as select * from sqlite_scan('benchmark_results.db','benchmark_results')")
    df = duckdb.sql(""" 
    FROM benchmarks
    SELECT 
    tool
    , file_size
    , regexp_replace(operation, '\d+$', '') as operation      -- remove trailing integers
    , execution_time
   -- where date_trunc('day',  timestamp) = '2025-04-19'

    """).to_df()

    # Create the lmplot with legend positioned at the upper middle left
    g = sns.lmplot(x='file_size', y='execution_time', data=df, hue='tool', 
                height=6, aspect=10/6,
                legend_out=False)  # Keep legend inside the plot

    # Get the figure and axes from the FacetGrid
    fig = g.fig
    ax = g.axes[0, 0]  # The first (and only) subplot

    # Set x-axis to log scale
    ax.set_xscale('log')

    # Add title and labels
    ax.set_title('Execution Time Distribution by Tool', fontsize=16)
    ax.set_xlabel('File Size (rows)', fontsize=14)
    ax.set_ylabel('Execution Time (seconds)', fontsize=14)

    # Add grid for better readability
    ax.grid(axis='y', linestyle='--', alpha=0.7)

    # Update the legend position
    handles, labels = ax.get_legend_handles_labels()
    ax.legend(handles, labels, loc='upper left', bbox_to_anchor=(0.6, .65))

    # Improve aesthetics
    sns.despine(left=False, bottom=False, ax=ax)
    plt.tight_layout()

    # Save the figure with correct function and parameters
    chart_name = f"chart_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.png"
    plt.savefig(chart_name)
    

def main():
    # Create output directory if it doesn't exist
    output_dir = Path('benchmark_data')
    output_dir.mkdir(exist_ok=True)
    
    # Setup SQLite database
    conn = setup_sqlite_db()
    
    # Define file sizes to test (in rows)
    file_sizes = [
                # 1000, 10000, 100000
                #  ,
                1000000,
                #    2000000,
                #    1000000,
                #    2000000,
                #    3000000,
                #    4000000,
                #    5000000,
                #    6000000,
                #    7000000,
                #    8000000,
                #    9000000,
                #   10000000,
                #    25000000,
                #    50000000,
                #   100000000
                   ]

    # First loop: Generate data files
    for size in tqdm(file_sizes, desc="Creating Data"):
        #print(f"\nGenerating data with {size:,} rows...")
        
        # Generate and save test data
        csv_file = output_dir / f'test_data_{size}.csv'
        if not csv_file.exists():
            df = generate_test_data(size)
            save_to_csv(df, csv_file)
        else:
            print(f"File already exists: {csv_file}")
    
    benchmarks = [
           # ('read', benchmark_pandas_read, 'pandas'),
           # ('read', benchmark_duckdb_read, 'duckdb'),
           # ('read', benchmark_polars_read, 'polars'),
            ('groupby', benchmark_pandas_groupby, 'pandas'),
            ('groupby', benchmark_duckdb_groupby, 'duckdb'),
            ('groupby', benchmark_polars_groupby, 'polars'),
            ('filter', benchmark_pandas_filter, 'pandas'),
            ('filter', benchmark_duckdb_filter, 'duckdb'),
            ('filter', benchmark_polars_filter, 'polars')
        ]

    # Second loop: Run benchmarks
    total_iterations = len(file_sizes) * len(benchmarks)
    for size in tqdm(file_sizes, desc="Benchmarking", total=total_iterations):
       # print(f"\nTesting {size:,} rows...")
        
        csv_file = output_dir / f'test_data_{size}.csv'
        file_size_mb = os.path.getsize(csv_file) / (1024 * 1024)
        
        # Run benchmarks

        
        for operation, benchmark_func, tool in tqdm(benchmarks, desc=f"Running benchmarks for {size:,} rows", leave = False):

            times = []
            for i in tqdm(range(5), desc =f"Running {operation} benchmark with {tool}", leave = False):
                execution_time = benchmark_func(csv_file)
                times.append(execution_time)
                
                try:
                    store_benchmark_result(conn, size, file_size_mb, f"{operation}", tool, execution_time)
                except Exception as e:
                    print(f"Error storing results: {e}")
    conn.close()
    generate_benchmark_image()

    print("\nBenchmarking completed. Results stored in benchmark_results.db")
    

if __name__ == "__main__":
    main()
