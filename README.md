# RGOV24 Presentation

## Wrangling Data With DuckDB
 
![Slides](Slideshttps://github.com/DataDrivenAngel/RGOV24/blob/main/Angel%2C%20Will%20RGOV24%20-%20Wrangling%20Data%20with%20DuckDB%20%F0%9F%A6%86.pdf)



## Key Takeaways 

1. DuckDB is a *very* fast in-process SQL database
2. Duckplyr is a package for using Dplyer on DuckDB
3. You can use Duckplyr as a *drop in replacement to get a 2-30x speedup for dplyr code on large datasets*
4. Big data is smaller than it used to be.

## Agenda
* What is DuckDB 
* Why should you care about DuckDB
* When should you use DuckDB
* How can you use DuckDB
* What is Duckplyr
* Data processing performance and profiling
* A brief tangent on the shrinking of big data.


# DuckDB
![](images/DuckDB.png)


## What is DuckDB?

DuckDB is an open source fast in-process analytical database!

* Open Source: Free & Open
* Fast: Performant. **Quickly and efficiently runs analytical SQL queries**
* In-process: **Runs locally** without a server
* Analytical: DuckDB is optimized for **aggregations** and analytical queries to support online analytical processing (OLAP). DuckDB supports ACID transactions, but is not as fast for online transaction processing (OLTP) workloads
* Database: DuckDB can be used to efficiently store relational data.


## Why should you care?
* DuckDB is a great tool for local SQL analysis. Import a file and you can do SQL locally!
* DuckDB is starting to power the next generation of embedded analytical tools, so expect browser based data filtering tools to get more powerful.

## Why should you care (more technical)
* DuckDB is like SQLite but for data processing. You can crunch significant amounts of data locally without spinning up a full database / data warehouse server, which may create significant time/cost savings and simplify system design
* DuckDB is versatile and fast for data processing. Competitive with spark/polars in benchmarks
* DuckDB is portable with zero dependencies.

##  When should you use DuckDB
* You should use DuckDB for SQL data processing!
* You have medium-largish data
* You don't want the hassle of procuring larger computing resources.

##  How can you use DuckDB
* Directly use the DuckDB package
* use DBplyr to connect to a local DuckDB database
* use Duckplyr!


## What is Duckplyr
* Duckplyr is a drop in replacement for Dplyr!
* Duckplyr uses DuckDB’s “relational” API to skip the SQL and directly construct logical query plans
* This means you can speed up your Dplyr code by ~2-30x by simply changing the package! 
* Unsupported operations will fall back to Dplyr, so your code will always run!
* Duckplyr *overwrites* Dplyr methods, so it only takes loading the package.
  ```
  library("duckplyr")
  ```

## Duckplyr Caveats
* Unsupported operations will fall back to Dplyr, so your code will always run!
  * This may mean you don't get a performance increase
  * If you're doing out of memory processing this may also cause issues
* Duckplyr is still under active development, so there are some surprising unsupported operations:
  * "message":"No relational implementation for group_by()."
  
