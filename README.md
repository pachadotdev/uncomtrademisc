# UN COMTRADE Misc Functions

The goal of uncomtrademisc is to ease some data munging when using UN COMTRADE data.

## 2023-07-17 update

The package will not be maintained anymore after 2023-07-30. A new package will be
created to work with the new UN COMTRADE Plus.

## 2023-07-15 update

To get the downloads to work, we need to alter `/usr/lib/ssl/openssl.cnf` and add this under `[system_default_sect]`

```
Options = UnsafeLegacyRenegotiation
```

Then it is required to restart the computer and try again, this is because of the error `OpenSSL: error:0A000152:SSL routines::unsafe legacy renegotiation disabled` which is created by UN COMTRADE using an old SSL protocol.

## Installation

You can install the development version of uncomtrademisc like so:

``` r
remotes::install_github("pachadotdev/uncomtrademisc")
```

This one-liner will install the next packages if needed: *dplyr, stringr,
purrr, janitor, RPostgres.*

## Usage

This is a summary of the [tradestatistics-database-postgresql repository](https://github.com/tradestatistics/tradestatistics-database-postgresql).

### Declare environment variables

Run `usethis::edit_r_environ()` and add the following variables

```
COMTRADE_TOKEN="THE_TOKEN"
LOCAL_SQL_USR="THE_USER"
LOCAL_SQL_PWD="THE_PWD"
```

Restart the R session after changing your environment.

### Clone the database project from GitHub

Clone https://github.com/tradestatistics/database-postgresql and open the 
project in RStudio.

From a command line
```bash
git clone git@github.com:tradestatistics/database-postgresql.git
```

Or:
```bash
git clone https://github.com/tradestatistics/database-postgresql.git
```

You can also use GitHub Desktop, but I don't know how to use it.

### Download data from UN COMTRADE

Run this code:
```r
uncomtrademisc::data_downloading()
```

To avoid the prompts, and assuming you followed the previous instructions, you can pass these arguments to `data_downloading()`, which will write to the `umcomtrade` PostgreSQL database in `localhost` after tidying the tables, unless you pass `postgres = F` to just download the data as zipped CSV files.

```r
library(uncomtrademisc)

# hs92
data_downloading(postgres = T, token = T, dataset = 1, remove_old_files = 1, subset_years = 1988:2021, parallel = 2, skip_updates = F)

# hs96
data_downloading(postgres = T, token = T, dataset = 2, remove_old_files = 1, subset_years = 1996:2021, parallel = 2, skip_updates = F)

# hs02
data_downloading(postgres = T, token = T, dataset = 3, remove_old_files = 1, subset_years = 2002:2021, parallel = 2, skip_updates = F)

# hs07
data_downloading(postgres = T, token = T, dataset = 4, remove_old_files = 1, subset_years = 2007:2021, parallel = 2, skip_updates = F)

# hs12
data_downloading(postgres = T, token = T, dataset = 5, remove_old_files = 1, subset_years = 2012:2021, parallel = 2, skip_updates = F)

# sitc1
data_downloading(postgres = T, token = T, dataset = 6, remove_old_files = 1, subset_years = 1962:2021, parallel = 2, skip_updates = F)

# sitc2
data_downloading(postgres = T, token = T, dataset = 7, remove_old_files = 1, subset_years = 1976:2021, parallel = 2, skip_updates = F)
```


### Upload the data to the server

These functions by default write to the public schema and will create new
tables or delete the existing tables and overwrite the content table by table.
For example, if you defined `TRADESTATISTICS_SQL_DB="mydb_main"` instead of
`TRADESTATISTICS_SQL_DB="mydb_testing"`, you'll be writing irreversible changes
to your main database, so please check twice before running the code.

# Downsides

The code to download the data from UN COMTRADE relies on specific UNIX
multicore tools, in order to speed up the downloading process. On Windows the
download is sequential, file by file, and therefore much slower (around 10x slower).

`RPostgres` uses the system package libpq. On Windows, it's not a problem. On
Ubuntu/Mac, you'll need to install `libpq-dev` or similar (i.e., the name depends
on the specific platform).
