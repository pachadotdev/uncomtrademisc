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

This one-liner will install the next packages if needed: *arrow, dplyr, stringr,
purrr, janitor, RPostgres, cepiigeodist.*

## Usage

This is a summary of the [tradestatistics-database-postgresql repository](https://github.com/tradestatistics/tradestatistics-database-postgresql).

### Declare environment variables

Run `usethis::edit_r_environ()` and add the following variables

```
COMTRADE_TOKEN="THE_TOKEN"
TRADESTATISTICS_SQL_HOST="THE_HOST"
TRADESTATISTICS_SQL_DB="THE_DB"
TRADESTATISTICS_SQL_USR="THE_USER"
TRADESTATISTICS_SQL_PWD="THE_PWD"
```

Restart the R session after changing your environment.

In my case, I decided to set `TRADESTATISTICS_SQL_HOST="localhost"` and then
connect by using a secure SSH tunnel, like this:

```
ssh -L DB_PORT:localhost:DB_PORT me@my.server
```

This approach is much more easier compared to modifying remote access in the
server.

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

To avoid the prompts, and assuming you followed the previous instructions,
you'll need to download HS02 and HS12 data:

```r
library(uncomtrademisc)

data_downloading(token = TRUE, dataset = 3) # H202
data_downloading(token = TRUE, dataset = 5) # HS12
```

### Tidy UN COMTRADE data

Run a code like so:
```r
library(purrr)

map(
  2002:2020,
  function(y) {
    message(y)
    if (dir.exists(paste0("hs-rev2012-tidy/", y))) { return(TRUE) }
    tidy_flows(y) %>%
      group_by(year, reporter_iso) %>%
      write_dataset("hs-rev2012-tidy", hive_style = F)
  }
)
```

The tidy function converts HS rev 2002 to HS rev 2012 by default and creates
data ready to upload to the server.

### Upload the data to the server

These functions by default write to the public schema and will create new
tables or delete the existing tables and overwrite the content table by table.
For example, if you defined `TRADESTATISTICS_SQL_DB="mydb_main"` instead of
`TRADESTATISTICS_SQL_DB="mydb_testing"`, you'll be writing irreversible changes
to your main database, so please check twice before running the code.

#### Commodities

Assuming that you have write access to a PostgreSQL database in a server (or
locally), run the next code to upload and/or update the commodities trade data:

```r
con <- con_tradestatistics()

update_yc(con)
update_yr(con)
update_yrc(con)
update_yrp(con)
update_yrpc(con)

RPostgres::dbDisconnect(con)
```

#### Attributes

You need to upload the Attributes (i.e., commodity, section and country codes).

The database repository contains the attributes in RDS format, therefore you
need to run:

```r
update_commodities(con)
update_countries(con)
update_sections(con)
```

#### Additional tables

The GDP, RTAs, and Tariffs tables can be generated by running the codes from
the database repository.

These MFN and PRF input tables are not uploaded to GitHub because of their 
weight, but you can download these from 
https://shiny.tradestatistics.io/mfn_prf.zip and run the codes from the database repository.

```r
update_rtas(con)
update_tariffs(con)
update_distances(con)
update_gdp(con)
update_gdp_deflator(con)
update_vaccine_inputs(con)
```

To check those tables, which are updated by separate from UN COMTRADE data,
check the aforementioned repository. In any case, these tables are
updated with much less frequency and use non-US sources (i.e., World Bank,
CEPII and DESTA).

# Downsides

The code to download the data from UN COMTRADE relies on specific UNIX
multicore tools, in order to speed up the downloading process. On Windows the download is sequential, file by file, and therefore much slower (around 10x slower).

`RPostgres` uses the system package libpq. On Windows, it's not a problem. On
Ubuntu/Mac, you'll need to install `libpq-dev` or similar (i.e., the name depends
on the specific platform).
