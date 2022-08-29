The goal of uncomtrademisc is to ease some data munging when using UN COMTRADE data.

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

In my case, I decided to set `TRADESTATISTICS_SQL_HOST="localhost"` and then
connect by using a secure SSH tunnel, like this:

```
ssh -L DB_PORT:localhost:DB_PORT me@my.server
```

This approach is much more easier compared to modifying remote access in the
server.

### Download data from UN COMTRADE

Run this code:
```r
uncomtrademisc::data_downloading()
```

Then select option 1 and then any number 1-9 (i.e., select 3 and 5 in two
different runs).

### Tidy UN COMTRADE data

Run a code like so:
```r
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

### Upload data to the server

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

This example skips updating the next relevant tables:

1. Attributes (i.e., commodity and country codes)
2. GDP
3. RTAs
4. Tariffs

To check those tables, which are updated by separate from UN COMTRADE data,
check the aforementioned repository on top. In any case, these tables are
updated with much less frequency and use non-US sources (i.e., World Bank,
CEPII and DESTA).

Also, these functions by default write to the public schema and will create new
tables or delete the existing tables and overwrite the content table by table.
For example, if you defined `TRADESTATISTICS_SQL_DB="mydb_main"` instead of
`TRADESTATISTICS_SQL_DB="mydb_testing"`, you'll be writing irreversible changes
to your main database, so please check twice before running the code.

# Downsides

The code to download the data from UN COMTRADE relies on specific UNIX
multicore tools, in order to speed up the downloading process. I am not sure
and I haven't tested this on Windows.

An older code in an old commit in this repository uses R's default downloading
function, which takes around 10x longer. Please let me know if you observe
anything and I'll revert the data downloading update.

`RPostgres` uses the system package libpq. On Windows, it's not a problem. On
Ubuntu/Mac, you'll need to install `libpq-dev` or similar (i.e., the name depends
on the specific platform).
