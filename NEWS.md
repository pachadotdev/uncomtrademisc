# uncomtrademisc 2023.08.21

* adds EB02 download option

# uncomtrademisc 2023.08.02

* Adds SITC rev 3-4 and BEC download option
* Updated COMTRADE links to use the API
* Uses archive (pkg) instead of 7zip (external)
* limits just to COMTRADE data (no gravity, no GDP, no RTAs)

# uncomtrademisc 2023.04.22

* Allows to skip download and create the database directly.
* Removes Arrow functions

# uncomtrademisc 2023.03.17

* Uses text and float8 to store UN COMTRADE data

# uncomtrademisc 2023.03.07

* Add requirements for Red Hat Linux
* Normalizes tables in SQL to save space

# uncomtrademisc 2022.12.31

* Allows to save raw data in PostgreSQL

# uncomtrademisc 2022.12.12

* updates default path to gdp tables

# uncomtrademisc 2022.10.31

* allows to download to sub-directories with the subdir parameter

# uncomtrademisc 2022.10.17

* uses gravity variables from UTSIC Dynamic Gravity Dataset instead of CEPII
* uses CEPII distances to keep the distances table in the server the same
* removes all modeling functions (no sucess at modeling CIF/FOB ratio with decent R2)

# uncomtrademisc 2022.10.15

* avoids re-creating parquet files for years without new data
* removes old zip files for years with new data
* allow sequential or parallel downloads on Linux/MacOS

# uncomtrademisc 2022.10.02

* `add_gravity_cols()` now uses the `cepiigravity` package for the data
* because of the previous point `add_rta_cols()` was removed, the RTAs/FTAs
  data comes in `cepiigravity`
* the previous gravity data came from `cepiigeodist`, that doesn't include GDP
  or RTA variables
* uses duckdb for gravity variables  

# uncomtrademisc 2022.09.21

* Added a `NEWS.md` file to track changes to the package.
* I added the option to return units in the tidy output
* As far as I've tested, the changes are compatible with the previous
  version.\
* Running `tidy_flows()` with default parameters returns the
  same output as in previous releases.
