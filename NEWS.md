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
