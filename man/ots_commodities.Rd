\name{ots_commodities}
\alias{ots_commodities}
\docType{data}
\title{
A table of official commodity names from the Harmonized System rev 2012
(HS12, also known as H4)
}
\description{
Provides official commodity and section codes and names taken from the
United Nations official sources. This data is was obtained from the
tradestatistics package.
}
\usage{ots_commodities}
\format{
  A data frame with 5304 observations on the following 4 variables.
  \describe{
    \item{\code{commodity_code}}{a character vector}
    \item{\code{commodity_fullname_english}}{a character vector}
    \item{\code{section_code}}{a character vector}
    \item{\code{section_fullname_english}}{a character vector}
  }
}
\examples{
ots_commodities
}
\keyword{datasets}
\source{Adapted from UN COMTRADE}
