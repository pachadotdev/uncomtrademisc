\name{ots_distances}
\alias{ots_distances}
\docType{data}
\title{
A table of bilateral distances adapted from CEPII
}
\description{
Provides distances between most populated cities or capitols between countries. This data is was adapted from the cepiigeodist package.
}
\usage{ots_distances}
\format{
  A data frame with 22,791 observations on the following 8 variables.
  \describe{
    \item{\code{country1}}{a character vector}
    \item{\code{country2}}{a character vector}
    \item{\code{dist}}{a numeric vector}
    \item{\code{distcap}}{a numeric vector}
    \item{\code{colony}}{an integer vector}
    \item{\code{comlang_ethno}}{an integer vector}
    \item{\code{comlang_off}}{an integer vector}
    \item{\code{contig}}{an integer vector}
  }
}
\examples{
ots_distances
}
\keyword{datasets}
\source{Adapted from CEPII}
