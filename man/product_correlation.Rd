\name{product_correlation}
\alias{product_correlation}
\docType{data}
\title{
A table of official equivalences between all the revisions in the Harmonized
System and the Standard International Trade Classification
}
\description{
Provides 1:1 and 1:n matches for all commodity codes available from the
United Nations official sources. This data is was obtained from
<https://github.com/tradestatistics/comtrade-codes>.
}
\usage{product_correlation}
\format{
  A data frame with 51841 observations on the following 11 variables.
  \describe{
    \item{\code{hs92}}{a character vector}
    \item{\code{hs96}}{a character vector}
    \item{\code{hs02}}{a character vector}
    \item{\code{hs07}}{a character vector}
    \item{\code{hs12}}{a character vector}
    \item{\code{hs17}}{a character vector}
    \item{\code{bec4}}{a character vector}
    \item{\code{sitc1}}{a character vector}
    \item{\code{sitc2}}{a character vector}
    \item{\code{sitc3}}{a character vector}
    \item{\code{sitc4}}{a character vector}
  }
}
\examples{
product_correlation
}
\keyword{datasets}
\source{UN COMTRADE}
