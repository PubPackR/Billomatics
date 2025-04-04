% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/database_functions.R
\name{postgres_select}
\alias{postgres_select}
\title{postgres_select}
\usage{
postgres_select(con, schema, table, columns = "*", where = NULL, limit = NULL)
}
\arguments{
\item{con}{The database connection object.}

\item{schema}{The schema of the table.}

\item{table}{The name of the table.}

\item{columns}{A character vector of column names to select (default is "*").}

\item{where}{Optional SQL \code{WHERE} clause (without the word WHERE).}

\item{limit}{Optional number of rows to limit.}
}
\value{
A data frame containing the result.
}
\description{
Selects data from a PostgreSQL table.
}
