% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/database_functions.R
\name{postgres_create_table}
\alias{postgres_create_table}
\title{postgres_create_table}
\usage{
postgres_create_table(con, schema, table, columns)
}
\arguments{
\item{con}{The database connection object.}

\item{schema}{The schema where the table will be created.}

\item{table}{The name of the table to be created.}

\item{columns}{A named vector where names are column names and values are data types.}
}
\value{
A feedback message in the console indicating success or failure.
}
\description{
Creates a new table in a PostgreSQL database.
}
