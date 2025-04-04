% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/database_functions.R
\name{postgres_add_metadata}
\alias{postgres_add_metadata}
\title{postgres_add_metadata}
\usage{
postgres_add_metadata(con, key, value, is_new = TRUE)
}
\arguments{
\item{con}{The database connection object.}

\item{key}{The metadata key.}

\item{value}{The metadata value.}

\item{is_new}{A logical value indicating whether the metadata should be treated as new (default is FALSE).
If TRUE, an error is raised if the metadata with the given key already exists.}
}
\value{
A feedback message in the console or an error message.
}
\description{
Adds or updates metadata in the PostgreSQL database.
}
