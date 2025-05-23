% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/database_management_functions.R
\name{postgres_drop_table}
\alias{postgres_drop_table}
\title{postgres_drop_table}
\usage{
postgres_drop_table(con, schema, table)
}
\arguments{
\item{con}{The database connection object, created using DBI.}

\item{schema}{The schema where the table is located.}

\item{table}{The name of the table to be dropped.}
}
\value{
A feedback message in the console indicating success or failure. Returns TRUE on success, FALSE on failure.
}
\description{
Drops a table from a PostgreSQL database.
This function allows dropping an existing table from the specified schema in the database.
}
\details{
\itemize{
\item The function first checks if the schema is provided either via the \code{schema} argument or embedded in the \code{table} argument as 'schema.table'.
\item The SQL query to drop the table is constructed and executed.
\item If the table exists, it will be dropped. If it doesn't exist, no error will occur due to the \verb{IF EXISTS} clause in the SQL statement.
\item If the operation is successful, a success message is shown, and the function returns \code{TRUE}.
\item If any error occurs during table deletion, an error message is shown, and the function returns \code{FALSE}.
}
}
\examples{
\dontrun{
# Example usage:
connection <- DBI::dbConnect(RPostgres::Postgres(), dbname = "my_database")
postgres_drop_table(connection, "raw", "old_table")
}

}
