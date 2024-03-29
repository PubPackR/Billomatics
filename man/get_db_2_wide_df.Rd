% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/wrangling_data_billomat_db_functions.R
\name{get_db_2_wide_df}
\alias{get_db_2_wide_df}
\title{get_db_2_wide_df}
\usage{
get_db_2_wide_df(
  db_table_name,
  billomatDB = billomatDB,
  export_csv = FALSE,
  encryption_key_db = encryption_key_db,
  path_for_export = getwd()
)
}
\arguments{
\item{db_table_name}{the name of the table you are interested in}

\item{export_csv}{set to true if you want to export the wide df as a csv, in this case,
you have to provide the path to the project}

\item{path_for_export}{the path where you want to store the exported file - default is the working directory}
}
\value{
the data from the long data frame turned into a wide data frame with all columns of interest
}
\description{
this function turns the db entry that is in a long format into a readable rowwise entry
}
