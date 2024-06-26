% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/create_fields_main_document.R
\name{create_document_level_fields}
\alias{create_document_level_fields}
\title{create_document_level_fields
This function takes a dataframe with the positions and creates the fields for the template}
\usage{
create_document_level_fields(df_positions, bills_created_from)
}
\arguments{
\item{df_positions}{The dataframe with all the positions}

\item{bills_created_from}{What are the bills created from? invoice or confirmation.}
}
\value{
The function returns a df with all the reported clicks in the comment
}
\description{
create_document_level_fields
This function takes a dataframe with the positions and creates the fields for the template
}
