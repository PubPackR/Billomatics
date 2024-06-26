% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/extract_information_billomat_functions.R
\name{get_information_from_comments}
\alias{get_information_from_comments}
\title{get_information_from_comments
This function takes a dataframe with comments and turns them into a table containing the reported performance}
\usage{
get_information_from_comments(
  df_comments,
  document_type = "confirmation",
  desired_information = c("Rechnungszusatzinformation", "CPC Kampagne")
)
}
\arguments{
\item{df_comments}{The dataframe with all the comments downloaded}

\item{document_type}{This passes the origin of the comment - visible in the comment_df NAME_id}

\item{desired_information}{What information from the comments should be returned, the invoice details OR CPC performance}
}
\value{
The function returns a df with either the extra billing informations in a table or the CPC performance
}
\description{
get_information_from_comments
This function takes a dataframe with comments and turns them into a table containing the reported performance
}
