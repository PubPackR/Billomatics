% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/call_billomat.R
\name{post_single_invoice_item}
\alias{post_single_invoice_item}
\title{post_single_invoice_item}
\usage{
post_single_invoice_item(
  df_items,
  billomatApiKey,
  billomatID,
  endpoint = "invoice-items"
)
}
\arguments{
\item{df_items}{the df containing the items that should be part of the respective invoice already posted.
The df must contain the invoice_id}

\item{billomatID}{the billomat ID}

\item{endpoint}{The endpoint you want to post (e.g., "invoice-items", "confirmation-items"). Always resource in plural!}

\item{billomat_api_key}{The Billomat API key.}

\item{entity_id}{The entity id of your object. Findable in the URL.}

\item{...}{Additional values corresponding to the resource (see Billomat API documentation).}
}
\value{
returns a list containing posted_invoice_id
}
\description{
this function needs to move through the rows in of the invoice table and create the single invoice
}
