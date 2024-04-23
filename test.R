
id <- "17937694"
template_id = "823437"

df <- data.frame(cbind(id,template_id))

# authenticate for BillomatDB
keys <- Billomatics::authentication_process("billomat")

billomatID <- "k16917022"

set_status_endpoint(df = df,
                    endpoint = "invoices",
                    status_to_set = "complete",
                    billomat_api_key = keys$billomat[2],
                    billomat_id = billomatID)



i <- 1
id <- unique(df$id)
template_id <-unique(df$template_id)

status2set <- paste0("/", status_to_set)
for (id in ids) {
  api_endpoint <- paste0("https://",
                         billomat_id,
                         ".billomat.net/api/",
                         endpoint,
                         "/",
                         id,
                         status2set)

  # set the header
  header <- c("X-BillomatApiKey" = billomat_api_key,
              "Content-Type" = "application/xml")

  if(status_to_set == "complete") {
    body <- list(
      complete = list(
        template_id = structure(list(template_id))
      )
    ) %>% xml2::as_xml_document(.) %>%
      # create a string from the xml
      paste0(.)
    # turn the list into an xml

    # get the response
    response <- httr::PUT(url = api_endpoint,
                          config = httr::add_headers(header),
                          encode = httr::accept("application/xml"),
                          body = body)
  } else {
    # get the response
    response <- httr::PUT(url = api_endpoint,
                          httr::add_headers(header),
                          httr::accept("application/xml"))
  }



  if (response$status_code != 200) {
    print(paste0("failed to put", id))
  }
