

#' curl_fetch_header
#'
#' this function carries out the call to get the header
#'
#' @param content the name of the tables you are interested in, this is one string
#' @param page which page to get
#' @param page how many entries per page to get
#' @param billomatApiKey please provide your billomat Api key here
#' @param billomatID please provide your billomat ID here
#' @return the call returns a list with the header information

#' @export
curl_fetch_header <- function(content=c("articles",
                                        "clients",
                                        "offers",
                                        "invoices",
                                        "confirmations"),
                              page = 1,
                              per_page=1,
                              billomatApiKey = billomatApiKey,
                              billomatID = billomatID){
  call = paste0("https://",
                billomatID, ## this is the url of the site, but it contains all information necessary
                ".billomat.net/api/",
                content, # this s what i want
                "?api_key=", # this is my key
                billomatApiKey,
                "&per_page=", # this is my limit per page
                per_page,
                "&page=",
                page) # my page number
  # so for this to work, i have to cycle through all pages, as the number increases
  # i need to stop when reaching the last page -> how do I know how many pages there are?
  response <- httr::GET(call) # here I use the method a GET, this means I retrieve data
  # the response contains the header, the method and content as well as additional information
  remaining_limit = as.numeric(httr::headers(response)$`x-rate-limit-remaining`)
  # this tells me how many more calls are possible -- not important at this point
  body <- xml2::read_xml(response$content)
  # this is the content of this call for the results of page 1
  num_results <- httr::headers(response)$`x-total-count`
  # this tells me how many results are on each page, i can keep calling pages forever,
  # but they will have 0 length, which means I should stop at this point
  cat(content,"header", "\n","total rows:",num_results,"remaining_limit:",remaining_limit)
  return(list(headers=httr::headers(response)))
}

#' curl_fetch_billomat
#' this function carries out the call to get the header
#' @param content the name of the tables you are interested in, this is one string
#' @param page which page to get
#' @param page how many entries per page to get
#' @param billomatApiKey please provide your billomat Api key here
#' @param billomatID please provide your billomat ID here
#' @return the call returns a list with the header information

#' @export
curl_fetch_billomat <- function(content=c("articles",
                                          "clients",
                                          "offers",
                                          "invoices",
                                          "confirmations"),
                                page = 1,
                                per_page=per_page,
                                billomatApiKey = billomatApiKey,
                                billomatID = billomatID){
  call = paste0("https://",
                billomatID, ## this is the url of the site, but it contains all information necessary
                ".billomat.net/api/",
                content, # this s what i want
                "?api_key=", # this is my key
                billomatApiKey,
                "&per_page=", # this is my limit per page
                per_page,
                "&page=",
                page) # my page number
  # so for this to work, i have to cycle through all pages, as the number increases
  # i need to stop when reaching the last page -> how do I know how many pages there are?
  response <- httr::GET(call) # here I use the method a GET, this means I retrieve data
  # the response contains the header, the method and content as well as additional information
  remaining_limit = as.numeric(httr::headers(response)$`x-rate-limit-remaining`)
  # this tells me how many more calls are possible -- not important at this point
  body <- xml2::read_xml(response$content)
  # this is the content of this call for the results of page 1
  num_results <-  xml2::xml_length(body)
  # this tells me how many results are on each page, i can keep calling pages forever,
  # but they will have 0 length, which means I should stop at this point
  cat("\n",content,"page:",page,"\n")
  return(list(body=body))
}

#' retrieveData
#' this function carries out the call to get all the content of the call
#' @param content the name of the tables you are interested in, this is one string
#' @param per_page how many entries per page to get
#' @param billomatApiKey please provide your billomat Api key here
#' @param billomatID please provide your billomat ID here
#' @return the call returns a list with the header information
### ergänzen der fehlenden params
#' @export
retrieveData <- function(content,
                         per_page,
                         billomatApiKey = billomatApiKey,
                         billomatID = billomatID) {
  page_result1 <-
    curl_fetch_header(
      page = 1,
      content = content,
      per_page = per_page,
      billomatApiKey = billomatApiKey,
      billomatID = billomatID
    )
  how_many_entries <-page_result1$headers$`x-total-count` %>% as.numeric()
  max_pages <- ceiling(how_many_entries/per_page) # to know at what page to stop

  # this allows me to retrieve the data for all pages that are in .x
  all_pages <-
    purrr::map(.x = 1:max_pages,
               ~ curl_fetch_billomat(
                 page = .,
                 content = content,
                 per_page = per_page,
                 billomatApiKey = billomatApiKey,
                 billomatID = billomatID
               ))
}

#' extract_single_entry
#' this function extracts a single entry from an xml
#' @param entry_as_xml the name of the entry in an xml you are interested in
#' @return the call returns a dataframe which contains the id of the entry and all information in one long df
#' @export
extract_single_entry <- function(entry_as_xml) {
  entry_as_df <- unlist(entry_as_xml) %>% tibble::enframe()
  ids <- dplyr::filter(entry_as_df, name == "id") %>% pull(value)
  entry_as_df$ids <- as.character(ifelse(is.null(ids),as.integer(runif(1,0,100000)),ids))
  entry_as_df$name <- as.character(entry_as_df$name)
  entry_as_df
}


#' extract_xml
#' this function extracts a all entries from an xml
#' @param data the name of the entry in an xml you are interested in
#' @param list_num the index ot the list which contains the data
#' @return the call returns a dataframe which contains the id of the entry and all information in one long df
#' @export
extract_xml <-
  function(data,list_num) {

    purrr::map_dfr(as_list(data[[list_num]]$body)[[1]], ~extract_single_entry(.))

  }

#' extract_xml
#' this function extracts a all entries from an xml
#' @param data the name of the entry in an xml you are interested in
#' @param list_num the index ot the list which contains the data
#' @return the call returns a dataframe which contains the id of the entry and all information in one long df
#' @export
# function to store the data in the DB. Data is encrypted entry wise with symmetric encryption
retrieve_and_store_db <- function (content, encryption_key){
  data <-retrieveData(content,per_page = 50)
  # now I have the whole result from the get call in one big list of lists
  # i now need to turn this list into a tibble but keep the
  # first I extract the xml body as a list
  data_db<-purrr::map_df(1:length(data),~extract_xml(data, list_num = .), progress = TRUE, .id = "page")
  data_db$downloaded <- as.character(as_datetime((lubridate::now())))
  text <- paste0("DROP TABLE IF EXISTS ", content)
  # create a function to save the respective content
  DBI::dbExecute(billomatDB, text)
  content <- str_replace_all(pattern = c("-" ="_",
                                         "`" = "" ),string = content)
  DBI::dbWriteTable(billomatDB, content, data_db,overwrite = TRUE)
}

#' download_all_tables
#' this function download pulls the selected tables from the Billomat API
#' @param content the name of the entry in an xml you are interested in
#' @return the call returns a dataframe which contains the id of the entry and all information in one long df
#' @export
download_all_tables <- function(content){
  tables <-
    c(
      "invoices",
      "`invoice-items`",
      "confirmations",
      "`confirmation-items`",
      "clients",
      "offers",
      "`offer-items`",
      "articles",
      "`offer-tags`",
      "templates"
    )
  #per_page = 10
  # here I get the index of each table
  index_table<-which(tables %in% content)

  for(table in index_table){
    retrieve_and_store_db(tables[table], encryption_key=key)
  }
}

