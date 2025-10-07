

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

#' fetch_header_httr2
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
fetch_header_httr2 <- function(content=c("articles",
                                              "clients",
                                              "offers",
                                              "invoices",
                                              "confirmations"),
                                    billomatApiKey = billomatApiKey,
                                    billomatID = billomatID){

  req <- httr2::request(paste0("https://", billomatID, ".billomat.net/api/")) |>
    httr2::req_headers(`X-BillomatApiKey` = billomatApiKey) %>%
    httr2::req_url_path_append(content) %>%
    httr2::req_url_query(page = 1, per_page = 1)
  response <- req %>% httr2::req_perform()

  # the response contains the header, the method and content as well as additional information
  remaining_limit = as.numeric(httr2::resp_headers(response)$`x-rate-limit-remaining`)
  # this tells me how many more calls are possible -- not important at this point
  # this is the content of this call for the results of page 1
  num_results <- httr2::resp_headers(response)$`x-total-count`
  # this tells me how many results are on each page, i can keep calling pages forever,
  # but they will have 0 length, which means I should stop at this point
  cat(content,"header", "\n","total rows:",num_results,"remaining_limit:",remaining_limit)
  return(list(headers=httr2::resp_headers(response)))
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

#' retrieveData_withCheck
#' this function carries out the call to get all the content of the call with validation
#' @param content the name of the tables you are interested in, this is one string
#' @param per_page how many entries per page to get
#' @param billomatApiKey please provide your billomat Api key here
#' @param billomatID please provide your billomat ID here
#' @param logger a log4r logger object for logging validation results
#' @return the call returns a list with all pages and logs whether the expected row count matches the actual count
#' @export
retrieveData_withCheck <- function(content, per_page, billomatApiKey = billomatApiKey, billomatID = billomatID, logger = NULL){
    page_result1 <-
      curl_fetch_header(
      page = 1,
      content = content,
      per_page = per_page,
      billomatApiKey = billomatApiKey,
      billomatID = billomatID
    )
  expected_count <- page_result1$headers$`x-total-count` %>% as.numeric()
  max_pages <- ceiling(expected_count/per_page)

  all_pages <-
    purrr::map(.x = 1:max_pages,
               ~ curl_fetch_billomat(
                 page = .,
                 content = content,
                 per_page = per_page,
                 billomatApiKey = billomatApiKey,
                 billomatID = billomatID
               ))

   actual_count <- sum(purrr::map_int(all_pages, ~ xml2::xml_length(.x$body)))

   if (!is.null(logger)) {
      if (actual_count == expected_count) {
        log4r::info(logger, paste0(content, " is complete"))
      } else {
        log4r::warn(logger, paste0(content, " data incomplete. Expected: ", expected_count, ", Got: ", actual_count))
      }
    }

    return(all_pages)
}


#' fetch_all_entries
#' this function carries out the call to get all the content of the call
#' This is a new function using httr2
#' @param content the name of the tables you are interested in, this is one string
#' @param per_page how many entries per page to get
#' @param billomatApiKey please provide your billomat Api key here
#' @param billomatID please provide your billomat ID here
#' @return the call returns a list for each page
### ergänzen der fehlenden params
#' @export
fetch_all_entries <- function(billomatID, content, billomatApiKey, per_page = 100) {

  # get the header to see the max pages

  req <- httr2::request(paste0("https://", billomatID, ".billomat.net/api/")) |>
    httr2::req_headers(`X-BillomatApiKey` = keys$billomat[2]) %>%
    httr2::req_url_path_append(content)
  response <- req %>% httr2::req_perform()
  headers <- resp_headers(response)

  remaining_calls <- headers[["X-Rate-Limit-Remaining"]]

  # Display the remaining calls and rate limit
  cat("Remaining API calls:", remaining_calls, "\n")

  # get the total pages
  total_entries <- headers[["X-Total-Count"]] %>% as.numeric()
  max_page <- ceiling(total_entries / as.numeric(per_page))


  results <- list()   # To store all results
  page <- 1


  for (page in (1:max_page)) {
    print(paste0(page," of ",max_page))
    req <- httr2::request(paste0("https://", billomatID, ".billomat.net/api/")) |>
      httr2::req_headers(`X-BillomatApiKey` = billomatApiKey,
                         `Accept`= "xml") |>
      httr2::req_url_path_append(content) |>
      httr2::req_url_query(page = page, per_page = per_page) # Pagination params


    #  Perform the request and parse the response
    resp <- req |>
      httr2::req_perform() |>
      httr2::resp_body_xml()

    # Optionally inspect as a list
    list_resp <- xml2::as_list(resp)
    #
    results <- c(list_resp[[1]], results)
  }
  ## return the final list
  return(results)
}


#' extract_single_entry
#' this function extracts a single entry from an xml
#' @param entry_as_xml the name of the entry in an xml you are interested in
#' @return the call returns a dataframe which contains the id of the entry and all information in one long df
#' @export
extract_single_entry <- function(entry_as_xml) {
  entry_as_df <- unlist(entry_as_xml) %>% tibble::enframe()
  ids <- dplyr::filter(entry_as_df, name == "id") %>% dplyr::pull(value)
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

    purrr::map_dfr(xml2::as_list(data[[list_num]]$body)[[1]], ~extract_single_entry(.))

  }

#' extract_xml
#' this function extracts a all entries from an xml
#' @param data the name of the entry in an xml you are interested in
#' @param list_num the index ot the list which contains the data
#' @return the call returns a dataframe which contains the id of the entry and all information in one long df
#' @export
# function to store the data in the DB. Data is encrypted entry wise with symmetric encryption
retrieve_and_store_db <- function (content,
                                   billomatApiKey = billomatApiKey,
                                   billomatID = billomatID,
                                   billomatDB = billomatDB,
                                   encryption_key_db = encryption_key_db){
  data <-retrieveData(content,
                      per_page = 250,
                      billomatApiKey = billomatApiKey,
                      billomatID = billomatID)
  # now I have the whole result from the get call in one big list of lists
  # i now need to turn this list into a tibble but keep the
  # first I extract the xml body as a list
  data_db<-purrr::map_df(1:length(data),~extract_xml(data, list_num = .), progress = TRUE, .id = "page")
  data_db$downloaded <- as.character(lubridate::as_datetime((lubridate::now())))
  # create a function to save the respective content
  content <- stringr::str_replace_all(pattern = c("-" ="_",
                                         "`" = "" ),string = content)
  # delete the table if it exists
  if (DBI::dbExistsTable(billomatDB,name = content))
    {
  DBI::dbRemoveTable(billomatDB,name = content)
  }

  # write the new tables
  shinymanager::write_db_encrypt(conn = billomatDB,name =  content,value =  data_db,passphrase = encryption_key_db)
}


#' download_all_tables
#' this function download pulls the selected tables from the Billomat API
#' @param content the name of the entry in an xml you are interested in
#' @return the call returns a dataframe which contains the id of the entry and all information in one long df
#' @export
download_all_tables <- function(content,
                                billomatApiKey = billomatApiKey,
                                billomatID = billomatID,
                                billomatDB = billomatDB,
                                encryption_key_db = encryption_key_db){
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

  billomatDB <- DBI::dbConnect(RSQLite::SQLite(), billomatDB_path)

  for(table in index_table){
    retrieve_and_store_db(tables[table],
                          billomatApiKey = billomatApiKey,
                          billomatID = billomatID,
                          billomatDB = billomatDB,
                          encryption_key_db = encryption_key_db)
  }
  DBI::dbDisconnect(billomatDB)
}


#' set_status_endpoint
#'
#' this function sets the status of endpoints to the respective state. Important: for the confirmation / invoice to be completed
#' you must have a template id -- an invoice / confirmation without any items cannot be completed.
#'
#' @param ids an df with the ids for the document (invoice_id,confirmation_id)
#' @param status_to_set a vector to set to either for confirmations: "clear"|"unclear"|"cancel"|"uncancel"
#' for the invoice: "complete"
#' @export
set_status_endpoint <- function(df,
                                endpoint = "confirmations",
                                status_to_set,
                                billomat_api_key = billomatApiKey,
                                billomat_id = billomatID) {
  i <- 1
  ids <- unique(df$id)

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
          template_id = structure(list(df$template_id[i]))
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
    } else if (status_to_set == "clear"){
      # get the response
      response <- httr::PUT(url = api_endpoint,
                            httr::add_headers(header),
                            httr::accept("application/xml"))
    } else if (status_to_set == "delete"){
      # get the response
      response <- httr::DELETE(url = api_endpoint,
                            httr::add_headers(header),
                            httr::accept("application/xml"))
    } else {
      print("unknown status to set.")
    }



    if (response$status_code != 200) {
      print(paste0("failed to put", id))
    }

    total <- length(ids)
    print(paste0("Putting:", i, " of ", total))
    i <- i + 1
  }
}


#' create_contacts_tibble
#' Function takes the list wise returned values from the api call to get contacts and
#' turns them into a tibble
#' @param df_list a list where each entry is its own list.
#' @return the call returns a tibble
#' @export
create_contacts_tibble <- function(df_list){
  purrr::map(1:length(df_list), function(.) {

    contact_data <- df_list[[.]]  # Safely access contact data

    tibble(
      id = if (!is.null(contact_data[["id"]]) &&
               length(contact_data[["id"]]) > 0)
        contact_data[["id"]][[1]]
      else
        NA,
      client_id = if (!is.null(contact_data[["client_id"]]) &&
                      length(contact_data[["client_id"]]) > 0)
        contact_data[["client_id"]][[1]]
      else
        NA,
      created = if (!is.null(contact_data[["created"]]) &&
                    length(contact_data[["created"]]) > 0)
        contact_data[["created"]][[1]]
      else
        NA,
      updated = if (!is.null(contact_data[["updated"]]) &&
                    length(contact_data[["updated"]]) > 0)
        contact_data[["updated"]][[1]]
      else
        NA,
      name = if (!is.null(contact_data[["name"]]) &&
                 length(contact_data[["name"]]) > 0)
        contact_data[["name"]][[1]]
      else
        NA,
      deviating_invoice_recipient = if (!is.null(contact_data[["label"]]) &&
                                        length(contact_data[["label"]]) > 0)
        contact_data[["label"]][[1]]
      else
        NA,
      street = if (!is.null(contact_data[["street"]]) &&
                   length(contact_data[["street"]]) > 0)
        contact_data[["street"]][[1]]
      else
        NA,
      city = if (!is.null(contact_data[["city"]]) &&
                 length(contact_data[["city"]]) > 0)
        contact_data[["city"]][[1]]
      else
        NA,
      zip = if (!is.null(contact_data[["zip"]]) &&
                length(contact_data[["zip"]]) > 0)
        contact_data[["zip"]][[1]]
      else
        NA,
      country_code = if (!is.null(contact_data[["country_code"]]) &&
                         length(contact_data[["country_code"]]) > 0)
        contact_data[["country_code"]][[1]]
      else
        NA
    )
  }) %>% dplyr::bind_rows()
}

#' convert_list_to_tibble
#' General function to convert each contact into a tibble row
#' @param df_list a list where each entry is its own list.
#' @return the call returns a tibble
#' @export
convert_list_to_tibble <- function(df_list){
  ## take the list and get each individual entry
  purrr::map(1:length(df_list), function(.x) {
    ## only get the entry
    one_list_entry <- df_list[[.x]]  # Safely access data
    ## get all the names in this entry
    field_names <- names(one_list_entry)

    ## extract the data of fields
    extracted_data <- purrr::map(field_names, function(field) {
      # If field exists and has a value, return it; otherwise return NA
      if (!is.null(one_list_entry[[field]]) &&
          length(one_list_entry[[field]]) > 0) {
        return(one_list_entry[[field]][[1]])
      } else {
        return(NA)
      }
    })

    # Convert the named list to a tibble
    data_row <- rlang::set_names(extracted_data, field_names)

    # Return the row as a tibble, only keep one row per id
    dplyr::tibble(!!!data_row) %>%
      dplyr::distinct(id, .keep_all = TRUE)
  }) %>%

    dplyr::bind_rows()
}


#' create_new_database
#'
#' this function deletes the db if it exists and adds the new data as new table.
#'
#' @param billomatDB is an DBI object containing the path to the db
#' @param content is the name of the table
#' @param data_db the tibble containing the data
#' @param keys_db the encryption key for the database
#' @export
create_new_database <- function(billomatDB, content, data_db,keys_db){
  if (DBI::dbExistsTable(billomatDB, name = content))
  {
    DBI::dbRemoveTable(billomatDB, name = content)
    print(paste("deleted old db:",content))
  }

  # write the new tables
  shinymanager::write_db_encrypt(
    conn = billomatDB,
    name =  content,
    value =  data_db,
    passphrase = keys_db
  )
  print(paste("added new db:",content))
}

##### comments ------
#' get_comments
#'
#' this function pulls all the comments for a list of given ids and endpoint
#'
#' @param ids a vector of ids numbers
#' @param endpoint is the name of the endpoint
#' @export
get_comments <- function(ids,
                         endpoint = "confirmation",
                         billomatApiKey = billomatApiKey,
                         billomatID = billomatID) {
  i <- 1
  comments <- dplyr::tibble()
  for (id in ids) {
    response <- httr::GET(
      paste0(
        "https://",
        billomatID,
        ".billomat.net/api/",
        endpoint,"-comments",
        ## ich gebe mit ? die Parameter mit und dann mit & wird der Api key übermittelt
        "?",endpoint,"_id=",
        id,
        "&api_key=",
        billomatApiKey
      )
    )
    if (response$status_code != 200) {
      print(paste0("failed to get comment for ", id))
    }

    total <- length(ids)

    # get the body of the response which contains all the information and turn it to a list
    data_xml <- xml2::as_list(xml2::read_xml(response$content))

    print(paste0("Getting comment:",i," of ",total))
    i <- i + 1

    endpoint_comment <- paste0(endpoint,"-comments")
    # create a tibble from the list that contains the field names
    comment <- tibble::as_tibble(data_xml) %>%

      # I unnest so that it keeps the node name as id
      tidyr::unnest_longer(col = !!sym(endpoint_comment)) %>%
      # I unnest only the values of the confirmation-comments column
      tidyr::unnest(cols = !!sym(endpoint_comment)) %>%
      # only keep relevant information
      dplyr::filter(!!sym(
        paste0(endpoint_comment,"_id")
      )%in% c("id",paste0(endpoint,"_id"),"created","comment")) %>%

      # turn the table wider so each comment has the all information in one row
      tidyr::pivot_wider(names_from = !!sym(paste0(endpoint_comment,"_id")),
                         values_from = !!sym(endpoint_comment),
                         values_fn = list) %>%

      # because all cols are a list I have to unnest everything
      tidyr::unnest(cols = everything())

    # append the new comment to the existing comments
    comments <- dplyr::bind_rows(comments,
                                 comment)
  }
  # return all comments
  return(comments)
}

#### client properties ------


#' extract_single_entry_client_properties
#'
#' this function extracts the properties of a single entry
#'
#' @param entry_as_xml is the data of one entry as an xml

#' @export
extract_single_entry_client_properties <- function(entry_as_xml) {
  # this function takes a single entry and turns it into a dataframe, it is a helper function to create entries for page
  entry_as_df <- unlist(entry_as_xml) %>% tibble::enframe()
  ids <- dplyr::filter(entry_as_df, name == "client-property-value.client_id") %>% dplyr::pull(value)
  id_property <- dplyr::filter(entry_as_df, name == "client-property-value.client_property_id") %>% dplyr::pull(value)

  entry_as_df$ids <- as.character(ifelse(is.null(ids),as.integer(runif(1,0,100000)),ids))
  entry_as_df$id_property <- as.character(ifelse(is.null(id_property),as.integer(runif(1,0,100000)),id_property))
  entry_as_df$property_name <- dplyr::filter(entry_as_df, name == "client-property-value.name") %>% dplyr::pull(value)
  entry_as_df
}

#' create_entries_for_page
#'
#' this function uses
#'
#' @param entry_as_xml is the data of one entry as extract_single_entry_client_properties

#' @export
create_entries_for_page <- function(entry_as_xml) {
  # this function takes all entries on a page and creates one large table binding individual entries
  all_entries_this_page <-
    purrr::map_dfr(
      1:length(entry_as_xml),
      ~ extract_single_entry_client_properties(entry_as_xml[.])
    )
  return(all_entries_this_page)
}

#' create_entries_for_all_pages
#'
#' this function uses the create_entries_for_page an purrrs over all pages
#'
#' @param data are all pages that are retrieved as xml

#' @export
create_entries_for_all_pages <- function(data) {

  purrr::map_dfr(1:length(data), ~ create_entries_for_page(xml2::as_list(data[[.]]$body)[[1]]))
}


#' get_all_client_properties
#'
#' this function downloads all client properties and saves them in the billomat db
#'
#' @param content the name of the tables you are interested in, this is one string
#' @param per_page how many entries per page to get
#' @param billomatApiKey please provide your billomat Api key here
#' @param billomatID please provide your billomat ID here
#' @export
get_all_client_properties <-function(content = "`client-property-values`",
                                     billomatApiKey = billomatApiKey,
                                     billomatID = billomatID,
                                     billomatDB = billomatDB,
                                     encryption_key_db = encryption_db){


  data <- Billomatics::retrieveData(content = content,
                                    per_page = 250,
                                    billomatApiKey = billomatApiKey,
                                    billomatID = billomatID)
  data_db <- create_entries_for_all_pages(data)

  data_db$downloaded <- as.character(lubridate::as_datetime((lubridate::now())))
  # create a function to save the respective content
  content <- stringr::str_replace_all(pattern = c("-" ="_",
                                                  "`" = "" ),string = content)
  ## connect to the db
  billomatDB <- DBI::dbConnect(RSQLite::SQLite(), billomatDB_path)

  # delete the table if it exists
  if (DBI::dbExistsTable(billomatDB,name = content)){
    DBI::dbRemoveTable(billomatDB,name = content)
  }

  # write the new tables
  shinymanager::write_db_encrypt(conn = billomatDB,name =  content,value =  data_db,passphrase = encryption_key_db)
  DBI::dbDisconnect(billomatDB)
}

##### Post Values ------
#' post_client_value
#'
#' this function posts values to a client and the respective table
#'
#' @param property_id the id for the property id that is to be changed
#' @param client_db this is the table with the client db that contains a client id and the value to be posted
#' @param client_ids a list of all client_ids that we want to put
#' @param billomatApiKey please provide your billomat Api key here
#' @param billomatID please provide your billomat ID here
#' @param value2post the string with the name of column name that contains the value
#' @param endpoint the string with the name of the endpont where you want to post the value

#' @export
post_client_value <- function(property_id = 18444,
                              client_db = df,
                              client_ids = list_of_ids,
                              billomatApiKey = billomatApiKey,
                              billomatID = billomatID,
                              value2post = "debitor_jp5",
                              endpoint = "client-property-values") {
  for (id in client_ids) {
    # get the respective client_id from the loop and pass it to the table with client ID and debitornumber
    post_value <- client_db %>%
      dplyr::filter(id == .$client_id) %>%
      dplyr::pull(get(value2post))



    # create the xml file for the individual body
    body <- list(
      client_property_value = list(
        client_id = structure(list(id)),
        client_property_id = structure(list(property_id)),
        value = structure(list(post_value))
      )
    ) %>%
      # turn the list into an xml
      xml2::as_xml_document(.) %>%
      # create a string from the xml
      paste0(.)


    # pass the url
    URI <-paste0("https://",billomatID,".billomat.net/api/",endpoint)

    # pass the header
    headers <- c("X-BillomatApiKey" = billomatApiKey,
                 "Content-Type" = "application/xml")

    # make the call and post
    response <- httr::POST(URI,
                           config = httr::add_headers(headers),
                           body = body)

    # handle status code not 201
    if (response$status_code != 201) {
      print
      print(paste0("could not post:", id))
    }
  }
}

##### Post resource  ------
#' Post a resource to Billomat API
#'
#' This function posts a resource to the Billomat API with specified values.
#'
#' @param billomat_id The Billomat account ID.
#' @param billomat_api_key The Billomat API key.
#' @param resource The resource you want to post (e.g., "invoice", "client", etc.). Always resource in singular!
#' @param ... Additional values corresponding to the resource (see Billomat API documentation).
#' @return A list containing the response from the API.

#' @export
post_resource <- function(billomat_id, billomat_api_key, resource, ...){
  #set the api endpoint
  api_endpoint <- paste0("https://", billomat_id, ".billomat.net/api/", resource, "s")

  # set header
  header <- c("X-BillomatApiKey" = billomat_api_key,
              "Content-Type" = "application/json")

  # set body
  body_values <- list()
  body_values[[resource]] <- list(...)

  # make the POST call
  response <- POST(url = api_endpoint,
                   add_headers(header),
                   body = toJSON(body_values),
                   accept("application/json")
  )

  # print the status code of the request
  print(paste("Der Statuscode:", response[["status_code"]]))

  if(response[["status_code"]] != 201){
    print(content[["errors"]])
  }

  return(response)

}

##### Put resource values  ------
#' Put values to a resource via Billomat API
#'
#' This function puts values to a selected resource via Billomat API with specified values.
#'
#' @param billomat_id The Billomat account ID.
#' @param billomat_api_key The Billomat API key.
#' @param resource The resource you want to put (e.g., "invoice", "client", etc.). Always resource in singular!
#' @param entity_id The entity id of your object. Findable in the URL.
#' @param ... Additional values corresponding to the resource (see Billomat API documentation).
#' @return A list containing the response from the API.

#' @export
put_resource_values <- function(billomat_id, billomat_api_key, resource, entity_id, ...){
  #set the api endpoint
  api_endpoint <- paste0("https://", billomat_id, ".billomat.net/api/", resource, "s/", entity_id)

  # set header
  header <- c("X-BillomatApiKey" = billomat_api_key,
              "Content-Type" = "application/json")

  # set body
  body_values <- list()
  body_values[[resource]] <- list(...)

  # make the PUT call
  response <- PUT(url = api_endpoint,
                  add_headers(header),
                  body = toJSON(body_values),
                  accept("application/json")
  )

  content <- content(response)

  # print the status code of the request
  print(paste("Der Statuscode:", response[["status_code"]]))

  if(response[["status_code"]] != 200){
    print(content[["errors"]])
  }

  return(response)

}

##### Write comments   ------
#' Posts comments to a resource via Billomat API
#'
#' This function posts comments to a selected resource via Billomat API with the desired comment.
#'
#' @param billomat_id The Billomat account ID.
#' @param billomat_api_key The Billomat API key.
#' @param comment The comment you want to post.
#' @param resource The resource you want to put (e.g., "invoice", "client", etc.). Always resource in singular!
#' @param entity_id The entity id of your object. Findable in the URL.
#' @return A list containing the response from the API.

#' @export
post_comment <- function(billomat_id, billomat_api_key, comment, resource, entity_id){

  # set the api endpoint for the desired resource
  api_endpoint <- paste0("https://", billomat_id, ".billomat.net/api/", gsub("_", "-", resource), "-comments")

  # set the header with format to json
  header <- c("X-BillomatApiKey" = billomat_api_key,
              "Content-Type" = "application/json")

  # set the body
  resource_comment <- paste0(gsub("_", "-", resource), "-comment")
  resource_id <- paste0(gsub("-", "_", resource), "_id")

  comment_data <- list()
  comment_data[[resource_comment]] <- list()
  comment_data[[resource_comment]][[resource_id]] <- entity_id
  comment_data[[resource_comment]][["comment"]] <- comment


  # send the POST request
  response <- POST(url = api_endpoint,
                   add_headers(header),
                   body = toJSON(comment_data),
                   accept("application/json")
  )

  content <- content(response, "parsed")

  # print the status code of the request
  print(paste("Der Statuscode:", response[["status_code"]]))

  if(response[["status_code"]] != 201){
    print(content[["errors"]])
  }

  return(response)

}

#### Post invoices to billomat ----

#' post_single_invoice
#' post a single invoice to the billomat. Information for the xml has to be passed via xml dict.

#' @param billomatID the billomat ID#'
#' @param billomat_api_key The Billomat API key.
#' @param endpoint The endpoint you want to post (e.g., "invoices", "clients", etc.). Always resource in plural!
#' @param entity_id The entity id of your object. Findable in the URL.
#' @param ... Additional values corresponding to the resource (see Billomat API documentation).
#' @return returns a list containing the invoice_id, confirmation_id and invoice_date

#' @export
post_single_invoice <-
  function(billomatID,
           billomatApiKey,
           endpoint = "invoices",
           client_id,
           contact_id,
           confirmation_id,
           invoice_intro,
           reduction,
           invoice_date,
           note = "") {
    # This function posts a single invoice main document

    #### create the invoice body to be posted -
    body <- list(
      invoice = list(
        client_id = structure(list(client_id)),
        contact_id = structure(list(contact_id)),
        confirmation_id = structure(list(confirmation_id)),
        date = structure(list(invoice_date)),
        intro = structure(list(invoice_intro)),
        reduction = structure(list(reduction)),
        note = structure(list(note))
      )
    ) %>% xml2::as_xml_document(.) %>%
      # create a string from the xml
      paste0(.)
    # turn the list into an xml


    # pass the url
    URI <-
      paste0("https://", billomatID, ".billomat.net/api/", endpoint)

    # pass the header
    headers <- c("X-BillomatApiKey" = billomatApiKey,
                 "Content-Type" = "application/xml")

    # make the call and post
    response <- httr::POST(URI,
                           config = httr::add_headers(headers),
                           body = body)

    # handle status code not 201
    if (response$status_code != 201) {
      print(paste0(
        "could not create invoice for confirmation:",
        confirmation_id
      ))
    }

    invoice_id <- httr::content(response, as = "parsed") %>%
      # turn the response into a list
      xml2::as_list(.) %>%
      # keep only the invoice entries
      .$invoice %>%
      # looking at the id
      .$id %>%
      # keeping the id
      unlist(.)

    # return the invoice_id, confirmation_id and invoice_date
    list(invoice_id = invoice_id,
         confirmation_id = confirmation_id,
         invoice_date = invoice_date)
  }

#' post_all_invoices
#' This function uses the post single invoice function and purrrs over a list of confirmations
#' to turn them into invoices. The function returns a tibble with the confirmation_id and invoice_id
#'
#' @param confirmations2post a df with all the confirmations that will be posted
#' @param billomatID the billomat ID#'
#' @param billomat_api_key The Billomat API key.
#'
#' @export
post_all_invoices <- function(confirmations2post,
                              billomatID,
                              billomatApiKey){
  purrr::map_dfr(
    1:nrow(confirmations2post),
    ~ post_single_invoice(
      billomatID = billomatID,
      billomatApiKey = billomatApiKey,
      client_id = confirmations2post$client_id[.],
      contact_id = confirmations2post$contact_id[.],
      confirmation_id = confirmations2post$id[.],
      invoice_date = confirmations2post$invoice_date[.],
      invoice_intro = confirmations2post$invoice_intro[.],
      reduction = confirmations2post$reduction[.],
      note = confirmations2post$note[.]
    ),.progress = TRUE
  )
}

#' post_single_invoice_item
#'
#' this function needs to move through the rows in of the invoice table and create the single invoice
#'
#' @param df_items the df containing the items that should be part of the respective invoice already posted.
#' The df must contain the invoice_id
#' @param billomatID the billomat ID
#' @param billomat_api_key The Billomat API key.
#' @param endpoint The endpoint you want to post (e.g., "invoice-items", "confirmation-items"). Always resource in plural!
#' @param entity_id The entity id of your object. Findable in the URL.
#' @param ... Additional values corresponding to the resource (see Billomat API documentation).
#' @return returns a list containing posted_invoice_id
#'
#' @export
post_single_invoice_item <- function(df_items,
                                     billomatApiKey,
                                     billomatID,
                                     endpoint = "invoice-items") {
  #df <- invoice_confirmation_items[1,]
  body <- list(

    `invoice-item` = list(
      invoice_id = structure(list(df_items$invoice_id)),

      title = structure(list(
        df_items$title
      )),

      article_id = structure(list(
        df_items$article_id
      )),

      unit_price = structure(list(
        df_items$unit_price
      )),

      quantity = structure(list(
        df_items$quantity
      )),

      description = structure(list(
        df_items$description
      )),

      reduction = structure(list(
        df_items$reduction
      ))
    )
  ) %>% xml2::as_xml_document(.) %>%
    # create a string from the xml
    paste0(.)
  # turn the list into an xml

  # pass the url
  URI <-
    paste0("https://", billomatID, ".billomat.net/api/", endpoint)

  # pass the header
  headers <- c("X-BillomatApiKey" = billomatApiKey,
               "Content-Type" = "application/xml")

  # make the call and post
  response <- httr::POST(URI,
                         config = httr::add_headers(headers),
                         body = body)

  # handle status code not 201
  if (response$status_code != 201) {
    print(paste0("could not create invoice item for invoice:", df_items$invoice_id))
  }

  posted_invoice_id <- httr::content(response, as = "parsed") %>%
    # turn the response into a list
    xml2::as_list(.) %>%
    # keep only the invoice entries
    .$invoice %>%
    # looking at the id
    .$id %>%
    # keeping the id
    unlist(.)

  # push the invoice id into the global environment
  print(list(posted_invoice_id = df_items$invoice_id))
  posted_invoice_id
}

#' post_all_invoice_items
#' This function uses the single invoice item fun and  uses it on a

#' @param df_items the df containing the items that should be part of the respective invoice already posted.
#' The df must contain the invoice_id
#' @param billomatID the billomat ID
#' @param billomat_api_key The Billomat API key.
#'
#' @return  The function returns a tibble with the posted confirmation_id and invoice_id
#'
#' @export
post_all_invoice_items <- function(df_items,
                                   billomatApiKey,
                                   billomatID
) {
  df_items <- df_items %>%
    tidyr::drop_na(invoice_id)
  # loop over all invoice ids in the posted_invoice_id table and create the invoice items of one invoice
  for (invoice_ids in unique(df_items$invoice_id)) {
    #invoice_ids = 18234948
    # only keep the confirmation items which are part of the confirmation id being billed
    invoice_items <- df_items %>%
      dplyr::filter(invoice_id == invoice_ids) %>%
      dplyr::select(
        position,
        confirmation_id,
        invoice_id,
        id,
        title,
        article_id,
        quantity,
        unit_price,
        description,
        reduction
      )

    # iterate over all positions to create each individual invoice item
    for (item in 1:length(invoice_items$position)) {
      # item = 1
      post_single_invoice_item(df = invoice_items[item, ],
                               billomatApiKey,
                               billomatID)

    }
  }
}


#' post_complete_invoices
#'
#' This function takes the confirmations and items and turns them into an invoice
#' @param df_items the df containing the items that should be part of the respective invoice already posted.
#' The df must contain the invoice_id
#' @param billomatID the billomat ID
#' @param billomat_api_key The Billomat API key.
#' @return  The functions returns a tibble with all posted ids
#'
#' @export
post_complete_invoices <- function(billomatID,
                                   billomatApiKey,
                                   confirmations2post_df,
                                   confirmations2post_items_df) {

  # get content of the response to find the newly created id of the invoice
  posted_invoice_ids <- post_all_invoices(confirmations2post_df,
                                          billomatID = billomatID,
                                          billomatApiKey = billomatApiKey)



  # create the invoice-item table
  # This has to consider the month of the bill - so I have to return the month
  invoice_confirmation_items <- confirmations2post_items_df %>% #confirmations2post_items %>%
    left_join(posted_invoice_ids,
              by = c("confirmation_id","invoice_date"),
              relationship = "many-to-many")%>%
    mutate(
      description = description,
      unit_price = as.numeric(total_net_unreduced) / as.numeric(quantity),
      reduction = paste0(
        round(
          (as.numeric(total_net_unreduced) - as.numeric(total_net)) /
            as.numeric(total_net_unreduced)*100,2),"%"),
      reduction = replace_na(reduction, "0%")
    )


  post_all_invoice_items(df_items = invoice_confirmation_items,
                         billomatID = billomatID,
                         billomatApiKey = keys$billomat[2])

  posted_invoice_ids
}



