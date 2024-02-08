

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


#' clear_confirmations
#'
#' this function sets the status of confirmations to cleared
#'
#' @param confirmation_ids an array containing AB numbers
#' @param status_to_set a vector to set to either "clear"|"unclear"|"cancel"|"uncancel" depending on what state the confirmation should have

#' @export
set_status_confirmations <- function(confirmation_ids,status_to_set, billomatApiKey = billomatApiKey, billomatID = billomatID) {
  i <- 1

  status2set <- paste0("/",status_to_set)
  for (confirmation_id in confirmation_ids) {

    api_endpoint <- paste0("https://",
                           billomat_id,
                           ".billomat.net/api/confirmations/",
                           confirmation_id,
                           status2set)

    # set the header
    header <- c("X-BillomatApiKey" = billomat_api_key,
                "Content-Type" = "application/xml")

    # get the response
    response <- httr::PUT(url = api_endpoint,
                    httr::add_headers(header),
                    httr::accept("application/xml")
    )


    if (response$status_code != 200) {
      print(paste0("failed to put", confirmation_id))
    }

    total <- length(confirmation_ids)
    print(paste0("Putting:",i," of ",total))
    i <- i + 1
  }
}

##### comments ------
#' get_comments
#'
#' this function pulls all the comments for a list of given ids
#'
#' @param confirmation_ids a vector of ids numbers

#' @export
get_comments <- function(confirmation_ids,
                         billomatApiKey = billomatApiKey,
                         billomatID = billomatID) {
  i <- 1
  comments <- dplyr::tibble()
  for (confirmation_id in confirmation_ids) {
    response <- httr::GET(
      paste0(
        "https://",
        billomatID,
        ".billomat.net/api/confirmation-comments",
        ## ich gebe mit ? die Parameter mit und dann mit & wird der Api key übermittelt
        "?confirmation_id=",
        confirmation_id,
        "&api_key=",
        billomatApiKey
      )
    )
    if (response$status_code != 200) {
      print(paste0("failed to get comment", confirmation_id))
    }

    total <- length(confirmation_ids)

    # get the body of the response which contains all the information and turn it to a list
    data_xml <- xml2::as_list(xml2::read_xml(response$content))

    print(paste0("Getting comment:",i," of ",total))
    i <- i + 1
    # create a tibble from the list that contains the field names
    comment <- tibble::as_tibble(data_xml) %>%

      # I unnest so that it keeps the node name as id
      tidyr::unnest_longer(col = `confirmation-comments`) %>%
      # I unnest only the values of the confirmation-comments column
      tidyr::unnest(cols = c(`confirmation-comments`)) %>%
      # only keep relevant information
      dplyr::filter(`confirmation-comments_id`%in% c("id","confirmation_id","created","comment")) %>%

      # turn the table wider so each comment has the all information in one row
      tidyr::pivot_wider(names_from = `confirmation-comments_id`,
                         values_from = `confirmation-comments`,
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
  api_endpoint <- paste0("https://", billomat_id, ".billomat.net/api/", resource, "-comments")

  # set the header with format to json
  header <- c("X-BillomatApiKey" = billomat_api_key,
              "Content-Type" = "application/json")

  # set the body
  resource_comment <- paste0(resource, "-comment")
  resource_id <- paste0(resource, "_id")

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


