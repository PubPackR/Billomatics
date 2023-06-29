

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
#'
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
  remaining_limit = as.numeric(headers(response)$`x-rate-limit-remaining`)
  # this tells me how many more calls are possible -- not important at this point
  body <- xml2::read_xml(response$content)
  # this is the content of this call for the results of page 1
  num_results <-  xml2::xml_length(body)
  # this tells me how many results are on each page, i can keep calling pages forever,
  # but they will have 0 length, which means I should stop at this point
  cat("\n",content,"page:",page,"\n")
  return(list(body=body))
}
# so now i have a function that can retrieve the content of a page

# now i have all information in the resulting list, but how do i know how many pages to call? and when do I stop?
# I just call all the information of the header and then use the number of pages and amount of information to determine the last page

# I now need to use this function and retrieve multiple pages and keep the results

#' this function carries out the call to get the header
#' retrieveData
#' this function
#' @param content the name of the tables you are interested in, this is one string
#' @param page which page to get
#' @param page how many entries per page to get
#' @param billomatApiKey please provide your billomat Api key here
#' @param billomatID please provide your billomat ID here
#' @return the call returns a list with the header information

#' @export
retrieveData <- function(content,per_page) {
  page_result1 <- curl_fetch_header(page = 1,content = content)
  how_many_entries <-page_result1$headers$`x-total-count` %>% as.numeric()
  max_pages <- ceiling(how_many_entries/per_page) # to know at what page to stop

  # this allows me to retrieve the data for all pages that are in .x
  all_pages <- purrr::map(.x = 1:max_pages, ~ curl_fetch_billomat(page = .,content = content,per_page = per_page))
}
