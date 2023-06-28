#################################################################################################################################################
# Update der Billomat Datenbank
#################################################################################################################################################
# Author: Tobias Aufenanger & Johannes Leder
# Date: 2022/12
#################################################################################################################################################
#delete everything
rm(list=ls())



#Settings
############################################
#includes

#libraries
library(tidyverse)
library(xml2)
library(httr)
library(utils)
library(DBI)
library(lubridate)
library(safer)

dbListTables(billomatDB)
df <- dbReadTable(billomatDB, "invoices")

#Data and Variables
############################################

key = readline(prompt = "decryption_key: \n")
message("key is set to: ", key)
billomatDB_path <- "metabase.db\\billomat.db"
billomatID <- "k16917022"
encrypted_ApiKey <- readLines("key\\billomat.txt")
billomatApiKey <- decrypt_string(encrypted_ApiKey, key=key)
# manual at: https://www.billomat.com/api/

############################################
#start----
############################################
billomatDB <- dbConnect(RSQLite::SQLite(), billomatDB_path)
# dbListTables(billomatDB)

#### curl functions ------
#this function carries out the call, the most important thing to pass as arguments
# content and page, the rest is fixed
# but do i need the page even from the outside? not really, since this can change..
curl_fetch_header <- function(content=c("articles",
                                          "clients",
                                          "offers",
                                          "invoices",
                                          "confirmations"),
                                page = 1,
                                per_page=1){
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
  response <- GET(call) # here I use the method a GET, this means I retrieve data
  # the response contains the header, the method and content as well as additional information
  remaining_limit = as.numeric(headers(response)$`x-rate-limit-remaining`) 
  # this tells me how many more calls are possible -- not important at this point
  body <- read_xml(response$content) 
  # this is the content of this call for the results of page 1
  num_results <- headers(response)$`x-total-count`
  # this tells me how many results are on each page, i can keep calling pages forever, 
  # but they will have 0 length, which means I should stop at this point 
  cat(content,"header", "\n","total rows:",num_results,"remaining_limit:",remaining_limit)
  return(list(headers=headers(response)))
}

curl_fetch_billomat <- function(content=c("articles",
                                          "clients",
                                          "offers",
                                          "invoices",
                                          "confirmations"),
                                page = 1,
                                per_page=per_page){
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
  response <- GET(call) # here I use the method a GET, this means I retrieve data
  # the response contains the header, the method and content as well as additional information
  remaining_limit = as.numeric(headers(response)$`x-rate-limit-remaining`) 
  # this tells me how many more calls are possible -- not important at this point
  body <- read_xml(response$content) 
  # this is the content of this call for the results of page 1
  num_results <- xml_length(body)
  # this tells me how many results are on each page, i can keep calling pages forever, 
  # but they will have 0 length, which means I should stop at this point 
  cat("\n",content,"page:",page,"\n")
  return(list(body=body))
}
# so now i have a function that can retrieve the content of a page

# now i have all information in the resulting list, but how do i know how many pages to call? and when do I stop?
# I just call all the information of the header and then use the number of pages and amount of information to determine the last page

# I now need to use this function and retrieve multiple pages and keep the results
content <- "clients"
retrieveData <- function(content,per_page) {
  page_result1 <- curl_fetch_header(page = 1,content = content)
  how_many_entries <-page_result1$headers$`x-total-count` %>% as.numeric()
  max_pages <- ceiling(how_many_entries/per_page) # to know at what page to stop
  
  # this allows me to retrieve the data for all pages that are in .x
  all_pages <- purrr::map(.x = 1:max_pages, ~ curl_fetch_billomat(page = .,content = content,per_page = per_page))
}


extract_single_entry <- function(entry_as_xml) {
  entry_as_df <- unlist(entry_as_xml) %>% enframe()
  ids <- filter(entry_as_df, name == "id") %>% pull(value)
  entry_as_df$ids <- as.character(ifelse(is.null(ids),as.integer(runif(1,0,100000)),ids))
  entry_as_df$name <- as.character(entry_as_df$name)
  entry_as_df
}
#purrr::map_dfr(as_list(all_pages[[1]]$body)[[1]], ~extract_single_entry(.))

## this function takes apart the xml data and returns a long df with an id of each entry and the key as well as value
extract_xml <-
  function(data,list_num) {
 
  purrr::map_dfr(as_list(data[[list_num]]$body)[[1]], ~extract_single_entry(.))
       
  }

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
  dbExecute(billomatDB, text)
  content <- str_replace_all(pattern = c("-" ="_",
                                         "`" = "" ),string = content)
  dbWriteTable(billomatDB, content, data_db,overwrite = TRUE)
}

#### getting data -----------------------

## invoices
# here we can just enter the content we need, or use the functions below to get it all
#content = "invoices"
#retrieve_and_store_db(content)
## HOW CAN WE ONLY GET THE DATA THAT IS MOST RECENT?
download_all_tables <- function(){
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

for(table in tables[1:length(tables)]){
  retrieve_and_store_db(table, encryption_key=key)
}
}

download_all_tables <- function(){
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
  
  for(table in tables[1:length(tables)]){
    retrieve_and_store_db(table, encryption_key=key)
  }
}
## getting data
# a=dbGetQuery(billomatDB, "SELECT * FROM clients")
# select the relevant keys before you run the next command -- important: This should happen in a separate file.
#invoices <- create_dataset(a)
# 



dbDisconnect(billomatDB)
