# -------------------------- Start script --------------------------------

#' get_central_station_contacts
#'
#' this function calls the crm api and downloads all fields that belong to a contact.
#' The notes and protocols are NOT downloaded.

#' @param api_key the api key you have to provide
#' @param pages the setting to call all pages (default), or a specific number of pages (one page includes 250 entries)
#' @return the tibble which contains all information, this can be stored in a single vector or lists

#' @export
get_central_station_contacts <- function (api_key, pages = "all") {
  # if the pages are set to "all" then all pages will be downloaded
  # if pages is set to a specific number, then this number of pages will be downloaded
  # define header
  headers <- c(
    "content-type" = "application/json",
    "X-apikey" = api_key,
    "Accept" = "*/*"
  )

  #define url to count all people
  url <- "https://api.centralstationcrm.net/api/people/count"

  # create access with httr GET-function
  response <- httr::GET(url, httr::add_headers(headers))

  # make response answer readable with jsonlite::fromJSON
  data <- jsonlite::fromJSON(httr::content(response, "text"))

  # save the answer in the variable number_of_persons
  number_of_persons <- data$total_entries
  # if the pages is set to "all" then the ceiling of all persons / 50 is the max number of pages
  if (pages == "all") {
    pages <- ceiling(number_of_persons / 250)
  } else {
    # if the pages is set to a specific number
    pages <- pages
  }

  #number_of_persons <- 200
  ################################################################################

  persons <- tidyr::tibble() # create an data.frame for persons

  # set url with "page=" so you can add endpoints
  url <-
    "https://api.centralstationcrm.net/api/people?perpage=250&page="

  for (i in 1:pages) {
    print(paste0("Page:", i, " of ", ceiling(number_of_persons / 250)))
    # create an response with httr and the GET function. In the function you paste
    # url and your endpoint and you also need the headers
    response <-
      httr::GET(
        paste0(
          url,
          i,
          "&includes=companies%20tags%20comments%20emails%20tels%20positions%20tasks%20tasks_pending%20custom_fields%20addrs_from_company&methods=all"
        ),
        httr::add_headers(headers)
      )
    #response <- httr::GET(paste0(url, i, "&includes=all&methods=all"),
    #                      add_headers(headers))

    # make response answer readable with jsonlite::fromJSON
    data <- jsonlite::fromJSON(httr::content(response, "text"))

    # check if the data already exists in "persons" to avoid duplicates
    if (!identical(data, persons[nrow(persons),])) {
      # if the data is not yet available then add to persons
      persons <- dplyr::bind_rows(persons, data)
    }
  }
  persons %>% tidyr::unnest(person)
}



#' get_central_station_cal_events
#'
#' this function calls the crm api and downloads the all person_ids with one defined tag and the time it was set.

#' @param api_key the api key you have to provide
#' @param tag_to_export the setting to set a tag-string for which all person_ids should be loaded
#' @param pages the setting to set a maximum number of pages to download, with 1000 pages as default (one page includes 250 entries)
#' @return the tibble which contains all person_ids with the defined tag, the dates when it was created and updated and the method and account who assigned it

#' @export
get_central_station_single_tags <- function (api_key, tag_to_export, pages = 1000) {
  # if no page number is set then a maximum of 1000 pages will be downloaded
  # the export stops, when not more data is available
  # if pages is set to a specific number, then this number of pages will maximum be downloaded
  # define header
  headers <- c(
    "content-type" = "application/json",
    "X-apikey" = api_key,
    "Accept" = "*/*"
  )

  # create an data.frame for the tag data
  tag_data <- tidyr::tibble()

  for (i in 1:pages) {
    # create an response with httr and the GET function. In the function you paste
    # url and your endpoint and you also need the headers
    response <-
      httr::GET(
        paste0(
          "https://api.centralstationcrm.net/api/tags?perpage=250&page=",
          i,
          "&filter%5Bname%5D[equal]=",
          tag_to_export
        ),
        httr::add_headers(headers)
      )

    # make response answer readable with jsonlite::fromJSON
    data <- jsonlite::fromJSON(httr::content(response, "text"))

    # check if data could be loaded for requested page
    if (length(data) == 0) {
      # stop export if no data could be loaded or everything is already loaded
      break
    } else {
      #get correct number of exported data
      print(paste0("Exported ", ifelse(nrow(data) == 250, i*250, ((i-1)*250+nrow(data))), " tags"))
      # check if the data already exists in "tag_data" to avoid duplicates
      if (!identical(data, tag_data[nrow(tag_data), ])) {
        # if the data is not yet available then add to tag_data
        tag_data <- dplyr::bind_rows(tag_data, data)
      }
    }

    if (i == pages) {
      # stop export after maximum number of pages was loaded
      print("Only part of the available data was exported")
      break
    }
  }

  # unnest people and filter only tag_data with customers as participants
  tag_data <- tag_data %>%
    tidyr::unnest(tag) %>%
    dplyr::rename(people_id = attachable_id)
}



#' get_central_station_cal_events
#'
#' this function calls the crm api and downloads the cal_events (past and future) to all contact.

#' @param api_key the api key you have to provide
#' @param pages the setting to set a maximum number of pages to download, with 1000 pages as default (one page includes 250 entries)
#' @return the tibble which contains all information of every member at the cal_events

#' @export
get_central_station_cal_events <- function (api_key, pages = 1000) {
  # if no page number is set then a maximum of 1000 pages will be downloaded
  # the export stops, when not more data is available
  # if pages is set to a specific number, then this number of pages will maximum be downloaded
  # define header
  headers <- c(
    "content-type" = "application/json",
    "X-apikey" = api_key,
    "Accept" = "*/*"
  )

  # create an data.frame for cal_events
  cal_events <- tidyr::tibble()

  for (i in 1:pages) {
    # create an response with httr and the GET function. In the function you paste
    # url and your endpoint and you also need the headers
    response <-
      httr::GET(
        # set url with "page=" so you can add endpoints
        paste0(
          "https://api.centralstationcrm.net/api/cal_events?perpage=250&page=",
          i,
          "&includes=people"
        ),
        httr::add_headers(headers)
      )

    # make response answer readable with jsonlite::fromJSON
    data <- jsonlite::fromJSON(httr::content(response, "text"))

    # check if data could be loaded for requested page
    if (length(data) == 0) {
      # stop export if no data could be loaded or everything is already loaded
      break
    } else {
      #get correct number of exported data
      print(paste0("Exported ", ifelse(nrow(data) == 250, i*250, ((i-1)*250+nrow(data))), " cal_events"))
      # check if the data already exists in "cal_events" to avoid duplicates
      if (!identical(data, cal_events[nrow(cal_events), ])) {
        # if the data is not yet available then add to cal_events
        cal_events <- dplyr::bind_rows(cal_events, data)
      }
    }

    if (i == pages) {
      # stop export after maximum number of pages was loaded
      print("Only part of the available data was exported")
      break
    }
  }

  # unnest people and export every participant for every cal event
  cal_events <- cal_events %>%
    tidyr::unnest(cal_event) %>%
    tidyr::unnest("people",names_sep = "_") %>%
    dplyr::select(id, attachable_id, name, location, description, created_at, updated_at, starts_at, ends_at, people_id)
}



#' get_central_station_protocols
#'
#' this function calls the crm api and downloads the protocols (notices and mails) to all contact.

#' @param api_key the api key you have to provide
#' @param filter_by setting to prefilter export by a list of "company_id" or "person_id", if all protocols are needed use FALSE
#' @param filter_vector vector to filter by "company_id" or "person_id" (decided by filter_by), not needed if all protocols should get downloaded
#' @param pages the setting to set a maximum number of pages to download, with 2000 pages as default (one page includes 250 entries)
#' @return the tibble which contains all information, this can be stored in a single vector or lists

#' @export
get_central_station_protocols <- function (api_key, filter_by = FALSE, filter_vector = NA, pages = 2000) {
  # if no page number is set then a maximum of 2000 pages will be downloaded
  # the export stops, when not more data is available
  # if pages is set to a specific number, then this number of pages will maximum be downloaded
  # define header
  headers <- c(
    "content-type" = "application/json",
    "X-apikey" = api_key,
    "Accept" = "*/*"
  )

  # define filter for request
  filter_option <- paste0("&", filter_by, "=")

  # create an data.frame for protocols
  protocols <- tidyr::tibble()

  if(filter_by == FALSE){
    for (i in 1:pages) {
      # create an response with httr and the GET function. In the function you paste
      # url and your endpoint and you also need the headers
      response <-
        httr::GET(
          # set url with "page=" so you can add endpoints
          paste0(
            "https://api.centralstationcrm.net/api/protocols?perpage=250&page=",
            i,
            "&includes=comments"
          ),
          httr::add_headers(headers)
        )

      # make response answer readable with jsonlite::fromJSON
      data <- jsonlite::fromJSON(httr::content(response, "text"))

      # check if data could be loaded for requested page
      if (length(data) == 0) {
        # stop export if no data could be loaded or everything is already loaded
        break
      } else {
        #get correct number of exported data
        print(paste0("Exported ", ifelse(nrow(data) == 250, i*250, ((i-1)*250+nrow(data))), " protocols"))
        # check if the data already exists in "protocols" to avoid duplicates
        if (!identical(data, protocols[nrow(protocols), ])) {
          # if the data is not yet available then add to protocols
          protocols <- dplyr::bind_rows(protocols, data)
        }
      }

      if (i == pages) {
        # stop export after maximum number of pages was loaded
        print("Only part of the available data was exported")
        break
      }
    }
  } else {
    for (i in 1:length(filter_vector)) {
      # create an response with httr and the GET function. In the function you paste
      # url and your endpoint and you also need the headers
      tryCatch({
        response <-
          httr::GET(
            # set url with "page=" so you can add endpoints and defined filter
            paste0(
              "https://api.centralstationcrm.net/api/protocols?perpage=250&page=1",
              filter_option,
              filter_vector[i],
              "&includes=comments"
            ),
            httr::add_headers(headers)
          )

        data <-
          jsonlite::fromJSON(httr::content(response, "text"))
        protocols <- dplyr::bind_rows(protocols, data)
      },
      error = function(cond) {
        message("Person does not exist")
        # Choose a return value in case of error
        NA
      })
      if (i %% 100 == 0) {
        print(i)
      }
    }
  }

 return(protocols)
}

#' get_central_station_protocols
#'
#' this function calls the crm api and downloads the data of attachments for a
#' given vector of protocol-ids

#' @param api_key the api key you have to provide
#' @param protocols_vector vector with protocols, where we want to export the attachments from
#' @return the tibble which contains all information, this can be stored in a single vector or lists

#' @export
get_central_station_attachments <- function (api_key, protocols_vector) {

  # define header
  headers <- c(
    "content-type" = "application/json",
    "X-apikey" = api_key,
    "Accept" = "*/*"
  )

  # create an data.frame for attachments
  attachments <- tidyr::tibble()

  for (i in 1:length(protocols_vector)) {
    # create an response with httr and the GET function. In the function you paste
    # url and your endpoint and you also need the headers
    tryCatch({
      response <-
        httr::GET(
          # set url with "page=" so you can add endpoints and defined filter
          paste0(
            "https://api.centralstationcrm.net/api/protocols/",
            protocols_vector[i],
            "/attachments?perpage=250&page=1"
          ),
          httr::add_headers(headers)
        )

      data <- jsonlite::fromJSON(httr::content(response, "text")) %>%
        select(-data)
      attachments <- dplyr::bind_rows(attachments, data)
    },
    error = function(cond) {
      message("Attachments do not exist")
      # Choose a return value in case of error
      NA
    })
    if (i %% 10 == 0) {
      print(i)
    }
  }

  return(attachments)
}


#' get_central_station_companies
#'
#' this function calls the crm api and downloads the general informations about all companies.

#' @param api_key the api key you have to provide
#' @param positions True if the company positions (and person_ids) should also get exported
#' @param pages the setting to set a maximum number of pages to download, with 2000 pages as default (one page includes 250 entries)
#' @return the tibble which contains all information, this can be stored in a single vector or lists

#' @export
get_central_station_companies <- function (api_key, positions = TRUE, pages = 2000) {
  # if no page number is set then a maximum of 2000 pages will be downloaded
  # the export stops, when not more data is available
  # if pages is set to a specific number, then this number of pages will maximum be downloaded
  # define header
  headers <-
    c(`content-type` = "application/json",
      `X-apikey` = api_key,
      Accept = "*/*"
  )

  #get number of companies in CRM and limit download to this number (calculate number of pages)
  response <- httr::GET("https://api.centralstationcrm.net/api/companies/count", httr::add_headers(headers))
  data <- jsonlite::fromJSON(httr::content(response, "text"))
  pages <- ceiling(data$total_entries/250)

  companies <- tidyr::tibble()

  if(positions){
    pos <- "%20positions"
  } else {
    pos <- ""
  }

  #request every page of companies from CRM and load them to companies table
  for (i in 1:pages) {
    response <-
      httr::GET(
        paste0(
          "https://api.centralstationcrm.net/api/companies?perpage=250&page=",
          i,
          "&includes=custom_fields",
          pos
        ),
        httr::add_headers(headers)
      )
    data <- jsonlite::fromJSON(httr::content(response, "text"))
    if (!identical(data, companies[nrow(companies),])) {
      companies <- dplyr::bind_rows(companies, data)
    }
  }

  companies_crm <- companies %>%
    tidyr::unnest(company) %>%
    mutate(custom_fields = map(custom_fields, as.data.frame)) %>%
    tidyr::unnest(custom_fields, names_sep = "_", keep_empty = TRUE)

  if(positions){
    companies_crm <- tidyr::unnest(companies_crm, positions, names_sep = "_", keep_empty = TRUE)
  }

  return(companies_crm)

}


# unnest the 'person' column to separate individuals into distinct rows

#crm_export <- call_central_station(api_key = api_key,pages = 2)

###--------------- create tables for the information from list values ---------------------
# I need to get all the emails and phone numbers linked to the contact,
# so i need to unnest them in a separate table


#####------------------------------ set up functions ---------------------------------
#' unnesting_lists
#' this function is used to unnest the lists that are part of retrieved table

#' @param df the df which contains the list wise variable
#' @param listname the lists which should be transformed -- this has to be a list with this name
#' @return the tibble which contains all information, this can be stored in a single vector or lists

#' @export
unnesting_lists <- function(df,listname = listname){
  df %>%
    tidyr::unnest(listname,names_sep = "_") %>%
    dplyr::select(contains(c(listname,"id"))) %>%
    # this selects only the variables that are of interest, possibly we need to keep more variables?
    dplyr::select(c(contains(c("name","department","type","precise_time","badge")),"id"))
}



#
#'create_single_table
#' this function creates the a single crm table with all information as a tibble from one list
#' the table is long and contains a name and value for each id
#' to ensure that names can be distinct, I create an entry_number based on the grouped id and nam.
#' If an id has the same field multiple times, then the entry_number will be >1.
#' @param df the df which contains the list wise variable
#' @param listname the lists which should be transformed -- this has to be a list with this name
#' @return the tibble which contains all information, this is stored in a tibble

#' @export
create_single_table <- function(df,listname = "companies",i = 1){
  # this function gets the tables that were lists into separate tables
  # unnest the list
  this_list_table <- unnesting_lists(df,all_lists[i]) %>%
    # make the list into one long data frame, only the id is kept as another column
    tidyr::pivot_longer(.,cols = -id) %>%
    # drop all na values to avoid empty cells
    tidyr::drop_na(value) %>%
    dplyr::group_by(id,name) %>%
    dplyr::mutate(entry_number = dplyr::row_number())

  # adding the new data to the final data table
  return(this_list_table)
}



#'create_full_table
#'this function joins all retrieved lists together in one long table
#'all_lists <- c("emails","tags","positions","companies","tels","tasks","tasks_pending")
#' @param df the df which contains the list wise variable
#' @param all_lists the lists which should be transformed -- this has to be a list with this name
#' @return the tibble which contains all information, this is stored in a tibble
#' to create the dataset, run the function and assign the result to a named object:
#' result <- create_full_table(crm_data,all_lists)
#' The id of the person is the primary key to link the resulting tables together
#' @export
create_full_table <- function(df, all_lists) {
  # the have a table that will be filled, the final table is initiated here
  final_table <- tidyr::tibble(
    name = as.character(),
    value = as.character(),
    id = as.integer()
  )
  i <- 1
  for (i in 1:length(all_lists)) {
    print(all_lists[i])
    # here i create a single table with the entries of the respective list found in i
    this_list_table <- create_single_table(df, listname = all_lists[i], i)
    # here I add the newly created list to the existing final table
    final_table <- rbind(final_table, this_list_table)
  }
  return(final_table)
}



