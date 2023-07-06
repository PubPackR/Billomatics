#' compress column
#'
#' this function shortens names.

#' @param column the path to the billomat db
#' @return the string

#' @export
compress_column <- function(column){
  column <- tolower(column)
  column <- str_remove_all(column," ag| se| gmbh| co. kg|kg")
  column <-  gsub("[^[:alnum:]]", "", column)
  column %>%
    stringr::str_replace_all("[äÄ]", "ae") %>%
    stringr::str_replace_all("[öÖ]", "oe") %>%
    stringr::str_replace_all("[üÜ]", "ue")

}





#' create a readable concise note
#'
#' This function creates a readable note

#' @param df the df which holds the data from which information is extracted
#' @param field the name of the field that contains the data
#' @return the data as df which has id and all information

#' @export
extract_note<- function(df,field) {
  df <- df %>%
    mutate(note = str_replace_all(get(field),
                                  pattern = c(`: [\n]` = ":",
                                              `in [\n]` = "",
                                              `–`= "-",
                                              ` – `= "-",
                                              `Produktbeschreibung` = "Produktbeschreibung")),
           note = str_remove(note,": \n")
    )
  df <- tibble(
    id = df$id,
    note = str_split(df$note, "\n"))
  df
}


#' ## extract the information from each note and put it in a long table

#' @param df the df which holds the data from which information is extracted
#' @param field the name of the field that contains the data
#' @return the data as df which has id and all information

#' @export
get_extracted_information <- function(df,field) {
  purrr::map_dfr(1:nrow(df), ~ extract_note(df[.,],field = field),.progress = TRUE) %>%
    unnest(cols = note)
}

## create the fields for each position
# Kommentar, Laufzeit_Start, Laufzeit_Ende, Zielgruppe,

#' extract the information from each note and put it in a long table
#' create the final dataframe from the provided data -- important, names must match
#' the data provided is split with a separate symbol

#' @param df the df which holds the data from which information is extracted
#' @param sep the separator between key and text
#' @return the data as df which has id and all information

#' @export
read_KeysFromDescription <- function(df, sep = sep,EntryFormatCheck = EntryFormatCheck) {
  #df <- extracted_information
  #EntryFormatCheck = FALSE
  df <- df %>%
    separate(note, into = c("key", "text"), sep = sep) %>%
    drop_na(text) %>%
    mutate(
      text = str_trim(text),
      text = na_if(text, ""))

  # if the process is not the format check, then I have to correct all wrong entries
  if (!EntryFormatCheck) {

    replace_string <- c(
      "–" = "-",
      "02023" ="2023",
      "Ende Mai" = "25.5.2022-24.6.2022",
      "März - Juli 2022" = "1.3.2022-31.7.2022",
      "Oktober 2021" = "1.10.2021-30.9.2022",
      "01.06 - 30.11.22" = "01.06.22 - 30.11.22",
      "14.04.2022" = "14.04.2022-13.10.2022",
      "10.05.2022\\. \\+ 17.05.2022" = "10.05.2022-17.05.2022",
      "bis 30.09.2022" = "1.9.2022-30.9.2022",
      "April 14th, 2022 - October 13th, 2022" = "14.4.2022-13.10.2022",
      "23.08.2022, 06.09.2022, 13.09.2022, 27.09.2022, 11.10.2022" = "23.08.2022-11.10.2022",
      "13.07.2021" = "13.07.2021-12.09.2021",
      #"14.09.2022" = "13.10.2022",
      "April 14th, 2022" = "14.4.2022",
      "6 months" = "6Monate",
      "01.11.2022 - 31.04.2023" = "01.11.2022-30.04.2023",
      "01.09.2022" = "01.09.2022-31.8.2023",
      "Anfang Oktober" = "1.10.2022",
      "27.01, 10.02 , 02.02, 16.02" = "27.01.2023-16.02.2023"
    )

    to_Leistungsbeginn <- paste0(
      c(
        "Versand.*",
        "Leistungsdatum",
        "Kampagnenstart",
        "Campaign launch",
        "Lieferdatum",
        "Beginn des Leistungszeitraum",
        ".osting.*",
        "Laufzeitbeginn",
        "Leistungsbeginn.*",
        "Leistungsstart"
      ),
      collapse = "|"
    )

    to_Leistungsende <- paste0(
      c(
        "Kampagnenende",
        "Laufzeitende",
        "Ende des Leistungszeitraums"),
      collapse = "|"
    )

    to_Leistungszeitraum <- paste0(
      c(
        "Laufzeit",
        "Leistungszeit",
        "Leistungsbeginn"
        ),
      collapse = "|"
    )


    df <- df %>%
      mutate(key = if_else(str_detect(key, to_Leistungsbeginn) & !str_detect(text, "-"),
                       "Leistungsbeginn",
                       if_else(str_detect(key, to_Leistungsende),
                               "Leistungsende",
                               if_else(str_detect(key, to_Leistungszeitraum)& str_detect(text, "-|bis|vor|im|nfang"),
                                       "Leistungszeitraum",
                                       if_else(str_detect(key,"Laufzeit") & !str_detect(text, "-|bis|vor|im|nfang"),
                                               "Laufzeit",
                                               key)
                                       )
                               )
                       ),
             text = str_replace_all(text, replace_string),
             text = str_remove_all(text, pattern = c("\\(|\\)|nach erneuter Absprache|&nbsp|-28.02.2023und01.03.2023|-30.06.2023und01.08.2023|- 02.07.2022\\/\\/01.09.2022|-24.06.2022\\(2\\)01.11.2022|-02.07.2022//01.09.2022|nachAbsprache|geplant|EmployerBranding|Stellenanzeigen|1\\.Kampagne|28.02.2023und01.03.2023-|-01.05.2023,01.11.2022|14.11.2023,01.11.2022-"))

      )
  }
}

#' extract the information from each note and put it in a long table
#' create the final dataframe from the provided data -- important, names must match
#' the data provided is split with a separate symbol

#' @param df the df which holds the data from which information is extracted
#' @param confirmation_items the confirmation items
#' @param confirmations the confirmations
#' @return the data as df which has id and all information

#' @export
create_laufzeiten_confirmations <- function(df,
                                            confirmation_items,
                                            confirmations,
                                            sep = ":",
                                            EntryFormatCheck = FALSE) {

  # keys to keep when filtering

  keep_keys <-
    c("Laufzeit",
      "Leistungszeitraum",
      "Leistungsbeginn",
      "Leistungsende"
    )%>%
    paste0(.,collapse = "|")

  # removing the non laufzeiten rows and coding start and end time

  df %>%
    read_KeysFromDescription(df = ., sep = sep, EntryFormatCheck = EntryFormatCheck) %>%
    left_join(confirmation_items %>%
                select(id,title,confirmation_id,article_id), by = "id") %>%
    left_join(confirmations %>%
                select(id,client_id,date,confirmation_number), by = c("confirmation_id"="id")) %>%
    group_by(key,id) %>%
    mutate(Number = row_number()) %>%
    mutate(Produkt = if_else(
      !str_detect(title, "Bewerber-Boost"),
      title,
      if_else(
        !str_detect(key, "Versand") &
          !str_detect(key, "Social"),
        paste0(title, "_Video_Ads"),
        if_else(
          str_detect(key, "Versand"),
          paste0(title, "_Direct_Mailing"),
          paste0(title, "_Social_Media_Post")
        )
      )
    )
    ) %>%
    filter(str_detect(key, pattern = keep_keys)&!str_detect(key, pattern = "Das Karriereprofil|Stellen")) %>%
    pivot_wider(names_from = key, values_from = text) %>%
    separate(Leistungszeitraum,into = c("Start","Ende"), sep = "-") %>%
    mutate(
      Leistungsbeginn = as.Date(coalesce(Leistungsbeginn,Start), format = "%d.%m.%Y"
      ),
      Leistungsende = as.Date(coalesce(Leistungsende,Ende),
                              format = "%d.%m.%Y"
      )
    )
}

#' extract the information from each note and put it in a long table
#' create the final dataframe from the provided data -- important, names must match
#' the data provided is split with a separate symbol

#' @param df the df which holds the data from which information is extracted
#' @param keep_keys the keys from where laufzeit information is extracted
#' @param is_invoice  logical, setting whether the df contains information on invoices or not
#' @return the data as df with the id and the extracted laufzeit

#' @export
get_laufzeiten_information <- function(df, keep_keys, is_invoice = TRUE) {
  # only keep leistungszeitraum keys

  if(is_invoice){
    df <- df %>%
      filter((grepl(x = key, pattern = keep_keys))) %>%
      # turn all keys to one name
      mutate(key = "Leistungszeitraum")
  } else {
    df <- df %>%
      filter((grepl(x = key, pattern = keep_keys)))
  }
  df <- df %>%
    # cleaning text
    mutate(
      text = str_trim(text),
      text = str_remove_all(text, pattern = c(" \\(nach erneuter Absprache\\)|&nbsp|-28.02.2023und01.03.2023|-30.06.2023und01.08.2023|- 02.07.2022\\/\\/01.09.2022|-24.06.2022\\(2\\)01.11.2022|-02.07.2022//01.09.2022|nachAbsprache|geplant|EmployerBranding|Stellenanzeigen|1\\.Kampagne|28.02.2023und01.03.2023-|-01.05.2023\\(\\),01.11.2022|14.11.2023\\(\\),01.11.2022-|\\(\\)|;")),
      text = na_if(text, "")) %>%
    drop_na(text) %>%
    group_by(id,key) %>%
    mutate(Laufzeitnummer = row_number()) %>%
    ungroup() %>%
    pivot_wider(
      id_cols = c("id","Laufzeitnummer"),
      names_from = "key",
      values_from = "text"
    ) %>%
    # take out breaks in laufzeit
    mutate(
      Leistungszeitraum = str_remove_all(Leistungszeitraum, pattern = "(?<=-).+(?=-)"),
      Leistungszeitraum = str_replace_all(Leistungszeitraum, pattern = c("--" = "-")),
      Leistungszeitraum = str_remove_all(Leistungszeitraum,pattern = c("\\(1\\)|\\(2. Kampagne\\)|dauerhaft ab dem"))
    )

  if (is_invoice){
    df <- df %>%
      separate(Leistungszeitraum,
               into = c("Start", "Ende"),
               sep = "-")
    #mutate(Ende = case_when(invoice_number %in% direct_mailing_invoice_number ~ Start,
    #                        TRUE ~ Ende)) %>%
  } else {
    df <- df %>%
      mutate(Start = Leistungsbeginn,
             Ende = Leistungsende)
  }
  df %>%
    mutate(Start_date = dmy(Start),
           Ende_date = dmy(Ende)) %>%
    group_by(id) %>%
    summarise(Laufzeit_Start = ymd(min(Start_date,na.rm = TRUE)),
              Laufzeit_Ende = ymd(max(Ende_date,na.rm = TRUE)))
}

