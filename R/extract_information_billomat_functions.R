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
read_KeysFromDescription <- function(df, sep = ":") {

  df %>%
    separate(note, into = c("key", "text"), sep = sep) %>%
    drop_na(text) %>%
    mutate(
      text = str_trim(text),
      text = na_if(text, ""),
      key = str_replace_all(
        string = key,
        pattern = c(".*Leistungszeitraum" = "Leistungszeitraum",
                    ".* Laufzeit" = "Leistungszeitraum")))

}

#' extract the information from each note and put it in a long table
#' create the final dataframe from the provided data -- important, names must match
#' the data provided is split with a separate symbol

#' @param df the df which holds the data from which information is extracted
#' @param confirmation_items the confirmation items
#' @param confirmations the confirmations
#' @return the data as df which has id and all information

#' @export
create_laufzeiten_confirmations <- function(df,confirmation_items,confirmations) {

  # keys to keep when filtering

  keep_keys <-
    c("Laufzeit",
      "Kampagnenstart",
      "Kampagnenende",
      "Leistungsdatum",
      "Leistungsbeginn",
      "Leistungsende",
      "Campaign launch",
      "Running time",
      "Lieferdatum",
      "osting am",
      "Versanddatum"
    )%>%
    paste0(.,collapse = "|")

  # removing the non laufzeiten rows and coding start and end time

  df %>%
    read_KeysFromDescription(df = .) %>%
    left_join(confirmation_items %>%
                select(id,title,confirmation_id,article_id), by = "id") %>%
    left_join(confirmations %>%
                select(id,client_id,date,confirmation_number), by = c("confirmation_id"="id")) %>%
    group_by(key,id) %>%
    mutate(Number = n()) %>%
    filter(str_detect(key, pattern = keep_keys)) %>%
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
    ),
    key = if_else(str_detect(key, "Versand"),"Versanddatum",
                  if_else(str_detect(key, "Laufzeit"),"Laufzeit",
                          key)
    )
    ) %>%
    pivot_wider(names_from = key, values_from = text) %>%
    mutate(
      Leistungsbeginn = as.Date(
        coalesce(Leistungsbeginn, Versanddatum, Leistungsdatum),
        format = "%d.%m.%Y"
      ),
      Leistungsende = as.Date(coalesce(Leistungsende),
                              format = "%d.%m.%Y"
      )
    )
}
