# functions to create the confirmation with the confirmation items in one data set

#first I need to read the information from the db and turn it into the long table that holds the information
# own script
#



### functions -------

compress_column <- function(column){
  column <- tolower(column)
  column <- str_remove_all(column," ag| se| gmbh| co. kg|kg")
  column <-  gsub("[^[:alnum:]]", "", column)
  column %>%
    stringr::str_replace_all("[äÄ]", "ae") %>%
    stringr::str_replace_all("[öÖ]", "oe") %>%
    stringr::str_replace_all("[üÜ]", "ue")

}

##mehrere Positionen gehören zu einer Rechnung, getrennt anhand der ID, dh wenn es mehrere laufzeiten gibt, dann sind die anhand der id getrennt und brauchen auch die rehcnugns id

# extract all information from note in rechnung

keep_keys <-
  c("Laufzeit",
    "Kampagnenstart",
    "Kampagnenende",
    "Leistungsdatum",
    "Leistungsbeginn",
    "Leistungsstart",
    "Leistungsende",
    "[Ss]tart",
    "[Ee]nde",
    "Leistungszeitraum",
    "Campaign launch",
    "Running time",
    "Lieferdatum",
    "- Newsletter (1)",
    "- Newsletter (2 &amp; 3)",
    "- Video Ads",
    "- Mailing",
    "- Direct Mailing",
    "- Erste Laufzeit",
    "- Zweite Laufzeit",
    "- Studenten-Kampagne",
    "- Schüler-Kampagne",
    "- Karriereprofil",
    "- Stellenplattform",
    "- Branding Berufsbilder",
    "- Employer Brand.ng",
    "Employer Branding",

    "Kampagne",
    "(1) 13.04.2022 - 13.06.2022",
    "(2) 13.09.2022 - 13.07.2023",
    "Branding Berufsbilder",
    "Leistungsdaten",
    "Stellenplattform",
    "Employer Branding \\/ Branding Berufsbilder",
    "Date of performance",
    "- EB und BB",
    "- Stellenanzeigen",
    "Newsletter",
    "Stellenanzeigen",
    "Direct Mailing"

  )%>%
  paste0(.,collapse = "|")


## create a readable concise note
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

## extract the information from each note and put it in a long table
get_extracted_information <- function(df,field) {
  purrr::map_dfr(1:nrow(df), ~ extract_note(df[.,],field = field),.progress = TRUE) %>%
    unnest(cols = note)
}

## create the fields for each position
# Kommentar, Laufzeit_Start, Laufzeit_Ende, Zielgruppe,

## create the final dataframe from the provided data -- important, names must match

read_KeysFromDescription <- function(df,keep_keys) {

  df %>%
    separate(note, into = c("key", "text"), sep = ":") %>%
    drop_na(text) %>%
    mutate(
      # key = case_when(
      #   str_detect(text, "-") &
      #     str_detect(text, pattern = "[:alpha:]", negate = TRUE) ~ "Leistungszeitraum",
      #   key == "Leistungszeitraum" &
      #     str_detect(text, "-", negate = TRUE) ~ "Leistungsende",
      #   TRUE ~ key
      # ),
      text = str_trim(text),
      text = na_if(text, ""),
      key = str_replace_all(
        string = key,
        pattern = c(".*Leistungszeitraum" = "Leistungszeitraum",
                    ".* Laufzeit" = "Leistungszeitraum")))

}

create_laufzeiten_confirmations <- function(df) {

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

# bonus keys


create_bonus_confirmations <- function(df) {
#df <- extracted_information
df <- df %>%
  mutate(note = if_else(
    str_detect(note,
               pattern = c(
                 ".B[b]onus.*|usätzlich|inklusive|inkl.*"
               )) &
      str_detect(note, "%")
    &!str_detect(note, "jetzt"),
    paste0("Bonus:", note),
    note
  ))


keep_keys<- c("Bonus")

create_num_bonus <- function(value){
  #value <- "50% und 50%"
  list_bonus <- str_extract_all(value, pattern = "..(=?%)")
  list_bonus %>%
    unlist() %>%
    str_remove_all(.,"%") %>%
    as.numeric() %>%
    sum()
  }


df %>%
  read_KeysFromDescription(df = .) %>%
  left_join(confirmation_items %>%
              select(id,title,confirmation_id,article_id), by = "id") %>%
  left_join(confirmations %>%
              select(id,client_id,date,confirmation_number), by = c("confirmation_id"="id")) %>%
  group_by(key,id) %>%
  mutate(Number = n()) %>%
  filter(str_detect(key, pattern = keep_keys)) %>%
  mutate(Bonus = create_num_bonus(value = text),
         Bonus = na_if(Bonus,0))
}

# für das Produkt Stellen müssen die Kontingente aus der description der confirmation-items gelesen werden
create_articles_confirmations <- function(df) {
  keep_keys <- c("Produktbeschreibung",
                 "Laufzeit",
                 "Direct Mailings",
                 "Social Media Posting",
                 "Kontingent")%>%
    paste0(.,collapse = "|")
  df <- articles_extracted
  # first I need to extract all keys and check if they are correct
  df %>%
    read_KeysFromDescription(df = .) %>%
    filter(str_detect(key, pattern = keep_keys)) %>%
    mutate(key = str_remove_all(key,"- "),
           numbers = word(text,1)
           ) %>%
    #pivot_wider(names_from = key, values_from = text) %>%

    #left_join(articles, by = "id") %>%
    #distinct(key,.keep_all = TRUE) %>%
    View()
  ## I want the information about the impressions, letters, postings
  ## this needs to be matched with the line of the article to get the name
}


