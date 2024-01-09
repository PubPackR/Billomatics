#' compress column
#'
#' this function shortens names.

#' @param column the path to the billomat db
#' @return the string

#' @export
compress_column <- function(column) {
  column <- tolower(column)
  column <- str_remove_all(column, " ag| se| gmbh| co. kg|kg")
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
extract_note <- function(df, field) {
  df <- df %>%
    mutate(
      note = str_replace_all(
        get(field),
        pattern = c(
          `: [\n]` = ":",
          `in [\n]` = "",
          `–` = "-",
          ` – ` = "-",
          `Produktbeschreibung` = "Produktbeschreibung"
        )
      ),

      note = str_remove_all(note, "geplant.*?\\s"),
      note = str_remove(note, ": \n"),

      # to avoid the problem of having multiple : in one line the \n is added after : when it occurs more than1
      note = str_replace_all(note, ":(?=.*?:)", ":\n ")
    )
  df <- tibble(id = df$id,
               note = str_split(df$note, "\n"))
  df
}


#' ## extract the information from each note and put it in a long table

#' @param df the df which holds the data from which information is extracted
#' @param field the name of the field that contains the data
#' @return the data as df which has id and all information

#' @export
get_extracted_information <- function(df, field) {
  purrr::map_dfr(1:nrow(df), ~ extract_note(df[., ], field = field), .progress = TRUE) %>%
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
read_KeysFromDescription <- function (df, sep = sep)
{replace_string <- c(`–` = "-", `bis zum` = "-", bis = "-",
                     `ab dem` = "", `02023` = "2023", `März - Juli 2022` = "1.3.2022-31.7.2022",
                     `01.06 - 30.11.22` = "01.06.22 - 30.11.22", `10.05.2022\\. \\+ 17.05.2022` = "10.05.2022-17.05.2022",
                     `bis 30.09.2022` = "1.9.2022-30.9.2022", `April 14th, 2022 - October 13th, 2022` = "14.4.2022-13.10.2022",
                     `23.08.2022, 06.09.2022, 13.09.2022, 27.09.2022, 11.10.2022` = "23.08.2022-11.10.2022",
                     `13.07.2021` = "13.07.2021-12.09.2021", `April 14th, 2022` = "14.4.2022",
                     `6 months` = "6Monate", `01.11.2022 - 31.04.2023` = "01.11.2022-30.04.2023",
                     `27.01, 10.02 , 02.02, 16.02` = "27.01.2023-16.02.2023")
df <- df %>% separate(note, into = c("key", "text"), sep = sep) %>%
  drop_na(text) %>% mutate(text = str_trim(text), text = na_if(text,
                                                               ""))
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
to_Leistungsende <- paste0(c("Kampagnenende", "Laufzeitende",
                             "Ende des Leistungszeitraums"),
                           collapse = "|")
to_Leistungszeitraum <- paste0(c("Laufzeit", "Leistungszeit",
                                 "Leistungsbeginn", "Versand.*"),
                               collapse = "|")
df <-
  df %>% mutate(
    key = if_else(
      str_detect(key, to_Leistungsbeginn) &
        !str_detect(text, "-"),
      "Leistungsbeginn",
      if_else(
        str_detect(key,
                   to_Leistungsende),
        "Leistungsende",
        if_else(
          str_detect(key,
                     to_Leistungszeitraum) &
            str_detect(text, "-|bis|vor|im|nfang") &
            !str_detect(text, "Wochen|Monat|asap|flexibel|einmalig"),
          "Leistungszeitraum",
          if_else(
            str_detect(key, "Laufzeit") & !str_detect(text,
                                                      "-|bis|vor|im|nfang"),
            "Laufzeit",
            key
          )
        )
      )
    ),

    text = str_remove_all(
      text,
      pattern = c(
        "\\(|\\)|nach erneuter Absprache|einmalig bis zum|&nbsp|-28.02.2023und01.03.2023|;|-30.06.2023und01.08.2023|- 02.07.2022\\/\\/01.09.2022|-24.06.2022\\(2\\)01.11.2022|-02.07.2022//01.09.2022|nachAbsprache|geplant|EmployerBranding|Stellenanzeigen|1\\.Kampagne|28.02.2023und01.03.2023-|-01.05.2023,01.11.2022|14.11.2023,01.11.2022-"
      )
    ),
    text = str_replace_all(text,
                           replace_string),
    text = str_squish(text),
    text = str_remove_all(text, "(?<![:alpha:]) (?![:alpha:])")
  )

}


#' extract the information from each note and put it in a long table
#' create the final dataframe from the provided data -- important, names must match
#' the data provided is split with a separate symbol

#' @param df the df which holds the data from which information is extracted
#' @return the data as df with the id and the extracted laufzeit

#' @export
get_laufzeiten_information <- function (df)
{
  keep_keys <-
    c("Laufzeit",
      "Leistungsdatum",
      "Leistungszeitraum",
      "Leistungsbeginn",
      "Leistungsende",
      "Date of performance",
      "Leistungszeitraum"
    ) %>%
    paste(.,collapse = "|")

  df <- df %>% filter((grepl(x = key, pattern = keep_keys))) %>%
    mutate(
      key = case_when(
        str_detect(text, c("-|bis")) &
          !str_detect(text, c("Wochen|Monate"))~
          "Leistungszeitraum",
        str_detect(key, "Leistungszeitraum") &
          !str_detect(text, "-|bis") ~ "Leistungsende",
        TRUE ~ key
      )
    )


  df <-
    df %>% mutate(
      text = str_trim(text),
      text = str_remove_all(
        text,
        pattern = c(
          " \\(nach erneuter Absprache\\)|&nbsp|-28.02.2023und01.03.2023|-30.06.2023und01.08.2023|- 02.07.2022\\/\\/01.09.2022|-24.06.2022\\(2\\)01.11.2022|-02.07.2022//01.09.2022|nachAbsprache|geplant|EmployerBranding|Stellenanzeigen|1\\.Kampagne|28.02.2023und01.03.2023-|-01.05.2023\\(\\),01.11.2022|14.11.2023\\(\\),01.11.2022-|\\(\\)|;"
        )
      ),
      text = na_if(text, "")
    ) %>% drop_na(text) %>%
    group_by(id,
             key) %>%
    mutate(Laufzeitnummer = row_number()) %>% ungroup() %>%
    pivot_wider(
      id_cols = c("id", "Laufzeitnummer"),
      names_from = "key",
      values_from = "text"
    )

  # in order to avoid that the code breaks if there is no "Leistungzeitraum I need to break the pipe

  condition1 <- "Leistungszeitraum" %in% colnames(df) &
    !("Leistungsbeginn" %in% colnames(df) | "Leistungsende" %in% colnames(df))


  condition2 <- "Leistungszeitraum" %in% colnames(df) &
    ("Leistungsbeginn" %in% colnames(df) |
    "Leistungsende" %in% colnames(df))


  if (condition1) {
    print ("1")
    df <- df %>% mutate(
      Leistungszeitraum = str_remove_all(Leistungszeitraum,
                                         pattern = "(?<=-).+(?=-)"),
      Leistungszeitraum = str_replace_all(Leistungszeitraum,
                                          pattern = c(`--` = "-")),
      Leistungszeitraum = str_remove_all(
        Leistungszeitraum,
        pattern = c("\\(1\\)|\\(2. Kampagne\\)|dauerhaft ab dem")
      )
    ) %>%
      # to break up the leistungszeitraum into start and end
      separate(Leistungszeitraum,
               into = c("Start",
                        "Ende"),
               sep = "-")
  } else if (condition2) {
    print ("2")
    # to coalesce the cases where both leistungszeitraum and start and end were given, we need another filter

    df <- df %>%
      # to break up the leistungszeitraum into start and end
      separate(Leistungszeitraum,
               into = c("Start",
                        "Ende"),
               sep = "-") %>%
      mutate(
        # to only keep the dates and remove all text information
        Start = stringr::str_extract_all(Start,"[0-9]*\\.[0-9]*\\.[0-9]*"),
        Ende = stringr::str_extract_all(Ende,"[0-9]*\\.[0-9]*\\.[0-9]*"),
        Leistungsbeginn = stringr::str_extract_all(Leistungsbeginn,"[0-9]*\\.[0-9]*\\.[0-9]*"),
        Leistungsende = stringr::str_extract_all(Leistungsende,"[0-9]*\\.[0-9]*\\.[0-9]*"),

        Start = ifelse(is.na(Start),Leistungsbeginn,Start),
        Ende = ifelse(is.na(Ende), Leistungsende,Ende))
  } else {
    print ("3")
    # when there is no start and ende then I have to make this column
    df <- df %>% mutate(

      # to only keep the dates and remove all text information
      Leistungsbeginn = stringr::str_extract_all(Leistungsbeginn,"[0-9]*\\.[0-9]*\\.[0-9]*"),
      Leistungsende = stringr::str_extract_all(Leistungsende,"[0-9]*\\.[0-9]*\\.[0-9]*"),

      Start = Leistungsbeginn,
      Ende = Leistungsende)
  } # to coalesce the cases where both leistungszeitraum and start and end were given, we need another filter



  df <- df %>%
    group_by(id) %>%
    summarise(Laufzeit_Start = min(dmy(Start),na.rm = TRUE),
              Laufzeit_Ende = max(dmy(Ende), na.rm = TRUE))

  return(df)
}

#' get the number of impressions ordered
#' this function allows to get the impressions from the orders
#' @param df the df which holds the data that was extracted from the field
#' @param field2extract the field we want to get
#' @return the data as df with the id and the extracted Impressions and Bonus

#' @export
get_impressions_and_bonus <- function(df,field2extract = "description") {
  df <- df %>%
  mutate(description = str_replace_all(
    description,
    pattern = c("Produktbeschreibung:" = "Produktbeschreibung:\n",
                "[Pp]lus" = "+")
  ))

  field <- field2extract
  extracted_information <-
    Billomatics::get_extracted_information(df,
                                           field = field)
  extracted_information |>
    mutate(
      video = str_detect(note, "Video Ads") &
        !str_detect(
          note,
          "Aktion|Angebots|Besonderheiten|Leistungsbeginn:|Ende"
        )
    ) |>
    filter(video) |>
    mutate(
      kontingent = str_extract_all(note, ".*(=?Video Ads)"),
      kontingent1 = str_remove_all(kontingent, pattern = "[:Alpha:]|\\.")
    ) |>
    separate(
      kontingent1,
      into = c("Impressionen", "Bonus1", "Bonus2", "Bonus3", "Bonus4"),
      sep = "\\+"
    ) |>
    mutate(
      Impressionen = as.numeric(str_remove_all(Impressionen, "[:punct:]")),
      Bonus1 = as.numeric(str_remove_all(Bonus1, "[:punct:]")),
      Bonus2 = as.numeric(str_remove_all(Bonus2, "[:punct:]")),
      Bonus3 = as.numeric(str_remove_all(Bonus3, "[:punct:]")),
      Bonus4 = as.numeric(str_remove_all(Bonus4, "[:punct:]")),
      Bonus = rowSums(across(contains("Bonus")), na.rm = TRUE)
    ) |>
    group_by(id) |>
    summarise(
      Impressionen = sum(Impressionen, na.rm = TRUE),
      Bonus = sum(Bonus, na.rm = TRUE)
    ) |>
    # mutate(rel_Bonus = Bonus/Impressionen,
    #        cut_Bonus = cut(rel_Bonus,breaks = c(-Inf,0,.1,.2,.3,Inf)),
    #        cut_Impressions = cut(Impressionen, breaks = c(-Inf,4500,9000,20000,50000,70000,140000,Inf))) |>
    filter(Impressionen > 0)
}
