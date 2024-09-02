#' compress column
#'
#' this function shortens names.

#' @param column the path to the billomat db
#' @return the string

#' @export
compress_column <- function(column) {
  column <- gsub("\u00A0", " ", column, fixed = TRUE)
  column <- tolower(column)
  column <- str_remove_all(column, " ag| se| gmbh| co\\. kg|kg")
  column <-  gsub("[^[:alnum:]]", "", column)
  column %>%
    stringr::str_replace_all("[äÄ]", "ae") %>%
    stringr::str_replace_all("[öÖ]", "oe") %>%
    stringr::str_replace_all("[üÜ]", "ue")

}




#----------------------------------------------------------------------------------------------------------------#
##------------------------------------ Functions when getting data from Billomat ---------------------------------
#----------------------------------------------------------------------------------------------------------------#


#' create a readable concise note
#'
#' This function creates a readable note

#' @param df the df which holds the data from which information is extracted
#' @param field the name of the field that contains the data
#' @return the data as df which has id and all information

#' @export
extract_note <- function(df, field) {
  pattern_break_1 <- "\\b(Leistu\\w*)\\b"
  pattern_break_2 <- "\\b(Laufzeit\\w*)\\b"

  df <- df %>%
    mutate(
      note = str_replace_all(
        get(field),
        pattern = c(
          `:[\n]` = ":",
          `: [\n]` = ":",
          `in [\n]` = "",
          `–` = "-",
          ` – ` = "-",
          `Produktbeschreibung` = "Produktbeschreibung"
        )
      ),

      note = str_remove_all(note, "[Gg]eplant.*?\\s"),
      note = str_remove(note, ": \n"),
      note = str_trim(note),

      # to avoid the problem of having multiple : in one line the \n is added after : when it occurs more than1
      # i want the break in Laufzeit Leistungsbeginn: 15.01.2024 Leistungsende: 15.05.2024\n2
      note = str_replace_all(note, pattern_break_1, "\n\\1"),
      note = str_replace_all(note, pattern_break_2, "\n\\1"),
    )
  df <- tibble(id = df$id, note = str_split(df$note, "\n"))
  df
}


#' ## extract the information from each note and put it in a long table

#' @param df the df which holds the data from which information is extracted
#' @param field the name of the field that contains the data
#' @return the data as df which has id and all information

#' @export
get_extracted_information <- function(df, field) {
  if (field == "description") {
    df <- df %>%
      mutate(description = str_replace_all(
        description,
        pattern = c(
          `Beginn des Leistungszeitraums` = "Laufzeitbeginn",
          `Ende des Leistungszeitraums` = "Laufzeitende"
        )
      ))
  }
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
{
  replace_string <- c(
    `–` = "-",
    `bis zum` = "-",
    bis = "-",
    `ab dem` = "",
    `02023` = "2023",
    `März - Juli 2022` = "1.3.2022-31.7.2022",
    `01.06 - 30.11.22` = "01.06.22 - 30.11.22",
    `10.05.2022\\. \\+ 17.05.2022` = "10.05.2022-17.05.2022",
    `bis 30.09.2022` = "1.9.2022-30.9.2022",
    `April 14th, 2022 - October 13th, 2022` = "14.4.2022-13.10.2022",
    `23.08.2022, 06.09.2022, 13.09.2022, 27.09.2022, 11.10.2022` = "23.08.2022-11.10.2022",
    `13.07.2021` = "13.07.2021-12.09.2021",
    `April 14th, 2022` = "14.4.2022",
    `6 months` = "6Monate",
    `01.11.2022 - 31.04.2023` = "01.11.2022-30.04.2023",
    `27.01, 10.02 , 02.02, 16.02` = "27.01.2023-16.02.2023",
    `31.09.` = "30.09.",
    `30.2.` = "28.02.",
    `31.04.` = "30.04.",
    `31.06.` = "30.06.",
    `31.11.` = "30.11."
  )
  df <- df %>% separate(note, into = c("key", "text"), sep = sep) %>%
    drop_na(text) %>% mutate(text = str_trim(text), text = na_if(text, ""))
  to_Leistungsbeginn <- paste0(
    c(
      "Versand.*",
      "Leistungsdatum",
      "Kampagnenstart",
      "Campaign launch",
      "Predicted start of performance",
      "Lieferdatum",
      "Beginn des Leistungszeitraum",
      ".osting.*",
      "Laufzeitbeginn",
      "Leistungsbeginn.*",
      "Leistungsstart",
      "Startdatum"
    ),
    collapse = "|"
  )
  to_Leistungsende <- paste0(c(
    "Kampagnenende",
    "Laufzeitende",
    "Ende des Leistungszeitraums",
    "Predicted end of performance"
  ),
  collapse = "|")
  to_Leistungszeitraum <- paste0(c("Laufzeit", "Leistungszeit", "Leistungsbeginn", "Versand.*"),
                                 collapse = "|")
  df <-
    df %>% mutate(
      key = if_else(
        str_detect(key, to_Leistungsbeginn) &
          !str_detect(text, "-"),
        "Leistungsbeginn",
        if_else(
          str_detect(key, to_Leistungsende),
          "Leistungsende",
          if_else(
            str_detect(key, to_Leistungszeitraum) &
              str_detect(text, "-|bis|vor|im|nfang") &
              !str_detect(text, "Wochen|Monat|asap|flexibel|einmalig"),
            "Leistungszeitraum",
            if_else(
              str_detect(key, "Laufzeit") & !str_detect(text, "-|bis|vor|im|nfang"),
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
      text = str_replace_all(text, replace_string),
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
    c(
      "Laufzeit",
      "Leistungsdatum",
      "Leistungszeitraum",
      "Leistungsbeginn",
      "Leistungsende",
      "Date of performance",
      "Kampagnenstart"
    ) %>%
    paste(., collapse = "|")

  df <- df %>% filter((grepl(x = key, pattern = keep_keys))) %>%
    mutate(
      key = case_when(
        str_detect(text, c("-|bis")) &
          !str_detect(text, c("Wochen|Monate")) ~
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
    group_by(id, key) %>%
    mutate(Laufzeitnummer = row_number()) %>% ungroup() %>%
    pivot_wider(
      id_cols = c("id", "Laufzeitnummer"),
      names_from = "key",
      values_from = "text"
    )

  # in order to avoid that the code breaks if there is no "Leistungzeitraum I need to break the pipe

  condition1 <- "Leistungszeitraum" %in% colnames(df) &
    !("Leistungsbeginn" %in% colnames(df) |
        "Leistungsende" %in% colnames(df))


  condition2 <- "Leistungszeitraum" %in% colnames(df) &
    ("Leistungsbeginn" %in% colnames(df) |
       "Leistungsende" %in% colnames(df))


  if (condition1) {
    print ("1")
    df <- df %>% mutate(
      Leistungszeitraum = str_remove_all(Leistungszeitraum, pattern = "(?<=-).+(?=-)"),
      Leistungszeitraum = str_replace_all(Leistungszeitraum, pattern = c(`--` = "-")),
      Leistungszeitraum = str_remove_all(
        Leistungszeitraum,
        pattern = c("\\(1\\)|\\(2. Kampagne\\)|dauerhaft ab dem")
      )
    ) %>%
      # to break up the leistungszeitraum into start and end
      separate(Leistungszeitraum,
               into = c("Start", "Ende"),
               sep = "-")
  } else if (condition2) {
    print ("2")
    # to coalesce the cases where both leistungszeitraum and start and end were given, we need another filter

    df <- df %>%
      # to break up the leistungszeitraum into start and end
      separate(Leistungszeitraum,
               into = c("Start", "Ende"),
               sep = "-") %>%
      mutate(
        # to only keep the dates and remove all text information
        Start = stringr::str_extract_all(Start, "[0-9]*\\.[0-9]*\\.[0-9]*"),
        Ende = stringr::str_extract_all(Ende, "[0-9]*\\.[0-9]*\\.[0-9]*"),
        Leistungsbeginn = stringr::str_extract_all(Leistungsbeginn, "[0-9]*\\.[0-9]*\\.[0-9]*"),
        Leistungsende = stringr::str_extract_all(Leistungsende, "[0-9]*\\.[0-9]*\\.[0-9]*"),

        Start = ifelse(is.na(Start), Leistungsbeginn, Start),
        Ende = ifelse(is.na(Ende), Leistungsende, Ende)
      )
  } else {
    print ("3")
    # add column if not existing
    if (!"Leistungsbeginn" %in% colnames(df)) {
      df <- df %>%
        mutate(Leistungsbeginn = NA)
    }
    if (!"Leistungsende" %in% colnames(df)) {
      df <- df %>%
        mutate(Leistungsende = NA)
    }
    # when there is no start and ende then I have to make this column
    df <- df %>% mutate(
      # to only keep the dates and remove all text information
      Leistungsbeginn = stringr::str_extract_all(Leistungsbeginn, "[0-9]*\\.[0-9]*\\.[0-9]*"),
      Leistungsende = stringr::str_extract_all(Leistungsende, "[0-9]*\\.[0-9]*\\.[0-9]*"),

      Start = Leistungsbeginn,
      Ende = Leistungsende
    )
  } # to coalesce the cases where both leistungszeitraum and start and end were given, we need another filter



  df <- df %>%
    group_by(id) %>%
    summarise(
      Laufzeit_Start = min(dmy(Start), na.rm = TRUE),
      Laufzeit_Ende = max(dmy(Ende), na.rm = TRUE)
    )

  return(df)
}

#' extract the laufzeit duration information and put it into a table with id, entrynumber and laufzeit
#' @param df_position the df which holds the data from which information is extracted
#' @param field the field name as string from which the data is extracted
#' @return the data as df with the id, entry number per position and the extracted laufzeit
#' only the first entry should be used, multiple entries indicate that multiple products are in the position

#' @export
extract_laufzeit_dauer <- function(df_position, field = "description") {
  df_position %>%
    Billomatics::get_extracted_information(df = ., field = field) %>%
    Billomatics::read_KeysFromDescription(., sep = ":") %>%
    dplyr::filter(key == "Laufzeit") %>%
    dplyr::group_by(id, key) %>%
    dplyr::mutate(entry_no = row_number()) %>%
    tidyr::pivot_wider(
      id_cols = c(id, entry_no),
      names_from = key,
      values_from = text
    )

}

#' get the number of impressions ordered
#' this function allows to get the impressions from the orders
#' @param df the df which holds the data that was extracted from the field
#' @param field2extract the field we want to get
#' @return the data as df with the id and the extracted Impressions and Bonus

#' @export
get_impressions_and_bonus <- function(df, field2extract = "description") {
  df <- df %>%
    mutate(description = str_replace_all(
      description,
      pattern = c("Produktbeschreibung:" = "Produktbeschreibung:\n", "[Pp]lus" = "+")
    ))

  field <- field2extract
  extracted_information <-
    Billomatics::get_extracted_information(df, field = field)
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

#----------------------------------------------------------------------------------------------------------------#
##------------------------------------ Functions to create documents export to jp5 ----------------------------------------------
#----------------------------------------------------------------------------------------------------------------#



#' get_invoice_information_from_document
#' This function takes a data frame with comments and turns them into a table containing the reported performance
#' @param df_document The data frame with all the documents intro
#' @param field For the confirmation the information is contained in the "intro". For invoices in "note". #'
#' @return The function returns a df key - value with all the fields that are found in the intro
#'
#' @export
get_invoice_information_from_document <- function(df_document,
                                                  field = "intro") {

  if(field == "intro") {

   extracted_field <- df_document %>%
    mutate(intro = intro) %>%
    #filter(id == 405476) %>%
    mutate(intro = str_remove_all(intro, "&nbsp;|amp;"),
           intro = str_replace_all(intro, ";", ";\n")) %>%
    Billomatics::get_extracted_information(.,field)}

  else if (field == "note") {
    extracted_field <- df_document %>%
      mutate(note = note) %>%
      #filter(id == 405476) %>%
      mutate(note = str_remove_all(note, "&nbsp;|amp;"),
             note = str_replace_all(note, ";", ";\n")) %>%
      Billomatics::get_extracted_information(.,field)
  } else {

    return("no known field")
  }

  ## only if there was a field to extract we can continue
  if(exists("extracted_field")){
  extracted_field %>%
    separate(note, into = c("key", "value"), sep = ":",extra = "drop", fill = "right") %>%
    mutate(value = na_if(str_squish(value), "")) %>%
    drop_na(value) %>%
    filter(
      !str_detect(key, c(
        "Laufzeit|Rechnungsstellung erfolgt|Zielsetzung|Zielgr"
      )),
      !str_detect(value, pattern = "AN....SF...."),
      !str_detect(value, pattern = "AB....SF....")
    ) %>%
    mutate(
      value = str_squish(str_remove_all(value, "&nbsp;")),
      key = str_squish(str_remove_all(key, "\\(Auftraggeber\\)|\\)|;")),
      key = str_trim(str_replace_all(
        key, pattern = c("nummer" = "nr", "gruppe" = "gr")
      ))
    )
  }
}

#'get_information_from_comments
#' This function takes a dataframe with comments and turns them into a table containing the reported performance
#' @param df_comments The dataframe with all the comments downloaded
#' @param document_type This passes the origin of the comment - visible in the comment_df NAME_id
#' @param desired_information What information from the comments should be returned, the invoice details OR CPC performance
#' @return The function returns a df with either the extra billing informations in a table or the CPC performance
#'
#' @export
get_information_from_comments <- function(df_comments,
                                          document_type = "confirmation",
                                          desired_information = c("Rechnungszusatzinformation", "CPC Kampagne")) {
  # To be able to use the function for all document types we use a document_id
  document_id_var <- paste0(document_type, "_id")
  df_comments <- df_comments %>%
    mutate(document_id = get(document_id_var)) %>%
    unnest(everything())


  df_comments %>%
    ## only keep comments with the desired information
    filter(str_detect(comment, desired_information)) %>%
    mutate(
      text = str_remove_all(
        comment,
        "Rechnungszusatzinformationen:|Rechnungszusatzinformation:|CPC Kampagne:|\\.|€"
      )
    ) %>%
    Billomatics::get_extracted_information(., "text")  %>%
    separate(note, into = c("key", "value"), sep = ":") %>%
    mutate(value = na_if(str_squish(value), ""),
           key = str_trim(str_replace_all(
             key, pattern = c("nummer" = "nr", "gruppe" = "gr")
           ))) %>%
    drop_na(value) %>%
    filter(
      !str_detect(value, "20022390 mit der Billomat ID"),!str_detect(key, "Billomat-ID|Billomat ID")
    ) %>%
    left_join(df_comments %>%
                distinct(id, document_id, created), by = c("id")) %>%
    group_by(key, document_id) %>%
    slice_max(created) %>%
    mutate(document_id= as.numeric(document_id))
}


#' consolidate_invoice_information
#' This function is used to create one dataframe from the billing information in the comments
#' and the billing information from the document. The information from the comments supersede the information
#' in the document.
#' @param df_Billing_information_comment the dataframe containing the extracted information of comments
#' @param df_Billing_information_document the dataframe containing the extracted information of the document
#' @return The function returns one dataframe with the document_id, key col containing the name of the information and value its value.
#'
#' @export
consolidate_invoice_information <- function(df_Billing_information_comment,
                                            df_Billing_information_document
                                            ) {

  if(!"document_id" %in% colnames(df_Billing_information_document)) {
    df_Billing_information_document <- df_Billing_information_document %>%
      mutate(document_id = as.numeric(id))
  }

  df_Billing_information_document %>%
  bind_rows(df_Billing_information_comment %>%
    select(document_id, key, value)) %>%
    ungroup() %>%
    distinct(document_id, value, .keep_all = TRUE) %>%
    select(document_id, key, value) %>%
    mutate(document_id = as.numeric(document_id))
}

