################################################################################-
# ----- Description -------------------------------------------------------------
#
# Helper and validation functions for CRM API operations.
# These functions are used internally by the CRM API functions to validate
# input data and build API requests.
#
# ------------------------------------------------------------------ #
# Authors@R: Moritz Hemmann
# Date: 2024/08
#
################################################################################-
# ----- Start -------------------------------------------------------------------

#' Validate required columns in dataframe
#' @param df dataframe to validate
#' @param required_cols character vector of required column names
#' @keywords internal
validate_required_columns <- function(df, required_cols) {
  missing_cols <- setdiff(required_cols, names(df))
  if (length(missing_cols) > 0) {
    stop(paste("❌ Missing required columns:", paste(missing_cols, collapse = ", ")))
  }
}

#' Filter dataframe by field_type and action
#' @param df dataframe to filter
#' @param field_type_value value to filter field_type by (e.g., "tag", "custom_field")
#' @param action_value value to filter action by (e.g., "add", "remove")
#' @keywords internal
filter_by_field_and_action <- function(df, field_type_value, action_value) {
  # Validate that required filter columns exist
  validate_required_columns(df, c("field_type", "action"))

  df <- df %>%
    filter(field_type == field_type_value) %>%
    filter(action == action_value)

  if (nrow(df) == 0) {
    warning("⚠️ No rows to process after filtering")
    return(NULL)
  }

  df
}

#' Validate attachable_id column
#' @param df dataframe to validate
#' @keywords internal
validate_attachable_id <- function(df) {
  invalid_ids <- df %>%
    filter(is.na(attachable_id) | attachable_id < 100000)

  if (nrow(invalid_ids) > 0) {
    stop(paste0("❌ attachable_id must be at least 6 digits (>= 100000). ",
                "Found invalid IDs: ", paste(unique(invalid_ids$attachable_id), collapse = ", ")))
  }
}

#' Validate attachable_type column
#' @param df dataframe to validate
#' @keywords internal
validate_attachable_type <- function(df) {
  invalid_types <- df %>%
    filter(!attachable_type %in% c("people", "companies"))

  if (nrow(invalid_types) > 0) {
    stop(paste0("❌ attachable_type must be 'people' or 'companies'. ",
                "Found invalid types: ", paste(unique(invalid_types$attachable_type), collapse = ", ")))
  }
}

#' Validate custom_fields_id column
#' @param df dataframe to validate
#' @keywords internal
validate_custom_fields_id <- function(df) {
  invalid_ids <- df %>%
    filter(is.na(custom_fields_id) | custom_fields_id <= 0)

  if (nrow(invalid_ids) > 0) {
    stop(paste0("❌ custom_fields_id must be a positive number. ",
                "Found invalid IDs at rows: ", paste(which(is.na(df$custom_fields_id) | df$custom_fields_id <= 0), collapse = ", ")))
  }
}

#' Validate field_name column (for tags)
#' @param df dataframe to validate
#' @keywords internal
validate_field_name <- function(df) {
  invalid_field_names <- df %>%
    filter(is.na(field_name) | trimws(as.character(field_name)) == "")

  if (nrow(invalid_field_names) > 0) {
    stop(paste0("❌ field_name (tag name) must not be empty or NA. ",
                "Found invalid values at rows: ", paste(which(is.na(df$field_name) | trimws(as.character(df$field_name)) == ""), collapse = ", ")))
  }
}

#' Validate value column (for custom fields)
#' @param df dataframe to validate
#' @keywords internal
validate_value <- function(df) {
  invalid_values <- df %>%
    filter(is.na(value) | trimws(as.character(value)) == "")

  if (nrow(invalid_values) > 0) {
    stop(paste0("❌ value must not be empty or NA. ",
                "Found invalid values at rows: ", paste(which(is.na(df$value) | trimws(as.character(df$value)) == ""), collapse = ", ")))
  }
}

#' Validate that a column is not empty
#' @param df dataframe to validate
#' @param column_name name of the column to validate
#' @keywords internal
validate_not_empty <- function(df, column_name) {
  if (!column_name %in% names(df)) {
    stop(paste0("❌ Column '", column_name, "' not found in dataframe"))
  }

  if (any(is.na(df[[column_name]]) | df[[column_name]] == "")) {
    stop(paste0("❌ ", column_name, " cannot be empty"))
  }
}

#' Validate atype column (for contact details)
#' @param df dataframe to validate
#' @keywords internal
validate_atype <- function(df) {
  invalid_atypes <- df %>%
    filter(is.na(atype) | trimws(as.character(atype)) == "")

  if (nrow(invalid_atypes) > 0) {
    stop(paste0("❌ atype (contact detail type) must not be empty or NA. ",
                "Found invalid values at rows: ", paste(which(is.na(df$atype) | trimws(as.character(df$atype)) == ""), collapse = ", ")))
  }
}

#' Build CRM API URL for DELETE operations
#' @param attachable_type "people" or "companies"
#' @param attachable_id ID of the person/company
#' @param resource_type "tags" or "custom_fields"
#' @param resource_id ID of the tag or custom field
#' @keywords internal
build_crm_delete_url <- function(attachable_type, attachable_id, resource_type, resource_id) {
  paste0(
    "https://api.centralstationcrm.net/api/",
    attachable_type, "/",
    attachable_id, "/",
    resource_type, "/",
    resource_id
  )
}

#' Transform attachable_type to API format
#' @param attachable_type "people" or "companies"
#' @keywords internal
transform_attachable_type <- function(attachable_type) {
  ifelse(attachable_type == "companies", "Company",
         ifelse(attachable_type == "people", "Person", attachable_type))
}

#' Map country name to ISO country code
#' @param country_name Country name in German (e.g., "Deutschland", "Frankreich", "USA")
#' @return ISO country code (e.g., "DE", "FR", "US")
#' @export
map_country_to_code <- function(country_name) {
  country_mapping <- c(
    # DACH Region
    "Deutschland" = "DE",
    "DE" = "DE",

    "Österreich" = "AT",
    "Oesterreich" = "AT",
    "AT" = "AT",

    "Schweiz" = "CH",
    "CH" = "CH",

    "Liechtenstein" = "LI",
    "LI" = "LI",

    # Europa
    "Frankreich" = "FR",
    "FR" = "FR",

    "Italien" = "IT",
    "IT" = "IT",

    "Spanien" = "ES",
    "ES" = "ES",

    "Portugal" = "PT",
    "PT" = "PT",

    "Niederlande" = "NL",
    "NL" = "NL",

    "Belgien" = "BE",
    "BE" = "BE",

    "Polen" = "PL",
    "PL" = "PL",

    "Tschechien" = "CZ",
    "CZ" = "CZ",

    "Vereinigtes Königreich" = "GB",
    "Grossbritannien" = "GB",
    "Großbritannien" = "GB",
    "GB" = "GB",
    "UK" = "GB",

    "Irland" = "IE",
    "IE" = "IE",

    "Dänemark" = "DK",
    "Daenemark" = "DK",
    "DK" = "DK",

    "Schweden" = "SE",
    "SE" = "SE",

    "Norwegen" = "NO",
    "NO" = "NO",

    "Finnland" = "FI",
    "FI" = "FI",

    # Weltweit
    "USA" = "US",
    "Vereinigte Staaten" = "US",
    "US" = "US",

    "Kanada" = "CA",
    "CA" = "CA",

    "China" = "CN",
    "CN" = "CN",

    "Japan" = "JP",
    "JP" = "JP",

    "Indien" = "IN",
    "IN" = "IN",

    "Brasilien" = "BR",
    "BR" = "BR",

    "Australien" = "AU",
    "AU" = "AU",

    "Russland" = "RU",
    "RU" = "RU",

    "Türkei" = "TR",
    "Tuerkei" = "TR",
    "TR" = "TR"
  )

  ifelse(!is.na(country_name) & country_name %in% names(country_mapping),
         country_mapping[country_name],
         country_name)
}

#' Map German state name to state code
#' @param state_name State name (e.g., "Bayern", "Nordrhein-Westfalen")
#' @return State code (e.g., "BY", "NW")
#' @keywords internal
map_state_to_code_de <- function(state_name) {
  state_mapping <- c(
    # German states
    "Baden-Württemberg" = "BW",
    "Baden-Wuerttemberg" = "BW",
    "BW" = "BW",

    "Bayern" = "BY",
    "Bavaria" = "BY",
    "BY" = "BY",

    "Berlin" = "BE",
    "BE" = "BE",

    "Brandenburg" = "BB",
    "BB" = "BB",

    "Bremen" = "HB",
    "HB" = "HB",

    "Hamburg" = "HH",
    "HH" = "HH",

    "Hessen" = "HE",
    "Hesse" = "HE",
    "HE" = "HE",

    "Mecklenburg-Vorpommern" = "MV",
    "MV" = "MV",

    "Niedersachsen" = "NI",
    "Lower Saxony" = "NI",
    "NI" = "NI",

    "Nordrhein-Westfalen" = "NW",
    "North Rhine-Westphalia" = "NW",
    "NRW" = "NW",
    "NW" = "NW",

    "Rheinland-Pfalz" = "RP",
    "Rhineland-Palatinate" = "RP",
    "RP" = "RP",

    "Saarland" = "SL",
    "SL" = "SL",

    "Sachsen" = "SN",
    "Saxony" = "SN",
    "SN" = "SN",

    "Sachsen-Anhalt" = "ST",
    "Saxony-Anhalt" = "ST",
    "ST" = "ST",

    "Schleswig-Holstein" = "SH",
    "SH" = "SH",

    "Thüringen" = "TH",
    "Thueringen" = "TH",
    "Thuringia" = "TH",
    "TH" = "TH"
  )

  ifelse(!is.na(state_name) & state_name %in% names(state_mapping),
         state_mapping[state_name],
         state_name)
}

#' Map Austrian state name to state code
#' @param state_name State name (e.g., "Wien", "Niederösterreich")
#' @return State code (e.g., "9", "3")
#' @keywords internal
map_state_to_code_at <- function(state_name) {
  state_mapping <- c(
    # Austrian states (Bundesländer)
    "Burgenland" = "1",
    "Kärnten" = "2",
    "Kaernten" = "2",
    "Carinthia" = "2",

    "Niederösterreich" = "3",
    "Niederoesterreich" = "3",
    "Lower Austria" = "3",

    "Oberösterreich" = "4",
    "Oberoesterreich" = "4",
    "Upper Austria" = "4",

    "Salzburg" = "5",

    "Steiermark" = "6",
    "Styria" = "6",

    "Tirol" = "7",
    "Tyrol" = "7",

    "Vorarlberg" = "8",

    "Wien" = "9",
    "Vienna" = "9"
  )

  ifelse(!is.na(state_name) & state_name %in% names(state_mapping),
         state_mapping[state_name],
         state_name)
}

#' Map Swiss canton name to canton code
#' @param canton_name Canton name (e.g., "Zürich", "Bern")
#' @return Canton code (e.g., "ZH", "BE")
#' @keywords internal
map_canton_to_code_ch <- function(canton_name) {
  canton_mapping <- c(
    # Swiss cantons
    "Aargau" = "AG",
    "Appenzell Ausserrhoden" = "AR",
    "Appenzell Innerrhoden" = "AI",
    "Basel-Landschaft" = "BL",
    "Basel-Stadt" = "BS",
    "Bern" = "BE",
    "Berne" = "BE",
    "Fribourg" = "FR",
    "Freiburg" = "FR",
    "Genève" = "GE",
    "Genf" = "GE",
    "Geneva" = "GE",
    "Glarus" = "GL",
    "Graubünden" = "GR",
    "Graubuenden" = "GR",
    "Grisons" = "GR",
    "Jura" = "JU",
    "Luzern" = "LU",
    "Lucerne" = "LU",
    "Neuchâtel" = "NE",
    "Neuenburg" = "NE",
    "Nidwalden" = "NW",
    "Obwalden" = "OW",
    "Schaffhausen" = "SH",
    "Schwyz" = "SZ",
    "Solothurn" = "SO",
    "St. Gallen" = "SG",
    "Sankt Gallen" = "SG",
    "Tessin" = "TI",
    "Ticino" = "TI",
    "Thurgau" = "TG",
    "Uri" = "UR",
    "Vaud" = "VD",
    "Waadt" = "VD",
    "Valais" = "VS",
    "Wallis" = "VS",
    "Zug" = "ZG",
    "Zürich" = "ZH",
    "Zuerich" = "ZH",
    "Zurich" = "ZH"
  )

  ifelse(!is.na(canton_name) & canton_name %in% names(canton_mapping),
         canton_mapping[canton_name],
         canton_name)
}

#' Map state/canton name to code based on country
#' @param state_name State or canton name in German
#' @param country_code ISO country code (e.g., "DE", "AT", "CH")
#' @return State/canton code
#' @export
map_state_to_code <- function(state_name, country_code) {
  # Handle NA values
  if (is.na(state_name) || is.na(country_code)) {
    return(state_name)
  }

  # Route to appropriate mapper based on country
  if (country_code == "DE") {
    return(map_state_to_code_de(state_name))
  } else if (country_code == "AT") {
    return(map_state_to_code_at(state_name))
  } else if (country_code == "CH") {
    return(map_canton_to_code_ch(state_name))
  } else {
    # Return as-is for unsupported countries
    return(state_name)
  }
}

#' Validate any positive ID column
#' @param df dataframe to validate
#' @param id_column_name name of the ID column to validate
#' @param id_label descriptive label for error messages
#' @keywords internal
validate_positive_id <- function(df, id_column_name, id_label = NULL) {
  if (is.null(id_label)) {
    id_label <- id_column_name
  }

  if (!id_column_name %in% names(df)) {
    stop(paste0("❌ Column '", id_column_name, "' not found in dataframe"))
  }

  invalid_ids <- df %>%
    filter(is.na(.data[[id_column_name]]) | .data[[id_column_name]] <= 0)

  if (nrow(invalid_ids) > 0) {
    stop(paste0("❌ ", id_label, " must be a positive number. ",
                "Found invalid IDs at rows: ", paste(which(is.na(df[[id_column_name]]) | df[[id_column_name]] <= 0), collapse = ", ")))
  }
}

#' Check if optional field has valid value
#' @param df dataframe
#' @param field_name name of the field to check
#' @param row_index row index
#' @return TRUE if field exists, is not NA and not empty string
#' @keywords internal
has_valid_value <- function(df, field_name, row_index) {
  field_name %in% names(df) &&
    !is.na(df[[field_name]][row_index]) &&
    trimws(as.character(df[[field_name]][row_index])) != ""
}

#' Get value from optional field or return NA
#' @param df dataframe
#' @param field_name name of the field
#' @param row_index row index
#' @param convert_func optional conversion function (e.g., as.numeric, as.logical)
#' @return field value or NA if not available
#' @keywords internal
get_optional_value <- function(df, field_name, row_index, convert_func = NULL) {
  if (has_valid_value(df, field_name, row_index)) {
    value <- df[[field_name]][row_index]
    if (!is.null(convert_func)) {
      return(convert_func(value))
    }
    return(value)
  }
  return(NA)
}
