#' Filter product mapping table by column
#'
#' Returns rows from the mapping table where the specified column is not NA.
#' Helper function used to get relevant patterns for product classification.
#'
#' @param mapping_table Data frame containing the product mapping data
#' @param column_name Name of the column to filter by (e.g., "product_shortname")
#' @return Filtered data frame with non-NA values in the specified column
#' @export
get_patterns_for_column <- function(mapping_table, column_name) {
  mapping_table %>%
    filter(!is.na(.data[[column_name]]))
}

#' Apply product mapping using vectorized pattern matching
#'
#' Efficiently matches product names to categories by iterating through patterns
#' once and applying matches vectorially. Respects pattern priority (first match wins).
#'
#' @param produkt_names Character vector of product names to classify
#' @param mapping_table Pre-filtered mapping table with patterns and category values
#' @param column_name Name of the column containing category values to return
#' @return Character vector of matched categories (original names if no match)
#'
#' #' @details
#' ## How "First Match Wins" Works:
#'
#' The function uses a trick to track which items have been matched:
#' - `result` starts identical to `produkt_names`
#' - After a match: `result[i]` becomes the category (e.g., "SF_EMPLOYBRAND")
#' - The condition `result == produkt_names` becomes FALSE for matched items
#' - This prevents re-matching in subsequent iterations
#'
#' Example with produkt_names = c("Employer Branding", "Bewerber"):
#'
#' **Iteration 1** (Pattern "Employer Branding" → "SF_EMPLOYBRAND"):
#' - unmatched_mask = c(TRUE, TRUE) (both still original)
#' - matches = c(TRUE, FALSE) (only first matches pattern)
#' - result = c("SF_EMPLOYBRAND", "Bewerber") (first updated!)
#'
#' **Iteration 2** (Pattern "Bewerber" → "SF_BEWERBBOOST"):
#' - unmatched_mask = c(FALSE, TRUE) (first already changed!)
#' - matches = c(FALSE, TRUE) (only second matches)
#' - result = c("SF_EMPLOYBRAND", "SF_BEWERBBOOST")
#'
#' This vectorized approach is faster than nested loops while maintaining
#' the correct pattern priority from the CSV.
#' @export
apply_mapping_vectorized <- function(produkt_names, mapping_table, column_name) {
  result <- produkt_names # Start with original names as default

  # Process patterns in order (first match wins, so we only update unmatched rows)
  for (i in seq_len(nrow(mapping_table))) {
    pattern <- mapping_table$pattern[i]
    category <- mapping_table[[column_name]][i]

    if (!is.na(pattern)) {
      # Find rows that: (1) have a product name, (2) haven't been matched yet, (3) match this pattern
      unmatched_mask <- !is.na(produkt_names) & (result == produkt_names)
      matches <- unmatched_mask & stringr::str_detect(produkt_names, pattern)

      # Update only the newly matched rows
      result[matches] <- category
    }
  }

  return(result)
}