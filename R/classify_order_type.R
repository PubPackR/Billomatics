#' Classify Order Type (New vs. Recurring Business)
#'
#' This function adds a column `order_type` to the provided dataframe.
#' It classifies contracts based on the gap between the previous contract's
#' service end date and the current order date.
#'
#' A contract is classified as **"Erstauftrag"** (New Business) if:
#' \enumerate{
#'   \item The client has no previous contracts within the dataset.
#'   \item The gap between the previous contract's \code{service_end} and the
#'         current \code{order_date} is greater than 24 months (indicating no revenue
#'         in the preceding 24 months).
#' }
#' Otherwise, it is classified as **"Folgeauftrag"** (Recurring Business).
#'
#' @param df A dataframe containing contract data. Must include the columns:
#'   \code{client_id}, \code{confirmation_number}, \code{order_date},
#'   \code{service_start}, and \code{service_end}.
#'
#' @return The original dataframe with an additional column \code{order_type}.
#' @export
#' @importFrom dplyr distinct arrange group_by mutate lag if_else case_when ungroup select left_join %>%
classify_order_type <- function(df) {

  # 1. Validate required columns
  required_cols <- c("client_id", "confirmation_number", "order_date", "service_start", "service_end")
  if (!all(required_cols %in% names(df))) {
    missing <- setdiff(required_cols, names(df))
    stop(paste("Missing required columns in dataframe:", paste(missing, collapse = ", ")))
  }

  # 2. Get unique contracts with their order and service dates
  # We select distinct records to avoid duplicates skewing the lag calculation
  contracts <- df %>%
    dplyr::distinct(client_id, confirmation_number, order_date, service_start, service_end) %>%
    dplyr::arrange(client_id, order_date)

  # 3. Classify based on revenue gaps (service_end -> next order_date)
  contracts_classified <- contracts %>%
    dplyr::group_by(client_id) %>%
    dplyr::mutate(
      # Get the service_end date of the immediately preceding contract
      previous_service_end = dplyr::lag(service_end),

      # Calculate months from previous contract's END to this contract's ORDER
      # Using 30.44 days as the average month length
      months_since_last_revenue = dplyr::if_else(
        is.na(previous_service_end),
        NA_real_,
        as.numeric(difftime(order_date, previous_service_end, units = "days")) / 30.44
      ),

      # Apply classification logic:
      # - No previous contract (NA) -> Erstauftrag
      # - Gap > 24 months -> Erstauftrag
      # - Gap <= 24 months -> Folgeauftrag
      order_type = dplyr::case_when(
        is.na(previous_service_end) ~ "Erstauftrag",
        months_since_last_revenue > 24 ~ "Erstauftrag",
        TRUE ~ "Folgeauftrag"
      )
    ) %>%
    dplyr::ungroup() %>%
    dplyr::select(confirmation_number, order_type)

  # 4. Join the classification back to the original data
  df %>%
    dplyr::left_join(contracts_classified, by = "confirmation_number")
}