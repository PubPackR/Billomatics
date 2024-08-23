################################################################################-
# ----- Description -------------------------------------------------------------
#
# These Functions could be used to get and process Offers and ABs from the
# attached Documents in the CRM.
#
# ------------------------------------------------------------------ #
# Authors@R: Moritz Hemmann
# Date: 2024/08
#
################################################################################-
# ----- Start -------------------------------------------------------------------

###############################################################################-

#' matching_table_AB_AN
#'
#' @param attachments all attachments from CRM
#' @return df with attachment creation
#'
#' @export
extract_offers_and_abs_from_crm <- function(attachments) {
  attachments <- attachments %>%
    select(attachment_attachable_id, attachment_filename, attachment_created_at)

  attachments_AB <- attachments %>%
    filter(grepl("AB2", attachment_filename)) %>%
    filter(!(grepl("(?-i)report", attachment_filename))) %>%
    mutate(document_id = str_extract(attachment_filename, "AB2[^_\\W]+")) %>%
    select(-attachment_filename) %>%
    mutate(type = "AB")

  attachments_AN <- attachments %>%
    filter(grepl("AN2", attachment_filename)) %>%
    mutate(document_id = str_extract(attachment_filename, "AN2[^_\\W]+")) %>%
    select(-attachment_filename) %>%
    mutate(type = "Offer")

  attachments_AN_and_AB <- bind_rows(attachments_AN, attachments_AB)
}

###############################################################################-

#' matching_table_AB_AN
#'
#' @param confirmations_billomat all confirmations from Billomat (DB)
#' @param offers_billomat all confirmations from Billomat (DB)
#' @return
#' A data frame with four columns:
#' \describe{
#'   \item{attachment_attachable_id}{(int) id of the protocol or task with the attachment}
#'   \item{attachment_created_at}{(chr) creation date attachement (not document)}
#'   \item{document_id}{(chr) document_id in CRM}
#'   \item{type}{(chr) "Offer" or "AB"}
#' }
#'
#' @export
matching_table_AB_AN <- function(confirmations_billomat, offers_billomat) {
  confirmations_billomat <- confirmations_billomat %>%
      select(confirmation_number, offer_id) %>%
      na.omit() %>%
      distinct()

  match_ab_an_numbers <- offers_billomat %>%
    select(id, offer_number) %>%
    distinct() %>%
    left_join(confirmations_billomat, by = c("id" = "offer_id")) %>%
    select(-id) %>%
    na.omit()
}

###############################################################################-

#' matching_ABs_Billomat_CRM
#'
#' @param attachments all attachments from CRM
#' @param confirmations_billomat all confirmations from Billomat (DB)
#' @param offers_billomat all confirmations from Billomat (DB)
#' @param simplified_protocols all basic informations about CRM protocols
#' @return
#' A data frame with four columns:
#' \describe{
#'   \item{attachment_created_at}{(chr) creation date attachement (not document)}
#'   \item{document_id}{(chr) document_id in CRM}
#'   \item{type}{(chr) "Offer" or "AB"}
#'   \item{document_number}{(int) number of Offer or AB}
#'   \item{attachable_id}{(int) id of the protocol or task with the attachment}
#'   \item{person_id}{(int) "Offer" or "AB"}
#' }
#'
#' @export
matching_ABs_Billomat_CRM <- function(attachments, confirmations_billomat, offers_billomat, simplified_protocols){

  # get confirmation for every offer
  match_ab_an_numbers <- matching_table_AB_AN(confirmations_billomat, offers_billomat)

  crm_ABs_Offers <-
    extract_offers_and_abs_from_crm(attachments) %>%
    select(-attachment_created_at) %>%
    distinct() %>%
    left_join(
      match_ab_an_numbers,
      by = c("document_id" = "offer_number"),
      relationship = "many-to-many"
    ) %>%
    mutate(document_number = ifelse(type == "AB", document_id, confirmation_number)) %>%
    select(-document_id, -confirmation_number) %>%
    arrange(type) %>%
    rename(attachable_id = attachment_attachable_id) %>%
    filter(!is.na(document_number)) %>%
    left_join(simplified_protocols %>%  distinct(person_ids, id) %>% rename(person_id = person_ids) %>% mutate(id = as.integer(id)), by = c("attachable_id" = "id"), relationship = "many-to-many")
}

###############################################################################-

#' enrich_offers_ABs_with_start_end
#'
#' @param crm_ABs_Offers data frame from matching_ABs_Billomat_CRM
#' @param latest_rev_per_mensem rev_per_mensem
#' @return enriched crm_ABs_Offers with start and end of campaign
#'
#' @export
enrich_offers_ABs_with_start_end <- function(crm_ABs_Offers, latest_rev_per_mensem){
  latest_rev_per_mensem_matchable <- latest_rev_per_mensem %>%
    group_by(confirmation_number) %>%
    summarise(StartDate = last(StartDate),
              EndDate = last(EndDate)) %>%
    na.omit() %>%
    right_join(crm_ABs_Offers, by = c("confirmation_number" = "document_number"))
}

###############################################################################-

#' prepare_companies_crm
#'
#' @param companies_crm companies file from crm main_data_files
#' @return companies file from crm main_data_files (prepared with prepare_companies_crm())
#'
#' @export
prepare_companies_crm <- function(companies_crm) {
  companies <- companies_crm %>%
    distinct() %>%
    select(custom_fields_custom_fields_type_name, custom_fields_name, id, positions_person_id, name) %>%
    rename(person_id = positions_person_id) %>%
    filter(custom_fields_custom_fields_type_name == "Billomat-Kundennummer") %>%
    select(-custom_fields_custom_fields_type_name) %>%
    rename(billomat_kd = custom_fields_name) %>%
    rename(crm_name = name,
           crm_company_id = id)
}

###############################################################################-

#' get_expanded_AB_AN_from_CRM
#'
#' @param confirmations_billomat confirmations from billomat db
#' @param offers_billomat offers from billomat db
#' @param attachments attachments file from crm main_data_files
#' @param simplified_protocols protocols file (simplified) from crm main_data_files
#' @param companies companies file from crm main_data_files (prepared with prepare_companies_crm())
#' @return df with all offers and ABs from the CRM Feed
#'
#' @export
get_expanded_AB_AN_from_CRM <- function(confirmations_billomat, offers_billomat, attachments, simplified_protocols, companies){

  confirmations_billomat_non_cancelled <- confirmations_billomat %>%
    filter(status != "CANCELED" & status != "DRAFT")

  match_ab_an_numbers <- matching_table_AB_AN(confirmations_billomat_non_cancelled, offers_billomat)

  attachments_AN_and_AB <- extract_offers_and_abs_from_crm(attachments) %>%
    left_join(match_ab_an_numbers, by = c("document_id" = "offer_number"), relationship = "many-to-many") %>%
    left_join(match_ab_an_numbers, by = c("document_id" = "confirmation_number"), relationship = "many-to-many") %>%
    mutate(offer_number = ifelse(type == "Offer", document_id, offer_number),
           confirmation_number = ifelse(type == "AB", document_id, confirmation_number)) %>%
    select(-document_id) %>%
    filter(!is.na(attachment_attachable_id))

  attachments_AN_and_AB <- attachments_AN_and_AB %>%
    # match with person_id through protocol id
    left_join(
      simplified_protocols %>%
        mutate(
          person_id_overall = ifelse(is.na(person_ids), protocol_email_person_ids, person_ids)
        ) %>%
        select(person_id_overall, id) %>%
        mutate(id = as.integer(id)),
      by = c("attachment_attachable_id" = "id"),
      relationship = "many-to-many"
    ) %>%
    rename(person_id = person_id_overall) %>%
    # add company id
    left_join(
      companies %>%
        select(crm_company_id, person_id),
      by = "person_id",
      relationship = "many-to-many"
    ) %>%
    # add startDate for confirmations
    left_join(
      latest_rev_per_mensem %>%
        ungroup() %>%
        select(confirmation_number, StartDate, EndDate, Auftragsdatum),
      by = c("confirmation_number" = "confirmation_number"),
      relationship = "many-to-many"
    ) %>%
    left_join(
      offers_billomat %>%
        ungroup() %>%
        select(offer_number, date, offer_updated = updated, status) %>%
        filter(!is.na(offer_number)),
      by = "offer_number",
      relationship = "many-to-many"
    ) %>%
    distinct() %>%
    mutate(confirmation_number = ifelse(confirmation_number %in% confirmations_billomat_non_cancelled$confirmation_number, confirmation_number, NA)) %>%
    filter(!(is.na(confirmation_number) & is.na(offer_number)))
}
