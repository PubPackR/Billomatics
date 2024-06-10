
#' rename_product
#'
#' This function renames the products so that they are consistent
#' @param df the data frame with the product names
#' @return the df with the products short categories
#' @export
rename_product <- function(df) {
  df %>%
    group_by(confirmation_number) %>%
    mutate(
      Produktart =  case_when(
        str_detect(string = Produkt, pattern =  "Video.*|Bewerberboost|Bewerber-Boost|Restkontingente|Social|TikTok|Employer Branding|Cost per Click|Produktmarketing|Kombi-Kampagne|Kombikampagne") ~ "Video Ads",
        str_detect(string = Produkt, pattern = "Lizenzen|E-Learning") ~ "E-Learning",
        # str_detect(string = title, pattern = "Erklärvideos|Lerninhalte") &
        #   Gebühr ~ "E-Learning",
        str_detect(string = Produkt, pattern = "Stellen") ~ "Stellenanzeigen",
        str_detect(string = Produkt, pattern = "Newsletter|Direct Mailing") ~ "Direct Mailing",
        str_detect(string = Produkt, pattern = ".*profil") ~ "Karriereprofil",
        str_detect(
          string = Produkt,
          "Sponsorship|Logo|Branding_Berufe|Branding Berufsbilder|Exklusiv-Werbepartnerschaft"
        ) ~ "Branding Berufe"
      ),
      Gebühr = str_detect(Produkt, "ebühr"),
      Produkt_short = case_when(
        str_detect(string = Produkt, pattern =  "Video.*|Bewerberboost|Bewerber-Boost|Restkontingente|Social|TikTok|Employer Branding|Cost per Click|Produktmarketing|Kombi-Kampagne|Kombikampagne") ~ "Video Ads",
        str_detect(string = Produkt, pattern = "Lizenzen|E-Learning") ~ "E-Learning",

      # str_detect(string = title, pattern = "Erklärvideos|Lerninhalte") &
      #   Gebühr ~ "E-Learning",
      str_detect(string = Produkt, pattern = "Stellen") ~ "Stellenanzeigen",
      str_detect(string = Produkt, pattern = "Stellen") & Gebühr ~ "Stellenanzeigen",
      str_detect(string = Produkt, pattern = "Lizenzen|E-Learning|Lerninhalte") & Gebühr ~ "E-Learning",
      str_detect(string = Produkt, pattern = "Newsletter|Direct Mailing") ~ "Direct Mailing",
      str_detect(string = Produkt, pattern = ".*profil") ~ "Karriereprofil",
      str_detect(string = Produkt, "Sponsorship|Logo|Branding_Berufe|Branding Berufsbilder|Exklusiv-Werbepartnerschaft") ~ "Branding Berufe",
      is.na(title.article) & any(Produktart =="Video Ads" ) ~ "Video Ads",
      TRUE ~ "Stellenanzeigen"
    ),
    Produkt_short = factor(
      as.factor(Produkt_short),
      levels = c(
        "E-Learning",
        "Direct Mailing",
        "Karriereprofil",
        "Video Ads",
        "Branding Berufe",
        "Stellenanzeigen")
    )
  )

}
