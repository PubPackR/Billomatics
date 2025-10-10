# R/addins.R

# ------------------------
# Tabellen-Extractor
# ------------------------

schemata <- c("raw", "processed", "mapping", "shiny")
schema_pattern <- paste0("^(", paste(schemata, collapse = "|"), ")\\.")

#' Extract postgres db tables from text
#'
#' @param text character, code to parse
#' @return character vector of table names
extract_postgres_tables <- function(text) {
  text <- paste(text, collapse = "\n")
  tables <- character(0)

  exprs <- tryCatch(parse(text = text), error = function(e) NULL)

  if (!is.null(exprs)) {
    walk_expr <- function(e) {
      if (is.call(e)) {
        args <- as.list(e)

        # analyze postgres_connect with needed_tables parameter
        if ("needed_tables" %in% names(args)) {
          val <- args[["needed_tables"]]
          if (is.call(val) && identical(val[[1]], quote(c))) {
            tables <<- c(tables, as.character(val[-1]))
          } else if (is.character(val)) {
            tables <<- c(tables, val)
          }
        }

        # analyze tbl queries
        if (identical(e[[1]], quote(tbl)) && length(e) >= 3) {
          arg <- e[[3]]
          if (is.call(arg) && identical(arg[[1]], quote(I))) {
            tables <<- c(tables, as.character(arg[[2]]))
          } else if (is.character(arg)) {
            tables <<- c(tables, as.character(arg))
          }
        }

        # analyze postgres_upsert_data function with schema and table parameter
        if (identical(e[[1]], quote(postgres_upsert_data))) {
          sc <- args[["schema"]]
          tb <- args[["table"]]
          if (is.character(tb) && length(tb) == 1) {
            if (grepl("\\.", tb)) {
              tables <<- c(tables, tb)
            } else if (is.character(sc) && length(sc) == 1) {
              tables <<- c(tables, paste0(sc, ".", tb))
            }
          }
        }

        # recursion
        lapply(e, walk_expr)
      }
    }
    lapply(exprs, walk_expr)
  }

  # regex fallback to collect all "schema.table" strings
  regex_pattern <- '["\']([a-z]+\\.[A-Za-z0-9_]+)["\']'
  regex_matches <- stringr::str_match_all(text, regex_pattern)[[1]]
  if (!is.null(regex_matches) && nrow(regex_matches) > 0) {
    tables <- c(tables, regex_matches[,2])
  }

  # only get valid and unique schema
  tables <- unique(tables[stringr::str_detect(tables, schema_pattern)])
  tables
}


# ------------------------
# Addins
# ------------------------

#' analyze_postgres_tables_selection
#'
#' Extract postgres tables from the current RStudio selection
#' @export
analyze_postgres_tables_selection <- function() {
  ctx <- rstudioapi::getActiveDocumentContext()
  text <- ctx$selection[[1]]$text
  tables <- extract_postgres_tables(text)
  if (length(tables) == 0) {
    message("Keine Tabellen in der aktuellen Auswahl gefunden.")
    return(invisible(NULL))
  }
  res <- tibble::tibble(table = unique(tables))
  clipr::write_clip(res$table)
  if (requireNamespace("DT", quietly = TRUE)) {
    DT::datatable(res, options = list(pageLength = 25), rownames = FALSE)
  } else {
    print(res, n = Inf)
  }
}

#' analyze_postgres_tables_file
#'
#' Extract postgres tables from the entire current R script.
#' @export
analyze_postgres_tables_file <- function() {
  ctx <- rstudioapi::getActiveDocumentContext()
  text <- paste(ctx$contents, collapse = "\n")
  tables <- extract_postgres_tables(text)
  if (length(tables) == 0) {
    message("Keine Tabellen im aktuellen Skript gefunden.")
    return(invisible(NULL))
  }
  res <- tibble::tibble(table = unique(tables))
  clipr::write_clip(res$table)
  if (requireNamespace("DT", quietly = TRUE)) {
    DT::datatable(res, options = list(pageLength = 25), rownames = FALSE)
  } else {
    print(res, n = Inf)
  }
}

#' analyze_postgres_tables_project
#'
#' Extract postgres tables from all R scripts in current project, grouped by file.
#' @export
analyze_postgres_tables_project <- function() {
  files <- list.files(".", pattern = "\\.R$", recursive = TRUE, full.names = TRUE)

  all_tables <- purrr::map_dfr(files, function(f) {
    text <- readLines(f, warn = FALSE)
    tt <- extract_postgres_tables(text)
    if (length(tt) > 0) tibble::tibble(file = f, table = tt)
  })

  if (nrow(all_tables) == 0) {
    message("Keine Tabellen im Projekt gefunden.")
    return(invisible(NULL))
  }

  all_tables <- dplyr::arrange(all_tables, file, table)
  clipr::write_clip(unique(all_tables$table))

  if (requireNamespace("DT", quietly = TRUE)) {
    DT::datatable(all_tables, options = list(pageLength = 25), rownames = FALSE)
  } else {
    print(all_tables, n = Inf)
  }
}

