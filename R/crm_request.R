#' Execute CRM API Request with Automatic 429 Retry
#'
#' Wrapper für httr-Requests mit automatischem Retry bei Rate-Limiting.
#' Liest den Retry-After Header und wartet entsprechend.
#'
#' @param method HTTP-Methode: "GET", "POST", "PUT", "DELETE"
#' @param url API-Endpunkt URL
#' @param headers Request-Headers (Named Vector mit X-apikey etc.)
#' @param body Request-Body (optional, für POST/PUT)
#' @param encode Encoding: "json" oder "raw"
#' @param max_retries Maximale Retry-Versuche bei 429 (default: 5)
#' @param query Named list of query parameters for GET requests (optional)
#'
#' @return httr response Objekt
#' @keywords internal
crm_request <- function(method, url, headers, body = NULL,
                        encode = "json", max_retries = 5, query = NULL) {
  attempt <- 1

  repeat {
    response <- switch(method,
      "GET" = httr::GET(url, httr::add_headers(headers), query = query),
      "POST" = httr::POST(url, httr::add_headers(headers), body = body, encode = encode),
      "PUT" = httr::PUT(url, httr::add_headers(headers), body = body, encode = encode),
      "DELETE" = httr::DELETE(url, httr::add_headers(headers), encode = encode),
      stop("Unsupported HTTP method: ", method)
    )

    if (httr::status_code(response) != 429) {
      return(response)
    }

    # 429 - Rate Limited
    if (attempt > max_retries) {
      warning("Max retries (", max_retries, ") exceeded for 429 rate limit")
      return(response)
    }

    # Retry-After Header auslesen (in Sekunden)
    retry_after <- as.numeric(httr::headers(response)$`retry-after`)
    
    if (length(retry_after) == 0 || is.na(retry_after) || retry_after <= 0) {
      retry_after <- 10
    }

    message(
      "Rate limit erreicht (429). Warte ", retry_after,
      "s... (Versuch ", attempt, "/", max_retries, ")"
    )
    Sys.sleep(retry_after)
    attempt <- attempt + 1
  }
}

#' CRM POST Request with Rate Limit Retry
#'
#' @param url API-Endpunkt URL
#' @param headers Request-Headers
#' @param body Request-Body
#' @param encode Encoding (default: "json")
#' @return httr response
#' @export
crm_POST <- function(url, headers, body = NULL, encode = "json") {
  crm_request("POST", url, headers, body, encode)
}

#' CRM PUT Request with Rate Limit Retry
#'
#' @param url API-Endpunkt URL
#' @param headers Request-Headers
#' @param body Request-Body
#' @param encode Encoding (default: "json")
#' @return httr response
#' @export
crm_PUT <- function(url, headers, body = NULL, encode = "json") {
  crm_request("PUT", url, headers, body, encode)
}

#' CRM DELETE Request with Rate Limit Retry
#'
#' @param url API-Endpunkt URL
#' @param headers Request-Headers
#' @param encode Encoding (default: "raw")
#' @return httr response
#' @export
crm_DELETE <- function(url, headers, encode = "raw") {
  crm_request("DELETE", url, headers, NULL, encode)
}

#' CRM GET Request with Rate Limit Retry
#'
#' @param url API-Endpunkt URL
#' @param headers Request-Headers
#' @param query Named list of query parameters (optional)
#' @return httr response
#' @export
crm_GET <- function(url, headers, query = NULL) {
  crm_request("GET", url, headers, query = query)
}

#' Execute CRM API Request with httr2 and Automatic 429 Retry
#'
#' httr2-basierter Wrapper mit automatischem Retry bei Rate-Limiting.
#' Liest den Retry-After Header und wartet entsprechend.
#'
#' @param url API-Endpunkt URL
#' @param headers Named vector mit Request-Headers (z.B. X-apikey)
#' @param query Named list mit Query-Parametern (optional)
#' @param max_retries Maximale Retry-Versuche bei 429 (default: 5)
#'
#' @return httr2 response Objekt
#' @keywords internal
crm_GET2 <- function(url, headers, query = NULL, max_retries = 5) {
  req <- httr2::request(url) |>
    httr2::req_headers(!!!as.list(headers))

  if (!is.null(query)) {
    req <- do.call(httr2::req_url_query, c(list(req), query))
  }

  req <- req |>
    httr2::req_error(is_error = \(resp) FALSE) |>
    httr2::req_retry(
      max_tries = max_retries + 1,
      is_transient = function(resp) httr2::resp_status(resp) == 429,
      after = function(resp) {
        retry_after <- suppressWarnings(
          as.numeric(httr2::resp_header(resp, "retry-after"))
        )
        if (length(retry_after) == 0 || is.na(retry_after) || retry_after <= 0) {
          retry_after <- 10
        }
        message("Rate limit erreicht (429). Warte ", retry_after, "s...")
        retry_after
      }
    )

  httr2::req_perform(req)
}

#' CRM PUT Request with httr2 and Automatic 429 Retry
#'
#' httr2-basierter PUT-Wrapper mit automatischem Retry bei Rate-Limiting.
#'
#' @param url API-Endpunkt URL
#' @param headers Named vector mit Request-Headers (z.B. X-apikey)
#' @param body Request-Body (String oder Liste)
#' @param max_retries Maximale Retry-Versuche bei 429 (default: 5)
#'
#' @return httr2 response Objekt
#' @export
crm_PUT2 <- function(url, headers, body = NULL, max_retries = 5) {
  req <- httr2::request(url) |>
    httr2::req_method("PUT") |>
    httr2::req_headers(!!!as.list(headers))

  if (!is.null(body)) {
    req <- req |>
      httr2::req_body_raw(body, type = "application/json")
  }

  req <- req |>
    httr2::req_error(is_error = \(resp) FALSE) |>
    httr2::req_retry(
      max_tries = max_retries + 1,
      is_transient = function(resp) httr2::resp_status(resp) == 429,
      after = function(resp) {
        retry_after <- suppressWarnings(
          as.numeric(httr2::resp_header(resp, "retry-after"))
        )
        if (length(retry_after) == 0 || is.na(retry_after) || retry_after <= 0) {
          retry_after <- 10
        }
        message("Rate limit erreicht (429). Warte ", retry_after, "s...")
        retry_after
      }
    )

  httr2::req_perform(req)
}

#' CRM DELETE Request with httr2 and Automatic 429 Retry
#'
#' httr2-basierter DELETE-Wrapper mit automatischem Retry bei Rate-Limiting.
#'
#' @param url API-Endpunkt URL
#' @param headers Named vector mit Request-Headers (z.B. X-apikey)
#' @param max_retries Maximale Retry-Versuche bei 429 (default: 5)
#'
#' @return httr2 response Objekt
#' @export
crm_DELETE2 <- function(url, headers, max_retries = 5) {
  req <- httr2::request(url) |>
    httr2::req_method("DELETE") |>
    httr2::req_headers(!!!as.list(headers))

  req <- req |>
    httr2::req_error(is_error = \(resp) FALSE) |>
    httr2::req_retry(
      max_tries = max_retries + 1,
      is_transient = function(resp) httr2::resp_status(resp) == 429,
      after = function(resp) {
        retry_after <- suppressWarnings(
          as.numeric(httr2::resp_header(resp, "retry-after"))
        )
        if (length(retry_after) == 0 || is.na(retry_after) || retry_after <= 0) {
          retry_after <- 10
        }
        message("Rate limit erreicht (429). Warte ", retry_after, "s...")
        retry_after
      }
    )

  httr2::req_perform(req)
}
