################################################################################-
# ----- Description -------------------------------------------------------------
#
# Methods for the Asana API
#
################################################################################-
# ----- Settings ----------------------------------------------------------------

## ----- libraries -----
library(jsonlite)
library(httr2)
library(dplyr)
library(log4r)

################################################################################-
# ----- Get information -----

#' get_asana_task_details
#'
#' Retrieves details of a task from Asana
#' @param task_id The Asana task ID
#' @param api_token The Asana API token
#' @param logger The logger object
#' @return A list containing task details or NULL
#' @export
get_asana_task_details <- function(task_id, api_token, logger) {

  url <- paste0("https://app.asana.com/api/1.0/tasks/", task_id)

  # Attempt the request with tryCatch to handle any network errors
  resp <- tryCatch(
    request(url) |>
      req_auth_bearer_token(token = api_token) |>
      req_perform(),
    error = function(e) {
      error(logger, paste("❌ Network error while retrieving task details:", e$message))
      return(NULL)
    }
  )

  # If we got NULL here, a network error occurred
  if (is.null(resp)) {
    error(logger, paste("🚫 No response received from Asana API for task ID:", task_id))
    return(NULL)
  }

  # Extract the status code
  status_code <- resp_status(resp)

  # Handle API response
  if (status_code == 200) {
    # Parse JSON and return
    return(fromJSON(resp_body_string(resp)))

  } else {
    error_message <- switch(
      as.character(status_code),
      "400" = "❌ Bad Request: The task ID may be invalid.",
      "401" = "❌ Unauthorized: Invalid or missing API token.",
      "403" = "❌ Forbidden: Insufficient permissions to access the task.",
      "404" = "❌ Not Found: Task does not exist or is inaccessible.",
      paste0("❌ Unexpected error (HTTP ", status_code, ").")
    )
    error(logger, paste(error_message, "| Response body:", resp_body_string(resp)))
    return(NULL)
  }
}

################################################################################-
# ----- Write comment -----

#' post_asana_comment
#'
#' Posts a comment to an Asana task
#'
#' @param task_gid The unique identifier (GID) of the Asana task
#' @param comment_text The text of the comment to be posted
#' @param api_token The Asana personal access token for authentication
#' @param logger A log4r logger object for logging messages
#' @return TRUE if the comment was successfully posted, FALSE otherwise
#' @export
post_asana_comment <- function(task_gid, comment_text, api_token, logger) {
  # Validate inputs
  if (missing(task_gid) || missing(comment_text) || missing(api_token)) {
    log4r::error(logger, "Missing required arguments for post_asana_comment function.")
    return(FALSE)
  }

  # Asana API endpoint
  url <- paste0("https://app.asana.com/api/1.0/tasks/", task_gid, "/stories")

  # Create the request body
  body <- list(data = list(text = comment_text))

  tryCatch({
    # Make the API request using httr2
    response <- request(url) %>%
      req_method("POST") %>%
      req_headers(
        "Authorization" = paste("Bearer", api_token),
        "Content-Type" = "application/json"
      ) %>%
      req_body_json(body) %>%
      req_perform()

    # Check response status
    status_code <- resp_status(response)

    if (status_code == 201) {
      log4r::info(logger, paste("Comment successfully posted on task:", task_gid))
      return(TRUE)
    } else {
      error_msg <- paste("Error:", status_code, resp_body_string(response))
      log4r::error(logger, error_msg)
      return(FALSE)
    }

  }, error = function(e) {
    log4r::error(logger, paste("Failed to post comment:", e$message))
    return(FALSE)
  })
}




################################################################################-
# ----- Sections -----


#' get_asana_tasks_in_section
#'
#' Gets all tasks in a specific section of a project
#'
#' @param section_gid The GID of the section
#' @param api_token The Asana API token
#' @param logger The logger object
#' @param limit The maximum number of tasks to retrieve (default: 100)
#' @return A data frame with task details
#' @export
get_asana_tasks_in_section <- function(section_gid, api_token, logger, limit = 100) {

  base_url <- "https://app.asana.com/api/1.0/sections/"
  url <- paste0(base_url, section_gid, "/tasks?limit=", limit)

  all_tasks <- list()
  has_more <- TRUE

  while (has_more) {
    resp <- tryCatch({
      request(url) |>
        req_auth_bearer_token(token = api_token) |>
        req_method("GET") |>
        req_error(is_error = function(resp) FALSE) |>
        req_perform()
    }, error = function(e) {
      error(logger, paste("❌ Network error while fetching tasks:", e$message))
      return(NULL)
    })

    # Handle null response (network failure)
    if (is.null(resp)) {
      error(logger, "🚫 Request failed. No response received from Asana API.")
      return(NULL)
    }

    # Check HTTP response status
    status_code <- resp_status(resp)
    if (status_code != 200) {
      error(logger, paste("❌ API Error (HTTP", status_code, "):", resp_body_string(resp)))
      return(NULL)
    }

    # Parse response
    resp_content <- resp_body_json(resp)
    tasks <- resp_content$data

    all_tasks <- c(all_tasks, tasks)

    # Pagination: Check for next page
    next_page <- resp_content$next_page
    if (!is.null(next_page) && !is.null(next_page$offset)) {
      url <- paste0(base_url, section_gid, "/tasks?limit=", limit, "&offset=", next_page$offset)
    } else {
      has_more <- FALSE
    }
  }

  # Convert to data frame
  tasks_df <- bind_rows(lapply(all_tasks, as.data.frame))
  return(tasks_df)
}


#' get_asana_project_sections
#'
#' Retrieves all sections and their GIDs for a given Asana project
#'
#' @param project_id The GID of the Asana project.
#' @param api_token Personal Access Token for Asana API authentication.
#' @param logger Logger object (optional).
#' @return A data frame with section names and their GIDs.
#' @export
get_asana_project_sections <- function(project_id, api_token, logger = NULL) {
  # Construct the API URL
  url <- paste0("https://app.asana.com/api/1.0/projects/", project_id, "/sections")

  # Prepare headers
  headers <- httr::add_headers(
    Authorization = paste("Bearer", api_token),
    `Content-Type` = "application/json"
  )

  # Make the GET request
  response <- httr::GET(url, headers)

  # Check response status
  if (httr::status_code(response) != 200) {
    error_message <- paste(
      "❌ Failed to retrieve sections for project:", project_id,
      "- Status code:", httr::status_code(response),
      "- Response:", httr::content(response, "text", encoding = "UTF-8")
    )
    if (!is.null(logger)) error(logger, error_message)
    stop(error_message)
  }

  # Parse the response
  sections_data <- httr::content(response, as = "parsed", encoding = "UTF-8")$data

  # Return NULL if no sections found
  if (length(sections_data) == 0) {
    if (!is.null(logger)) warn(logger, paste("⚠️ No sections found for project:", project_id))
    return(NULL)
  }

  # Convert the sections into a data frame
  sections_df <- data.frame(
    section_name = sapply(sections_data, function(x) x$name),
    section_gid = sapply(sections_data, function(x) x$gid),
    stringsAsFactors = FALSE
  )

  if (!is.null(logger)) info(logger, paste("✅ Retrieved", nrow(sections_df), "sections for project:", project_id))

  return(sections_df)
}



################################################################################-
# ----- Task Templates -----


#' get_asana_task_templates
#'
#' Gets all tasks templates of a project
#'
#' @param project_id The GID of the project
#' @param logger The logger object
#' @return task templates of the project
#' @export
get_asana_task_templates <- function(project_id, asana_token, logger) {
  url <- "https://app.asana.com/api/1.0/task_templates"

  queryString <- list(project = project_id)

  # Perform the GET request with error handling
  response <- tryCatch(
    {
      GET(
        url,
        query = queryString,
        add_headers('Authorization' = paste('Bearer', asana_token)),
        content_type("application/json"),
        accept("application/json")
      )
    },
    error = function(e) {
      error(logger, paste("❌ Network error while fetching task templates:", e$message))
      return(NULL)
    }
  )

  # Handle response if request fails
  if (is.null(response)) {
    warn(logger, paste("⚠️ No response received for project_id:", project_id))
    return(NULL)
  }

  # Handle response status codes
  status_code <- status_code(response)
  response_body <- content(response, "text")

  if (status_code == 200) {
    templates <- fromJSON(response_body)$data
    return(templates)
  } else {
    error(logger, paste("❌ Failed to fetch task templates. HTTP", status_code, "- Response:", response_body))
    return(NULL)
  }
}


#' get_asana_task_template_details
#'
#' Get the details of a task template in Asana
#'
#' @param task_template_gid The GID of the task template
#' @param asana_token The Asana API token
#' @param logger The logger object
#' @return A list containing the task template details or NULL
#' @export
get_asana_task_template_details <- function(task_template_gid, asana_token, logger) {
  url <- paste0("https://app.asana.com/api/1.0/task_templates/", task_template_gid)

  # Perform the GET request
  response <- tryCatch(
    {
      request(url) |>
        req_auth_bearer_token(asana_token) |>
        req_method("GET") |>
        req_error(is_error = function(resp) FALSE) |>
        req_perform()
    },
    error = function(e) {
      error(logger, paste("❌ Network error while fetching template:", e$message))
      return(NULL)
    }
  )

  # Return NULL if request failed
  if (is.null(response)) {
    warn(logger, paste("⚠️ No response received for template_gid:", task_template_gid))
    return(NULL)
  }

  # Handle response status codes
  status_code <- resp_status(response)
  response_body <- resp_body_string(response)

  if (status_code == 200) {
    template_details <- resp_body_json(response)$data
    return(template_details)
  } else {
    error(logger, paste("❌ Failed to fetch template details. HTTP", status_code, "- Response:", response_body))
    return(NULL)
  }
}


#' get_asana_task_template_details_with_nested_subtasks
#'
#' Get the details of a task template in Asana, including deeper levels of subtasks
#'
#' @param task_template_gid The GID of the task template
#' @param asana_token The Asana API token
#' @param logger The logger object
#' @return A list containing the task template details or NULL
#' @export
get_asana_task_template_details_with_nested_subtasks <- function(task_template_gid, asana_token, logger) {

  # Specify the fields you want, e.g., name, plus multiple subtask levels
  fields_needed <- c(
    "template.name",
    "template.subtasks.name",
    "template.subtasks.subtasks.name",
    "template.subtasks.subtasks.subtasks.name"
  )
  fields_param <- paste(fields_needed, collapse = ",")

  # Build the URL with opt_fields
  url <- paste0(
    "https://app.asana.com/api/1.0/task_templates/",
    task_template_gid,
    "?opt_fields=",
    fields_param
  )

  # Perform the GET request
  response <- tryCatch(
    {
      request(url) |>
        req_auth_bearer_token(asana_token) |>
        req_method("GET") |>
        req_error(is_error = function(resp) FALSE) |>
        req_perform()
    },
    error = function(e) {
      error(logger, paste("❌ Network error while fetching template:", e$message))
      return(NULL)
    }
  )

  # Return NULL if request failed
  if (is.null(response)) {
    warn(logger, paste("⚠️ No response received for template_gid:", task_template_gid))
    return(NULL)
  }

  # Handle response status codes
  status_code <- resp_status(response)
  response_body <- resp_body_string(response)

  if (status_code == 200) {
    template_details <- resp_body_json(response)$data
    return(template_details)
  } else {
    error(logger, paste("❌ Failed to fetch template details. HTTP", status_code, "- Response:", response_body))
    return(NULL)
  }
}


################################################################################-
# ----- Subtasks -----

#' get_asana_subtasks
#'
#' Retrieves the subtasks of a given Asana task (httr2 version)
#'
#' @param task_id The Asana task ID
#' @param api_token The Asana API token
#' @param logger The logger object
#' @return A list containing subtask details or NULL if an error occurs
#' @export
get_asana_subtasks <- function(task_id, api_token, logger) {

  url <- paste0("https://app.asana.com/api/1.0/tasks/", task_id, "/subtasks")

  # Attempt the request with tryCatch to handle any network errors
  resp <- tryCatch(
    request(url) |>
      req_auth_bearer_token(token = api_token) |>
      req_perform(),
    error = function(e) {
      error(logger, paste("❌ Network error while retrieving subtasks:", e$message))
      return(NULL)
    }
  )

  # If no response, log and return NULL
  if (is.null(resp)) {
    error(logger, paste("🚫 No response received from Asana API for task ID:", task_id))
    return(NULL)
  }

  # Extract the status code
  status_code <- resp_status(resp)

  # Handle API response
  if (status_code == 200) {
    # Parse JSON response and extract subtasks
    response_data <- fromJSON(resp_body_string(resp))
    if (length(response_data$data) == 0) {
      info(logger, paste("ℹ️ No subtasks found for task ID:", task_id))
      return(NULL)
    }
    return(response_data$data)

  } else {
    error_message <- switch(
      as.character(status_code),
      "400" = "❌ Bad Request: The task ID may be invalid.",
      "401" = "❌ Unauthorized: Invalid or missing API token.",
      "403" = "❌ Forbidden: Insufficient permissions to access the task's subtasks.",
      "404" = "❌ Not Found: Task does not exist or is inaccessible.",
      paste0("❌ Unexpected error (HTTP ", status_code, ").")
    )
    error(logger, paste(error_message, "| Response body:", resp_body_string(resp)))
    return(NULL)
  }
}


#' create_asana_subtask
#'
#' Creates a subtask in an Asana task and returns its GID
#'
#' @param parent_task_gid The GID of the parent task where the subtask will be created
#' @param subtask_name The name of the subtask
#' @param api_token Your Asana API token
#' @param logger The logger object
#' @param assignee (Optional) The GID of the assignee (default: NULL)
#' @param due_on (Optional) The due date in "YYYY-MM-DD" format (default: NULL)
#' @param notes (Optional) Notes or description for the subtask (default: NULL)
#' @return The GID of the created subtask or NULL if the creation failed
#' @export
create_asana_subtask <- function(parent_task_gid, subtask_name, api_token, logger, assignee = NULL, due_on = NULL, notes = NULL) {

  # Input validation
  if (is.null(parent_task_gid) || is.null(subtask_name)) {
    error(logger, "❌ Error: 'parent_task_gid' and 'subtask_name' are required.")
    return(NULL)
  }

  url <- paste0("https://app.asana.com/api/1.0/tasks/", parent_task_gid, "/subtasks")

  body <- list(
    data = list(
      name = subtask_name,
      assignee = assignee,
      due_on = due_on,
      notes = notes
    )
  )

  # Clean NULL fields
  body$data <- Filter(Negate(is.null), body$data)

  # Make the API request
  resp <- tryCatch(
    request(url) |>
      req_auth_bearer_token(api_token) |>
      req_body_json(body) |>
      req_perform(),
    error = function(e) {
      error(logger, paste("❌ Network error while creating subtask:", e$message))
      return(NULL)
    }
  )

  # Handle null response
  if (is.null(resp)) {
    error(logger, "🚫 Request failed. No response received from Asana API.")
    return(NULL)
  }

  # Extract and handle response status
  status_code <- resp_status(resp)
  response_body <- resp_body_string(resp)

  if (status_code == 201) {
    created_subtask <- fromJSON(response_body)$data
    return(created_subtask$gid)
  } else {
    error_message <- switch(
      as.character(status_code),
      "400" = "❌ Bad Request: Invalid input data.",
      "401" = "❌ Unauthorized: Invalid or missing API token.",
      "403" = "❌ Forbidden: Insufficient permissions to create subtask.",
      "404" = "❌ Not Found: Parent task ID may be incorrect.",
      paste0("❌ Unexpected error (HTTP ", status_code, ").")
    )
    error(logger, paste(error_message, "| Response body:", response_body))
    return(NULL)
  }
}


################################################################################-
# ----- Tags -----

#' check_asana_tag_exists_in_workspace
#'
#' Checks if a tag exists in a workspace
#'
#' @param tag_gid The GID of the tag to check
#' @param api_token The Asana API token
#' @param logger The logger object
#' @return TRUE if the tag exists, FALSE if not, or NA for an error
#' @export
check_asana_tag_exists_in_workspace <- function(tag_gid, api_token, logger) {

  url <- paste0("https://app.asana.com/api/1.0/tags/", tag_gid)

  resp <- request(url) |>
    req_auth_bearer_token(token = api_token) |>
    req_method("GET") |>
    req_error(is_error = function(resp) FALSE) |>
    req_perform()

  status_code <- resp_status(resp)

  if (status_code == 200) {
    return(TRUE)
  } else if (status_code %in% c(403, 404)) {
    # Treat "forbidden" and "not found" as "does not exist"
    info(logger, paste("Tag", tag_gid, "is inaccessible or does not exist in the workspace (HTTP", status_code, ")."))
    return(FALSE)
  } else {
    # Log other status codes as unexpected
    error(logger, paste("⚠️ Unexpected error (HTTP", status_code, "):", resp_body_string(resp)))
    return(NA)
  }
}



#' add_asana_tag_to_task
#'
#' Adds a tag to an Asana task
#'
#' @param task_gid The GID of the task
#' @param tag_gid The GID of the tag to add
#' @param api_token The Asana API token
#' @param logger The logger object
#' @return TRUE if the tag was added successfully, FALSE otherwise
#' @export
add_asana_tag_to_task <- function(task_gid, tag_gid, api_token, logger) {
  # API endpoint for adding a tag to a task
  url <- paste0("https://app.asana.com/api/1.0/tasks/", task_gid, "/addTag")

  # Request body
  body <- list(
    data = list(
      tag = tag_gid
    )
  )

  # Perform the POST request
  resp <- request(url) |>
    req_auth_bearer_token(token = api_token) |>
    req_method("POST") |>
    req_body_json(body, auto_unbox = TRUE) |>
    req_error(is_error = function(resp) FALSE) |>
    req_perform()

  # Handle response
  status_code <- resp_status(resp)
  response_body <- resp_body_string(resp)

  if (status_code == 200) {
    info(logger, paste("✅ Successfully added tag", tag_gid, "to task", task_gid))
    return(TRUE)
  } else {
    error(logger, paste("❌ Error (HTTP", status_code, ") while adding tag", tag_gid, "to task", task_gid, ":", response_body))
    return(FALSE)
  }
}

#' create_asana_tag
#'
#' Creates a new tag in Asana and returns the tag ID
#'
#' @param workspace_id The ID of the workspace where the tag will be created
#' @param tag_name The name of the new tag
#' @param asana_token Personal Access Token (PAT) for Asana API authentication
#' @param logger The logger object
#' @return The ID (GID) of the newly created tag or NULL if creation fails
#' @export
create_asana_tag <- function(workspace_id, tag_name, asana_token, logger) {

  url <- "https://app.asana.com/api/1.0/tags"

  # Build and send the request
  response <- request(url) |>
    req_auth_bearer_token(asana_token) |>
    req_method("POST") |>
    req_headers(`Content-Type` = "application/json") |>
    req_body_json(list(
      data = list(
        name = tag_name,
        workspace = workspace_id
      )
    ), auto_unbox = TRUE) |>
    req_error(is_error = function(resp) FALSE) |>
    req_perform()

  # Check the status code
  status_code <- resp_status(response)
  response_body <- resp_body_string(response)

  if (status_code == 201) {
    content <- resp_body_json(response)

    # Correct extraction of the tag ID (GID)
    tag_id <- tryCatch(content$data$gid, error = function(e) {
      error(logger, paste("⚠️ Tag created but unable to extract tag ID. Error:", e$message))
      return(NULL)
    })

    if (!is.null(tag_id)) {
      info(logger, paste("✅ Successfully created tag '{tag_name}' in workspace {workspace_id} with ID {tag_id}."))
      return(tag_id)
    } else {
      warn(logger, paste("⚠️ Tag '{tag_name}' created but unable to extract tag ID."))
      return(NULL)
    }
  } else {
    error(logger, paste("❌ Failed to create tag '{tag_name}' in workspace {workspace_id}. Status code:", status_code))
    error(logger, paste("Response body:", response_body))
    return(NULL)
  }
}

#' get_all_asana_tags
#'
#' Gets all tags from the Asana board
#'
#' @param workspace_id Workspace id for the asana board
#' @param asana_key Asana key (bearer) for the asana board
#' @return All tags in the asana board
#' @export
get_all_asana_tags <- function(workspace_id, asana_key) {

  base_url <- "https://app.asana.com/api/1.0/tags/"

  limit <- 100
  tags <- list()
  offset <- NULL

  repeat {
    url <- paste0(base_url, "?limit=", limit, "&workspace=", workspace_id)
    if (!is.null(offset)) {
      url <- paste0(url, "&offset=", offset)
    }

    response <- GET(
      url,
      add_headers(
        "accept" = "application/json",
        "authorization" = paste("Bearer", asana_key)
      )
    )

    content <- content(response, as = "parsed", encoding = "UTF-8")

    if (!is.null(content$data)) {
      tags <- append(tags, content$data)
    }

    if (!is.null(content$next_page$offset)) {
      offset <- content$next_page$offset
    } else {
      break
    }
  }

  return(tags)
}


#' rename_asana_tag
#'
#' Renames an existing Asana tag
#'
#' @param tag_id The GID of the tag to rename
#' @param new_name The new name for the tag
#' @param asana_token Personal Access Token (PAT) for Asana API authentication
#' @param logger The logger object
#' @return TRUE if renaming was successful, FALSE otherwise
#' @export
rename_asana_tag <- function(tag_id, new_name, asana_token, logger) {

  url <- paste0("https://app.asana.com/api/1.0/tags/", tag_id)

  # Build and send the request
  response <- request(url) |>
    req_auth_bearer_token(asana_token) |>
    req_method("PUT") |>
    req_headers(`Content-Type` = "application/json") |>
    req_body_json(list(
      data = list(
        name = new_name
      )
    ), auto_unbox = TRUE) |>
    req_error(is_error = function(resp) FALSE) |>
    req_perform()

  # Check the status code
  status_code <- resp_status(response)
  response_body <- resp_body_string(response)

  if (status_code == 200) {
    info(logger, paste("✅ Successfully renamed tag", tag_id, "to", new_name))
    return(TRUE)
  } else {
    error(logger, paste("❌ Failed to rename tag", tag_id, "to", new_name, ". Status code:", status_code))
    error(logger, paste("Response body:", response_body))
    return(FALSE)
  }
}



#' delete_asana_tag
#'
#' Deletes an Asana tag
#'
#' @param tag_id Tag id to delete
#' @param asana_key Asana key (bearer) for the asana board
#' @return Logical. TRUE on success, FALSE on failure
#' @export
delete_asana_tag <- function(tag_id, asana_key, request_func = DELETE) {

  base_url <- "https://app.asana.com/api/1.0/tags/"

  url <- paste0(base_url, tag_id)

  response <- request_func(
    url,
    add_headers(
      "accept" = "application/json",
      "authorization" = paste("Bearer", asana_key)
    )
  )

  # error handling
  if (response$status_code >= 400) {
    warning("❌ API Error: Failed to delete tag: ", tag_id,
            "\nStatus Code: ", httr::status_code(response),
            "\nResponse: ", httr::content(response, as = "text", encoding = "UTF-8"))
    return(FALSE)
  }

  # Return TRUE on success
  print(paste("✅ Tag deleted: ", tag_id))
  TRUE
}

################################################################################-
# ----- Custom Fields -----


#' get_asana_custom_field_gids
#'
#' Gets the gids of all custom fields for a project
#'
#' @param project_id The GID of the project
#' @param api_token The Asana API token
#' @param logger The logger object
#' @return A list of custom field GIDs
#' @export
get_asana_custom_field_gids <- function(project_id, api_token, logger) {

  url <- paste0("https://app.asana.com/api/1.0/projects/", project_id, "/custom_field_settings")

  resp <- request(url) |>
    req_auth_bearer_token(token = api_token) |>
    req_method("GET") |>
    req_error(is_error = function(resp) FALSE) |>
    req_perform()

  fromJSON(resp_body_string(resp))
}


#' update_asana_task_date
#'
#' Updates the date custom fields of an Asana task
#'
#' @param task_id The GID of the task
#' @param custom_field_gid The GID of the custom field
#' @param date The new date value
#' @param api_token The Asana API token
#' @param logger The logger object
#' @return TRUE if the custom field was updated successfully, FALSE otherwise
#' @export
update_asana_task_date <- function(task_id, custom_field_gid, date, api_token, logger) {

  # Validate the date format
  if (!grepl("^\\d{4}-\\d{2}-\\d{2}$", date)) {
    error(logger, "❌ Invalid date format. Please use 'YYYY-MM-DD'.")
    return(FALSE)
  }

  url <- paste0("https://app.asana.com/api/1.0/tasks/", task_id)

  # Prepare the body: set custom_field_gid directly to the date string
  body <- list(
    data = list(
      custom_fields = setNames(
        list(list(date = as.character(date))),
        custom_field_gid
      )
    )
  )

  # Send the PUT request with error handling
  resp <- tryCatch({
    request(url) |>
      req_auth_bearer_token(token = api_token) |>
      req_method("PUT") |>
      req_body_json(body, auto_unbox = TRUE) |>
      req_error(is_error = function(resp) FALSE) |>
      req_perform()
  }, error = function(e) {
    error(logger, paste("❌ Network error while updating custom field date:", e$message))
    return(NULL)
  })

  # Handle null response (network failure)
  if (is.null(resp)) {
    error(logger, "🚫 Request failed. No response from Asana API.")
    return(FALSE)
  }

  # Extract response details
  status_code <- resp_status(resp)
  response_body <- resp_body_string(resp)

  # Process response
  if (status_code == 200) {
    info(logger, paste("✅ Successfully updated date custom field", custom_field_gid, "for task ID:", task_id, "to", date))
    return(TRUE)
  } else {
    error_message <- switch(
      as.character(status_code),
      "400" = "❌ Bad Request: Invalid data or task ID.",
      "401" = "❌ Unauthorized: Invalid API token.",
      "403" = "❌ Forbidden: Insufficient permissions to update the task.",
      "404" = "❌ Not Found: Task ID or custom field ID may be incorrect.",
      paste0("❌ Unexpected error (HTTP ", status_code, ").")
    )
    error(logger, paste(error_message, "| Response body:", response_body))
    return(FALSE)
  }

}


#' update_asana_custom_field_with_options
#'
#' Updates a custom field with optional checkboxes of an Asana task
#'
#' @param task_id The Asana task ID
#' @param api_token The Asana API token
#' @param custom_field_gid The GID of the custom field
#' @param option_gids A character vector of the GIDs of the options to select
#' @param logger The logger object
#' @return TRUE if the custom field was updated successfully, FALSE otherwise
#' @export
update_asana_custom_field_with_options <- function(task_id, api_token, custom_field_gid, option_gids, logger) {

  url <- paste0("https://app.asana.com/api/1.0/tasks/", task_id)

  # Prepare the body: set custom_field_gid directly to an array of GID strings
  body <- list(
    data = list(
      custom_fields = setNames(
        list(unname(as.character(option_gids))),  # Direct array of strings
        custom_field_gid
      )
    )
  )

  # Send the PUT request with error handling
  resp <- tryCatch({
    request(url) |>
      req_auth_bearer_token(token = api_token) |>
      req_method("PUT") |>
      req_body_json(body, auto_unbox = FALSE) |>
      req_error(is_error = function(resp) FALSE) |>
      req_perform()
  }, error = function(e) {
    error(logger, paste("❌ Network error while updating custom field:", e$message))
    return(NULL)
  })

  # Handle response

  # Handle null response (request failure)
  if (is.null(resp)) {
    error(logger, "🚫 Request failed. No response from Asana API.")
    return(FALSE)
  }

  # Extract status code and response body
  status_code <- resp_status(resp)
  response_body <- resp_body_string(resp)

  # Handle successful and unsuccessful responses
  if (status_code == 200) {
    info(logger, paste("✅ Successfully updated custom field", custom_field_gid, "for task ID:", task_id))
    return(TRUE)
  } else {
    error_message <- switch(
      as.character(status_code),
      "400" = "❌ Bad Request: Invalid data or task ID.",
      "401" = "❌ Unauthorized: Invalid API token.",
      "403" = "❌ Forbidden: Insufficient permissions to update the task.",
      "404" = "❌ Not Found: Task ID or custom field ID may be incorrect.",
      paste0("❌ Unexpected error (HTTP ", status_code, ").")
    )
    error(logger, paste(error_message, "| Response body:", response_body))
    return(FALSE)
  }
}

#' update_asana_custom_field
#'
#' Updates a custom text field in an Asana task
#'
#' @param task_gid The GID of the task to update
#' @param custom_field_gid The GID of the custom field to update
#' @param new_value The new text value to set
#' @param api_token Your Asana API token
#' @param logger The logger object
#' @return TRUE if successful, FALSE if not
#' @export
update_asana_custom_field <- function(task_gid, custom_field_gid, new_value, api_token, logger) {
  # Construct the API URL
  url <- paste0("https://app.asana.com/api/1.0/tasks/", task_gid)

  # Prepare the request body
  body <- list(
    data = list(
      custom_fields = setNames(list(new_value), custom_field_gid)
    )
  )

  # Send the PATCH request to update the custom field
  resp <- tryCatch(
    {
      request(url) |>
        req_auth_bearer_token(token = api_token) |>
        req_method("PUT") |>
        req_body_json(body) |>
        req_perform()
    },
    error = function(e) {
      error(logger, paste("❌ Network error while updating custom field:", e$message))
      return(NULL)
    }
  )

  # Handle the response
  if (is.null(resp)) {
    error(logger, "🚫 Request failed. No response received from Asana API.")
    return(FALSE)
  }

  status_code <- resp_status(resp)
  if (status_code == 200) {
    info(logger, paste("✅ Successfully updated custom field for task ID:", task_gid))
    return(TRUE)
  } else {
    error_message <- switch(
      as.character(status_code),
      "400" = "❌ Bad Request: Invalid task ID or data.",
      "401" = "❌ Unauthorized: Invalid",
      "403" = "❌ Forbidden: Insufficient permissions to update the task.",
      "404" = "❌ Not Found: Task ID may be incorrect or inaccessible.",
      paste0("❌ Unexpected error (HTTP ", status_code, ").")
    )

    return(FALSE)
  }
}


################################################################################-
# ----- Due Date -----

#' set_asana_due_date
#'
#' Sets the due date of an Asana task
#'
#' @param task_gid The GID of the task to update
#' @param due_date The new due date in "YYYY-MM-DD" format
#' @param api_token Your Asana API token
#' @param logger The logger object
#' @return TRUE if successful, FALSE if not
#' @export
set_asana_due_date <- function(task_gid, due_date, api_token, logger) {

  # Validate the due date format
  if (!grepl("^\\d{4}-\\d{2}-\\d{2}$", due_date)) {
    error(logger, "❌ Invalid due date format. Use 'YYYY-MM-DD'.")
    return(FALSE)
  }

  # Construct the API URL
  url <- paste0("https://app.asana.com/api/1.0/tasks/", task_gid)

  # Prepare the request body
  body <- list(
    data = list(
      due_on = due_date
    )
  )

  # Send the PUT request to update the due date
  resp <- tryCatch(
    {
      request(url) |>
        req_auth_bearer_token(token = api_token) |>
        req_method("PUT") |>
        req_body_json(body) |>
        req_perform()
    },
    error = function(e) {
      error(logger, paste("❌ Network error while updating due date:", e$message))
      return(NULL)
    }
  )

  # Handle the response
  if (is.null(resp)) {
    error(logger, "🚫 Request failed. No response received from Asana API.")
    return(FALSE)
  }

  status_code <- resp_status(resp)
  response_body <- resp_body_string(resp)

  if (status_code == 200) {
    info(logger, paste("✅ Successfully updated due date for task ID:", task_gid, "to", due_date))
    return(TRUE)
  } else {
    error_message <- switch(
      as.character(status_code),
      "400" = "❌ Bad Request: Invalid task ID or date.",
      "401" = "❌ Unauthorized: Invalid API token.",
      "403" = "❌ Forbidden: Insufficient permissions to update the task.",
      "404" = "❌ Not Found: Task ID may be incorrect or inaccessible.",
      paste0("❌ Unexpected error (HTTP ", status_code, ").")
    )
    error(logger, paste(error_message, "| Response body:", response_body))
    return(FALSE)
  }
}



################################################################################-
# ----- Asana Assignee -----

#' get_asana_task_assignee_from_url
#'
#' Gets the assignee of an Asana task
#'
#' @param api_token The Asana API token
#' @param task_url The URL of the Asana task
#' @param logger The logger object
#' @return The assignee information or NULL if unassigned
#' @export
get_asana_task_assignee_from_url <- function(api_token, task_url, logger) {
  # 1. Parse the URL and get the Task GID.
  #    Asana URLs are typically ".../0/PROJECT_GID/TASK_GID".
  splitted_url <- strsplit(task_url, "/")[[1]]
  task_gid <- splitted_url[length(splitted_url)]

  # 2. Construct the Asana Task endpoint
  endpoint <- paste0("https://app.asana.com/api/1.0/tasks/", task_gid)

  # 3. Make the request
  res <- tryCatch({
    GET(
      url = endpoint,
      add_headers(Authorization = paste("Bearer", api_token))
    )
  }, error = function(e) {
    error(logger, paste("❌ Failed to send request. Error:", e$message))
    return(NULL)
  })

  # Exit if request failed
  if (is.null(res)) {
    return(NULL)
  }

  # 4. Check for HTTP error codes
  if (http_error(res)) {
    error(logger, paste("❌ HTTP error occurred:", status_code(res), content(res, as = "text")))
    return(NULL)
  }

  # 5. Parse the JSON response
  parsed <- tryCatch({
    content(res, as = "parsed")
  }, error = function(e) {
    error(logger, paste("❌ Failed to parse response. Error:", e$message))
    return(NULL)
  })

  # Exit if parsing failed
  if (is.null(parsed)) {
    return(NULL)
  }

  # 6. Extract and return the 'assignee' data (NULL if unassigned)
  assignee_info <- parsed$data$assignee
  return(assignee_info)
}


#' get_asana_task_assignee
#'
#' Gets the assignee of an Asana task, if it exists, NULL otherwise
#'
#' @param task_id Character. The GID of the Asana task.
#' @param api_token Character. Your Asana API token.
#' @param logger The logger object.
#' @return Assignee details if the task has an assignee, NULL otherwise.
#' @export
get_asana_task_assignee <- function(task_id, api_token, logger) {

  # Construct the API endpoint URL
  url <- paste0("https://app.asana.com/api/1.0/tasks/", task_id)

  # Send the GET request with error handling
  resp <- tryCatch({
    request(url) |>
      req_auth_bearer_token(token = api_token) |>
      req_method("GET") |>
      req_error(is_error = function(resp) FALSE) |>  # Prevent auto error throwing
      req_perform()
  }, error = function(e) {
    error(logger, paste("⚠️ Network error while checking task:", e$message))
    return(NULL)
  })

  # Handle request-level errors
  if (is.null(resp)) {
    error(logger, "🚫 Failed to connect to Asana API. Check your network or API token.")
    return(FALSE)
  }

  # Extract response details
  status_code <- resp_status(resp)
  response_body <- resp_body_string(resp)

  # Handle unsuccessful responses
  if (status_code != 200) {
    error_message <- switch(
      as.character(status_code),
      "400" = "❌ Bad Request: Check the task ID.",
      "401" = "❌ Unauthorized: Invalid or missing API token.",
      "403" = "❌ Forbidden: Insufficient permissions to access the task.",
      "404" = "❌ Not Found: Task ID may be incorrect or inaccessible.",
      paste0("❌ Unexpected error (HTTP ", status_code, ").")
    )
    error(logger, paste("HTTP Error:", error_message, "| Response body:", response_body))
    return(FALSE)
  }

  # Parse the JSON response safely
  parsed_response <- tryCatch({
    fromJSON(response_body)
  }, error = function(e) {
    error(logger, paste("❌ Failed to parse API response. Error:", e$message))
    return(NULL)
  })

  if (is.null(parsed_response)) {
    return(FALSE)
  }

  # Check and return the 'assignee' field
  assignee <- parsed_response$data$assignee
  if (!is.null(assignee)) {
    return(assignee)  # Return the assignee details (a list with id, name, etc.)
  } else {
    return(NULL)  # No assignee found
  }
}


#' update_asana_task_assignee
#'
#' Updates the assignee of an Asana task
#'
#' @param task_id The Asana task ID
#' @param assignee_gid The GID of the new assignee (user)
#' @param api_token The Asana API token
#' @param workspace_id The ID of the workspace where the task resides
#' @param logger The logger object
#' @return TRUE if the assignee was updated successfully, FALSE otherwise
#' @export
update_asana_task_assignee <- function(task_id, assignee_gid, workspace_id, api_token, logger) {

  url <- paste0("https://app.asana.com/api/1.0/tasks/", task_id)

  # Build and send the PUT request with error handling
  resp <- tryCatch({
    request(url) |>
      req_auth_bearer_token(token = api_token) |>
      req_method("PUT") |>
      req_body_json(list(
        data = list(
          assignee = assignee_gid,
          workspace = workspace_id
        )
      )) |>
      req_error(is_error = function(resp) FALSE) |>  # Prevent automatic error throwing
      req_perform()
  }, error = function(e) {
    error(logger, paste("❌ Network error while updating assignee:", e$message))
    return(NULL)
  })

  # Exit if request failed
  if (is.null(resp)) {
    error(logger, "🚫 Failed to send request to Asana API.")
    return(FALSE)
  }

  # Extract response details
  status_code <- resp_status(resp)
  response_body <- resp_body_string(resp)

  # Handle the response
  if (status_code == 200) {
    info(logger, paste("✅ Successfully updated assignee for task ID:", task_id, "to assignee GID:", assignee_gid))
    return(TRUE)
  } else {
    error_message <- switch(
      as.character(status_code),
      "400" = "❌ Bad Request: Invalid data or task ID.",
      "401" = "❌ Unauthorized: Invalid or missing API token.",
      "403" = "❌ Forbidden: Insufficient permissions to update the task.",
      "404" = "❌ Not Found: Task ID may be incorrect or inaccessible.",
      paste0("❌ Unexpected error (HTTP ", status_code, ").")
    )
    error(logger, paste(error_message, "| Response body:", response_body))
    return(FALSE)
  }
}

################################################################################-
# ----- Move Task to Other Section -----

#' move_asana_task_to_section
#'
#' Moves a task to a specific section in Asana with logging
#'
#' @param task_id The GID of the task to move
#' @param section_id The GID of the target section
#' @param api_token Your Asana API token
#' @param logger The log4r logger object
#' @return TRUE if the move was successful, FALSE otherwise
#' @export
move_asana_task_to_section <- function(task_id, section_id, api_token, logger) {

  # Correct endpoint
  url <- paste0("https://app.asana.com/api/1.0/sections/", section_id, "/addTask")

  # Request body with 'data' wrapper
  body <- list(
    data = list(
      task = as.character(task_id)
    )
  )

  # Perform the API request with error handling
  resp <- tryCatch({
    request(url) |>
      req_auth_bearer_token(api_token) |>
      req_method("POST") |>
      req_body_json(body, auto_unbox = TRUE) |>
      req_perform()
  }, error = function(e) {
    error(logger, paste("❌ Network error while moving task:", e$message))
    return(NULL)
  })

  # Handle null response (network failure)
  if (is.null(resp)) {
    error(logger, paste("🚫 No response received from Asana API when moving task:", task_id))
    return(FALSE)
  }

  # Check response status
  status_code <- resp_status(resp)
  response_body <- resp_body_string(resp)

  if (status_code == 200) {
    info(logger, paste("✅ Successfully moved task", task_id, "to section", section_id))
    return(TRUE)
  } else {
    error(logger, paste(
      "❌ Failed to move task", task_id, "to section", section_id,
      "- Status code:", status_code, "- Response:", response_body
    ))
    return(FALSE)
  }
}

