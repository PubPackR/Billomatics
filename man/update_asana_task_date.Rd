% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/asana_api.R
\name{update_asana_task_date}
\alias{update_asana_task_date}
\title{update_asana_task_date}
\usage{
update_asana_task_date(task_id, custom_field_gid, date, api_token, logger)
}
\arguments{
\item{task_id}{The GID of the task}

\item{custom_field_gid}{The GID of the custom field}

\item{date}{The new date value}

\item{api_token}{The Asana API token}

\item{logger}{The logger object}
}
\value{
TRUE if the custom field was updated successfully, FALSE otherwise
}
\description{
Updates the date custom fields of an Asana task
}
