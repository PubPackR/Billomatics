% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/asana_api.R
\name{get_asana_tasks_in_section}
\alias{get_asana_tasks_in_section}
\title{get_asana_tasks_in_section}
\usage{
get_asana_tasks_in_section(section_gid, api_token, logger, limit = 100)
}
\arguments{
\item{section_gid}{The GID of the section}

\item{api_token}{The Asana API token}

\item{logger}{The logger object}

\item{limit}{The maximum number of tasks to retrieve (default: 100)}
}
\value{
A data frame with task details
}
\description{
Gets all tasks in a specific section of a project
}
