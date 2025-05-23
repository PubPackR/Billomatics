% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/asana_api.R
\name{move_asana_task_to_section}
\alias{move_asana_task_to_section}
\title{move_asana_task_to_section}
\usage{
move_asana_task_to_section(task_id, section_id, api_token, logger)
}
\arguments{
\item{task_id}{The GID of the task to move}

\item{section_id}{The GID of the target section}

\item{api_token}{Your Asana API token}

\item{logger}{The log4r logger object}
}
\value{
TRUE if the move was successful, FALSE otherwise
}
\description{
Moves a task to a specific section in Asana with logging
}
