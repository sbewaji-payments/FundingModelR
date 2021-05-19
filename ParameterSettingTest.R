require(data.table);
require(plyr);
require(dplyr);
require(xts);#used to manipulate date-time data into quarters
require(lubridate);# handles that date elements of the data


seedNumber <- 777;
startYr <- 2014;
endYr <- 2014;
intervals <- "5 mins";
startingTimeStamp <- c(paste(startYr,"-07-27 22:00:00", sep = "", collapse = NULL));
sourcePath <- c("~/Documents/Development Workspaces/R-Workspace/IntervalSettingTest.R");

source(paste(sourcePath), echo=TRUE);
