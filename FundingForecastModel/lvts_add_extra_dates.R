##clear system memory and workspace
rm(list=ls());
gc();

##import require libraries 
require(lubridate);# handles that date elements of the data
require(dplyr);# contains method and function calls to be used to filter and select subsets of the data
require(data.table);#used to store and manipiulate data in the form of a data table similar to a mySQL database table
require(ggplot2);#used to plot data
require(magrittr);#contains method/function calls to use for chaining e.g. %>%
require(scales);#component of ggplot2 that requires to be manually loaded to handle the scaling of various axies
require(foreach);#handles computations for each element of a vector/data frame/array/etc required the use of %do% or %dopar% operators in base R
require(iterators);#handles iteration through for each element of a vector/data frame/array/etc



load("C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/DataFiles/FullLVTSTransDataSeries.Rdata");#mergedLVTSTrans

gc():

mergedLVTSTrans <- mergedLVTSTrans[,Year_Month:=ymd(paste(year(mergedLVTSTrans$date.time),month(mergedLVTSTrans$date.time),1)
                                                    ,tz="America/New_York")];

gc();

mergedLVTSTrans <- mergedLVTSTrans[,Year_Month_Day:=ymd(paste(year(mergedLVTSTrans$date.time),month(mergedLVTSTrans$date.time),mday(mergedLVTSTrans$date.time))
                                                        ,tz="America/New_York")];

gc();