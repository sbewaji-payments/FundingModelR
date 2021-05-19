gc();
rm(list=ls());
gc();

##import require libraries 
require(lubridate);# handles that date elements of the data
require(dplyr);# contains method and function calls to be used to filter and select subsets of the data
require(data.table);#used to store and manipiulate data in the form of a data table similar to a mySQL database table
require(ggplot2);#used to plot data
require(magrittr);#contains method/function calls to use for chaining e.g. %>%
require(scales);#component of ggplot2 that requires to be manually loaded to handle the scaling of various axies
require(moments);#3rd-party library used to compute statistics such as skewness and kurtosis which are not provided in the base R package set
require(rlist);#3-party package to allow more flexible and java-collections like handling of lists in R. Allows you to save a list as a Rdata file
require(foreign);#to export data into formats readable by STATA and other statistical packages
require(xts);#used to manipulate date-time data into quarters
require(xlsx);#to export data into formats readable by excel
require(lattice);#to export data into formats readable by excel
require(latticeExtra);#to export data into formats readable by excel
require(forecast);#to export data into formats readable by excel
require(tseries);
require(timeSeries);


options(scipen=999)



startYr = 2000;
endYr = 2019;

##ACSS
extractedACSSDataLoadPath <- c(paste("C:/Projects/FundingDataTables/Cleaned Transactions Data/acssusbeTXNsSentRecievedByFIQuarterlySeries",
                                     startYr,"-",endYr,".RData", sep = "", collapse = NULL));
load(extractedACSSDataLoadPath)

setDT(ACSSUSBETXNsDataSeriesSentRecieved)[, TotalValueShare := TotalValue / sum(TotalValue), by=Year_Quarter];
setDT(ACSSUSBETXNsDataSeriesSentRecieved)[, TotalVolumeShare := TotalVolume / sum(TotalVolume), by=Year_Quarter];

write.csv(ACSSUSBETXNsDataSeriesSentRecieved, file="ACSSUSBETXNsDataSeriesSentRecieved.csv");