##This script acts to load and convert manually created ACSSUSBE transactions files. These files are created from payment ops MRG and QRG files
## 
##The Script will import the saved csv file and convert it to a .RData file
##This will involve, converting the date charaters to actual dates in R and outputing the imported data as a data.table
##
##
##Final input data tables that the code will work with are 
##        a: acssusbeTransSentRecievedByFIXLSQuarterlySeries2000-15.csv - The manually created ACSSUSBE data file
##
##
##Final output data tables that the code will work with are 
##        a: ACSSUSBETransDataSeriesSentRecieved - The ACSSUSBE data file
##        b: acssusbeTransSentRecievedByFIQuarterlySeries2000-15.Rdata - The ACSSUSBE data file
##
##
##
## Author : Segun Bewaji
## Creation Date : 22 Jul 2016
## Modified : Segun Bewaji
## Modifications Made: 22 Jul 2016
##        1) 
##        2) 
##        3)  
##           
##        4)  
##           
##        5) 
##        6)  
## Modified : Segun Bewaji
## Modifications Made:
##        7) 
##        8) 
##           
##        9)  
##           
##       10) 
##
##
##
##
##
##
## 
## $Id$

##the usual
gc();
rm(list=ls());
gc();

##import required libraries
require(plyr);# contains method and function calls to be used to filter and select subsets of the data
require(lubridate);# handles that date elements of the data
require(dplyr);# contains method and function calls to be used to filter and select subsets of the data
require(data.table);#used to store and manipiulate data in the form of a data table similar to a mySQL database table
require(xts);#used to manipulate date-time data into quarters
require(ggplot2);#used to plot data
require(magrittr);#contains method/function calls to use for chaining e.g. %>%
require(scales);#component of ggplot2 that requires to be manually loaded to handle the scaling of various axies
require(foreach);#handles computations for each element of a vector/data frame/array/etc required the use of %do% or %dopar% operators in base R
require(iterators);#handles iteration through for each element of a vector/data frame/array/etc
require(stringr);#handles strings. Will be used to remove all quotation marks from the data
require(foreign);#to export data into formats readable by STATA and other statistical packages
require(xlsx);#3-party package to export data into excel xlsx format




##<<<<<<<<<<<<<<<<<<<<<<<<<LOAD AND PROCESS DATA>>>>>>>>>>>>>>>>>>>>>>>>
##Set Data Frequency
dataFrequency <- "quarterly"; #the options are monthly, quarterly, annual, daily (does not make sense since it does not distingush the year)
print(paste("data frequency is", dataFrequency, sep=" "));#used to record speed of computations
##Now load the individual FI's complete data tables
print("processing data tables");#used to record speed of computations
print(now());#used to record speed of computations


if(dataFrequency == "monthly"){
  acssusbeTransSentRecievedByFIXLSQuarterlySeries2000.15 <- read.csv("E:/Projects/RawData/USBE/acssusbeTransSentRecievedByFIXLSMonthlySeries2000-15.csv", stringsAsFactors=FALSE);
ACSSUSBETransDataSeriesSentRecieved <- as.data.table(acssusbeTransSentRecievedByFIXLSMonthlySeries2000.15);
ACSSUSBETransDataSeriesSentRecieved$Year_Month <- as.yearmon(ACSSUSBETransDataSeriesSentRecieved$Year_Month, format = "%YM%m");##as.yearqtr puts a space between the year and quarter
ACSSUSBETransDataSeriesSentRecieved$Year_Month <- format(ACSSUSBETransDataSeriesSentRecieved$Year_Month, "%YM%m");##forcing the format removes the space
##set the save path
ACSSUSBETransDataSeriesSentRecievedSavePath <-  c("E:/Projects/FundingDataTables/acssusbeTransSentRecievedByFIMonthlySeries2000-15.Rdata");
save(ACSSUSBETransDataSeriesSentRecieved, file=ACSSUSBETransDataSeriesSentRecievedSavePath); ##save the file
}else if(dataFrequency == "quarterly"){
  acssusbeTransSentRecievedByFIXLSQuarterlySeries2000.15 <- read.csv("E:/Projects/RawData/USBE/acssusbeTransSentRecievedByFIXLSQuarterlySeries2000-15.csv", stringsAsFactors=FALSE);
  ACSSUSBETransDataSeriesSentRecieved <- as.data.table(acssusbeTransSentRecievedByFIXLSQuarterlySeries2000.15);
  ACSSUSBETransDataSeriesSentRecieved$Year_Quarter <- as.yearqtr(ACSSUSBETransDataSeriesSentRecieved$Year_Quarter, format = "%YQ%q");##as.yearqtr puts a space between the year and quarter
  ACSSUSBETransDataSeriesSentRecieved$Year_Quarter <- format(ACSSUSBETransDataSeriesSentRecieved$Year_Quarter, "%YQ%q");##forcing the format removes the space
  ##set the save path
  ACSSUSBETransDataSeriesSentRecievedSavePath <-  c("E:/Projects/FundingDataTables/acssusbeTransSentRecievedByFIQuarterlySeries2000-15.Rdata");
  save(ACSSUSBETransDataSeriesSentRecieved, file=ACSSUSBETransDataSeriesSentRecievedSavePath); ##save the file
  
  }


