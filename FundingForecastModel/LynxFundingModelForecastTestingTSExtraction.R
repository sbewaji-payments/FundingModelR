
##This script is used to collect the FI data generated in the lvts_post_processing_file_Level_2_V##.R file.
##The generated file will be used to provide finance with the sumarry details for each FI's sending an recieving habits
##
##Final input data tables that the code will work with are 
##        a: LVTSTransDataSeriesSentRecieved - The LVTS data file
##        b: 
##
##
##Final outut is a list containing the transaction volume/value time series data 
##        a: LVTSFILevelTransDataSeriesList - The list containing LVTS time series data for each FI. Each element of the list is a time series object for the FI
##        b: 
##
##
## Author : Segun Bewaji
## Creation Date : 28 Jun 2016
## Modified : Segun Bewaji
## Modifications Made: 
##        1)  
##           
##        2) 
##        3)  
##           
##        4)  
##           
##        5) 
##        6)  
## Modified :  
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
## $Id$

##the usual
gc();
rm(list=ls());
gc();

##<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<IMPORT REQUIRED LIBRARIES>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
##The use of require forces R to install the library if not already installed
require(plyr);# contains method and function calls to be used to filter and select subsets of the data
require(lubridate);# handles that date elements of the data
require(dplyr);# contains method and function calls to be used to filter and select subsets of the data
require(data.table);#used to store and manipiulate data in the form of a data table similar to a mySQL database table
require(xts);#used to manipulate date-time data into quarters
require(ggplot2);#used to plot data
require(zoo);#for date conventions used in financial analysis and quantitiative analysis
require(magrittr);#contains method/function calls to use for chaining e.g. %>%
require(scales);#component of ggplot2 that requires to be manually loaded to handle the scaling of various axies
require(foreach);#handles computations for each element of a vector/data frame/array/etc required the use of %do% or %dopar% operators in base R
require(iterators);#handles iteration through for each element of a vector/data frame/array/etc
require(stringr);#handles strings. Will be used to remove all quotation marks from the data
require(moments);#3rd-party library used to compute statistics such as skewness and kurtosis which are not provided in the base R package set
require(rlist);#3-party package to allow more flexible and java-collections like handling of lists in R. Allows you to save a list as a Rdata file
require(foreign);#to export data into formats readable by STATA and other statistical packages
require(xlsx);#3-party package to export data into excel xlsx format




##<<<<<<<<<<<<<<<<<<<<<<<<<DECLARE GLOBAL VARIABLES>>>>>>>>>>>>>>>>>>>>>>>>
##Declare all global variables to be used in this class
startYr <- 2000;
endYr <- 2019;
minObservations <- NULL; #minimum number of observations required for the collected time series
keyField <- "sender"; #the colum to be used as the unique key forthe data table (can be "sender" or "reciever")'
dataFrequency <- "quarterly"; #the options are monthly, quarterly, annual, daily (does not make sense since it does not distingush the year)
dataFrequencyColumnName <- NULL; #used to capture the name of the time colum for use in the time series generation method/function.
#dataFrequencyColumnName can take values "Year_Month", "Year_Quarter", "Year" "date" and will be set automatically 
#based on the dataFrequency
extractedLVTSDataLoadPath <- NULL; #used to store the location of the RData file from which the time sereis are to be generate
LVTSTransDataSeriesSavePath <- NULL;
LVTSTransDataSeriesSentRecievedSavePath <- NULL;
manualIntervention <- FALSE;
under50K <- FALSE;



#will load the data table: LVTSTransDataSeriesSentRecieved
#and set the name of the time column (dataFrequencyColumnName)
#if monthly
if(dataFrequency == "monthly"){
  minObservations <- 24+1; # 12*2+1 number of observations per period multiplied by 2 full periods of data minimum number of observations required for the collected time series
  ##The ARIMA analysis requires 2 full periods
  if(under50K){
    extractedLVTSDataLoadPath <- c(paste("C:/Projects/FundingDataTables/Cleaned Transactions Data/lvtsTXNsupto50KMonthlySeries",startYr,"-",endYr,".RData", sep = "", collapse = NULL));
  } else {
    extractedLVTSDataLoadPath <- c(paste("C:/Projects/FundingDataTables/Cleaned Transactions Data/lvtsTXNsOver50KMonthlySeries",startYr,"-",endYr,".RData", sep = "", collapse = NULL));
  }
  LVTSFIVolumeTimeSeriesDataTableSavePath <- 
    c(paste("C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/ExtractedTimeSeriesData/lvtsTXNsOver50KMonthlyVolumeSeriesTimeSeriesData",
            startYr,"-",endYr,".RData", sep = "", collapse = NULL));
  LVTSFIValueTimeSeriesDataTableSavePath <- 
    c(paste("C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/ExtractedTimeSeriesData/lvtsTXNsOver50KMonthlyValueSeriesTimeSeriesData",
            startYr,"-",endYr,".RData", sep = "", collapse = NULL));
  dataFrequencyColumnName <- "Year_Month";
  #if quarterly  
}else if(dataFrequency == "quarterly"){
  minObservations <- 8+3; # 4*2 number of observations per period multiplied by 2 full periods of data minimum number of observations required for the collected time series
  ##The ARIMA analysis requires 2 full periods
  if(under50K){
    extractedLVTSDataLoadPath <- c(paste("C:/Projects/FundingDataTables/Cleaned Transactions Data/lvtsTXNsupto50KQuarterlySeries",startYr,"-",endYr,".RData", sep = "", collapse = NULL));
   } else {
    extractedLVTSDataLoadPath <- c(paste("C:/Projects/FundingDataTables/Cleaned Transactions Data/lvtsTXNsOver50KQuarterlySeries",startYr,"-",endYr,".RData", sep = "", collapse = NULL));
  }
  LVTSFIVolumeTimeSeriesDataTableSavePath <- 
    c(paste("C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/ExtractedTimeSeriesData/lvtsTXNsOver50KQuarterlyVolumeSeriesTimeSeriesData",
            startYr,"-",endYr,".RData", sep = "", collapse = NULL));
  LVTSFIValueTimeSeriesDataTableSavePath <- 
    c(paste("C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/ExtractedTimeSeriesData/lvtsTXNsOver50KQuarterlyValueSeriesTimeSeriesData",
            startYr,"-",endYr,".RData", sep = "", collapse = NULL));
  dataFrequencyColumnName <- "Year_Quarter";
  #if annual 
}else if(dataFrequency == "annual"){
  minObservations <- 2+1; #1*2 number of observations per period multiplied by 2 full periods of data minimum number of observations required for the collected time series
  ##The ARIMA analysis requires 2 full periods
  if(under50K){
    extractedLVTSDataLoadPath <- c(paste("C:/Projects/FundingDataTables/Cleaned Transactions Data/lvtsTXNsupto50KAnnualSeries",startYr,"-",endYr,".RData", sep = "", collapse = NULL));
  } else {
    extractedLVTSDataLoadPath <- c(paste("C:/Projects/FundingDataTables/Cleaned Transactions Data/lvtsTXNsOver50KAnnualSeries",startYr,"-",endYr,".RData", sep = "", collapse = NULL));
  }
  LVTSFIVolumeTimeSeriesDataTableSavePath <- 
    c(paste("C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/ExtractedTimeSeriesData/lvtsTXNsOver50KAnnualVolumeSeriesTimeSeriesData",
            startYr,"-",endYr,".RData", sep = "", collapse = NULL));
  LVTSFIValueTimeSeriesDataTableSavePath <- 
    c(paste("C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/ExtractedTimeSeriesData/lvtsTXNsOver50KAnnualValueSeriesTimeSeriesData",
            startYr,"-",endYr,".RData", sep = "", collapse = NULL));
  dataFrequencyColumnName <- "Year";
  #if daily   
}else if(dataFrequency == "daily"){
  minObservations <- 500; #250*2 number of observations per period multiplied by 2 full periods of data minimum number of observations required for the collected time series
  ##The ARIMA analysis requires 2 full periods
  if(under50K){
    extractedLVTSDataLoadPath <- c(paste("C:/Projects/FundingDataTables/Cleaned Transactions Data/lvtsTXNsupto50KDailySeries",startYr,"-",endYr,".RData", sep = "", collapse = NULL));
  } else {
    extractedLVTSDataLoadPath <- c(paste("C:/Projects/FundingDataTables/Cleaned Transactions Data/lvtsTXNsOver50KDailySeries",startYr,"-",endYr,".RData", sep = "", collapse = NULL));
  }
  LVTSFIVolumeTimeSeriesDataTableSavePath <- 
    c(paste("C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/ExtractedTimeSeriesData/lvtsTXNsOver50KDailyVolumeSeriesTimeSeriesData",
            startYr,"-",endYr,".RData", sep = "", collapse = NULL));
  LVTSFIValueTimeSeriesDataTableSavePath <- 
    c(paste("C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/ExtractedTimeSeriesData/lvtsTXNsOver50KDailyValueSeriesTimeSeriesData",
            startYr,"-",endYr,".RData", sep = "", collapse = NULL));
  dataFrequencyColumnName <- "date";
}



##<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<START DATA/STATISTICAL COLLECTION PROCESSES>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
###Load used functions
gc();
print("Loading Used Functions");
source('C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/timeSeries_extraction_functions_V01.R', encoding = 'UTF-8', echo=TRUE);
gc();


if(manualIntervention==TRUE){
  ##Manual intervention
  # LVTSTXNsDataSeriesSentRecievedCSVSavePath <- 
  #   c(paste("C:/Projects/FundingDataTables/Cleaned Transactions Data/LVTSTXNsDataSeriesSentRecievedCSVSeries",startYr,"-",endYr,".csv", sep = "", collapse = NULL));
  ##Save the data to csv for maunal editing
  # load(extractedLVTSDataLoadPath)
  # write.csv(LVTSTXNsDataSeriesSentRecieved, file=LVTSTXNsDataSeriesSentRecievedCSVSavePath);
  
  ##LVTSTXNsDataSeriesSentRecievedCSVEditsLoadPath 
  ##reset the extracted data load path to manually edited file
  # extractedLVTSDataLoadPath <- 
  #   c(paste("C:/Projects/FundingDataTables/Cleaned Transactions Data/LVTSTXNsDataSeriesSentRecievedCSVSeriesEdited",startYr,"-",endYr,".csv", sep = "", collapse = NULL));
}

###Build the Collection of Timeseries
gc();
print("Loading Used Functions");
source('C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/lynx_testing_timeSeries_extraction_V01.R', encoding = 'UTF-8', echo=TRUE);
gc();

