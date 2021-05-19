
##This script is used to collect the FI data generated in the acss_post_processing_file_Level_2_V##.R file.
##The generated file will be used to provide finance with the sumarry details for each FI's sending an recieving habits
##
##Final input data tables that the code will work with are 
##        a: ACSSTransDataSeriesSentRecieved - The ACSS data file
##        b: 
##
##
##Final outut is a list containing the transaction volume/value time series data 
##        a: ACSSFILevelTransDataSeriesList - The list containing ACSS time series data for each FI. Each element of the list is a time series object for the FI
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
endYr <- 2021;
minObservations <- 24; #minimum number of observations required for the collected time series
keyField <- "sender"; #the colum to be used as the unique key forthe data table (can be "sender" or "reciever")'
dataFrequency <- "monthly"; #the options are monthly, quarterly, annual, daily (does not make sense since it does not distingush the year)
onlyACSS <- FALSE;##Determine if the timeseries only looks at ACSS data or if it combines ACSS and USBE trannsactions data
dataFrequencyColumnName <- NULL; #used to capture the name of the time colum for use in the time series generation method/function.
                                 #dataFrequencyColumnName can take values "Year_Month", "Year_Quarter", "Year" "date" and will be set automatically 
                                 #based on the dataFrequency
extractedACSSDataLoadPath <- NULL; #used to store the location of the RData file from which the time sereis are to be generate
ACSSTransDataSeriesSavePath <- NULL;
ACSSTransDataSeriesSentRecievedSavePath <- NULL;



##Determine if the timeseries only looks at ACSS data or if it combines ACSS and USBE trannsactions data
if(onlyACSS==TRUE){
  #will load the data table: ACSSTransDataSeriesSentRecieved
  #and set the name of the time column (dataFrequencyColumnName)
  #if monthly
  if(dataFrequency == "monthly"){
    minObservations <- 24; # 12*2 number of observations per period multiplied by 2 full periods of data minimum number of observations required for the collected time series
    ##The ARIMA analysis requires 2 full periods
    ##Load image stream data
    ##load("C:/Projects/FundingDataTables/Cleaned Transactions Data/monthlyACSSTXNsDataSeriesSentRecievedStreamImageSum2015-2017.Rdata")
    #extractedACSSDataLoadPath <- c(paste("C:/Projects/FundingDataTables/Cleaned Transactions Data/monthlyACSSTXNsDataSeriesSentRecievedStreamImageSum",
    #                                     startYr,"-",endYr,".Rdata", sep = "", collapse = NULL));
    
    extractedACSSDataLoadPath <- c(paste("C:/Projects/FundingDataTables/Cleaned Transactions Data/acssTXNsSentRecievedByFIMonthlySeries",
                                         startYr,"-",endYr,".Rdata", sep = "", collapse = NULL));
    ACSSFITimeSeriesDataTableSavePath <- 
      c(paste("C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/ExtractedTimeSeriesData/ACSSFIMonthlyTimeSeriesData",
              startYr,"-",endYr,".RData", sep = "", collapse = NULL));
    
    ACSSUSBEValueFITimeSeriesDataTableSavePath <- 
      c(paste("C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/ExtractedTimeSeriesData/ACSSFIMonthlyValueTimeSeriesData",
              startYr,"-",endYr,".RData", sep = "", collapse = NULL));
   
    ##Load image stream data
   # ACSSFITimeSeriesDataTableSavePath <- 
    #  c(paste("C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/ExtractedTimeSeriesData/ACSSImageStreamFIMonthlyTimeSeriesData",
    #          startYr,"-",endYr,".RData", sep = "", collapse = NULL));
    
    dataFrequencyColumnName <- "Year_Month";
    #if quarterly  
  }else if(dataFrequency == "quarterly"){
    minObservations <- 8; # 4*2 number of observations per period multiplied by 2 full periods of data minimum number of observations required for the collected time series
    ##The ARIMA analysis requires 2 full periods
    extractedACSSDataLoadPath <- c(paste("C:/Projects/FundingDataTables/Cleaned Transactions Data/acssTXNsSentRecievedByFIQuarterlySeries",
                                         startYr,"-",endYr,".RData", sep = "", collapse = NULL));
    ACSSFITimeSeriesDataTableSavePath <- 
      c(paste("C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/ExtractedTimeSeriesData/ACSSFIQuarterlyTimeSeriesData",
              startYr,"-",endYr,".RData", sep = "", collapse = NULL));
    
    ACSSUSBEValueFITimeSeriesDataTableSavePath <- 
      c(paste("C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/ExtractedTimeSeriesData/ACSSFIQuarterlyValueTimeSeriesData",
              startYr,"-",endYr,".RData", sep = "", collapse = NULL));
    
    dataFrequencyColumnName <- "Year_Quarter";
    #if annual 
  }else if(dataFrequency == "annual"){
    minObservations <- 2; #1*2 number of observations per period multiplied by 2 full periods of data minimum number of observations required for the collected time series
    ##The ARIMA analysis requires 2 full periods
    extractedACSSDataLoadPath <- c(paste("C:/Projects/FundingDataTables/Cleaned Transactions Data/acssTXNsSentRecievedByFIAnnualSeries",
                                         startYr,"-",endYr,".RData", sep = "", collapse = NULL));
    ACSSFITimeSeriesDataTableSavePath <- 
      c(paste("C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/ExtractedTimeSeriesData/ACSSFIAnnualTimeSeriesData",
              startYr,"-",endYr,".RData", sep = "", collapse = NULL));
    
    ACSSUSBEValueFITimeSeriesDataTableSavePath <- 
      c(paste("C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/ExtractedTimeSeriesData/ACSSFIAnnualValueTimeSeriesData",
              startYr,"-",endYr,".RData", sep = "", collapse = NULL));
    
    dataFrequencyColumnName <- "Year";
    #if daily   
  }else if(dataFrequency == "daily"){
    minObservations <- 500; #250*2 number of observations per period multiplied by 2 full periods of data minimum number of observations required for the collected time series
    ##The ARIMA analysis requires 2 full periods
    extractedACSSDataLoadPath <- c(paste("C:/Projects/FundingDataTables/Cleaned Transactions Data/acssTXNsSentRecievedByFIDailySeries",
                                         startYr,"-",endYr,".RData", sep = "", collapse = NULL));
    ACSSFITimeSeriesDataTableSavePath <- 
      c(paste("C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/ExtractedTimeSeriesData/ACSSFIDailyTimeSeriesData",
              startYr,"-",endYr,".RData", sep = "", collapse = NULL));
    
    ACSSUSBEValueFITimeSeriesDataTableSavePath <- 
      c(paste("C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/ExtractedTimeSeriesData/ACSSFIDailyValueTimeSeriesData",
              startYr,"-",endYr,".RData", sep = "", collapse = NULL));
    
    dataFrequencyColumnName <- "date";
  }
  
}else{
  #will load the data table: USBETransDataSeriesSentRecieved
  #and set the name of the time column (dataFrequencyColumnName)
  #if monthly
  if(dataFrequency == "monthly"){
    minObservations <- 24; # 12*2 number of observations per period multiplied by 2 full periods of data minimum number of observations required for the collected time series
    ##The ARIMA analysis requires 2 full periods
#    ACSSUSBETXNsDataSeriesSentRecieved <- read.csv(c(paste("C:/Projects/RawData/ACSS-USBE/usbeACSSTransSentRecievedByFIXLSMonthlySeries",
#                                                           startYr,"-",endYr,".csv", sep = "", collapse = NULL)));
    extractedACSSUSBEDataLoadPath <- c(paste("C:/Projects/FundingDataTables/Cleaned Transactions Data/acssusbeTXNsSentRecievedByFIMonthlySeries",
                                         startYr,"-",endYr,".Rdata", sep = "", collapse = NULL));
    
    allFIACSSUSBETXNsDataSeriesSentRecievedSavePath <- c(paste("C:/Projects/FundingDataTables/Cleaned Transactions Data/allFIMonthlyACSSUSBETXNsDataSeriesSentRecieved",
                                                               startYr,"-",endYr,".RData", sep = "", collapse = NULL));
    
    
    allFIACSSUSBETXNsDataSeriesSentRecievedCSVSavePath <- c(paste("C:/Projects/FundingDataTables/Cleaned Transactions Data/allFIMonthlyACSSUSBETXNsDataSeriesSentRecieved",
                                                               startYr,"-",endYr,".csv", sep = "", collapse = NULL));
    
#    save(ACSSUSBETXNsDataSeriesSentRecieved,file=extractedACSSUSBEDataLoadPath);
    ACSSUSBEFITimeSeriesDataTableSavePath <- 
      c(paste("C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/ExtractedTimeSeriesData/ACSSUSBEFIMonthlyTimeSeriesData",
              startYr,"-",endYr,".RData", sep = "", collapse = NULL));
    
    ACSSUSBEValueFITimeSeriesDataTableSavePath <- 
      c(paste("C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/ExtractedTimeSeriesData/ACSSUSBEFIMonthlyValueTimeSeriesData",
              startYr,"-",endYr,".RData", sep = "", collapse = NULL));
    
    m112001ACSSUSBEFITimeSeriesMissingDataDummySavePath <- 
      c("C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/ExtractedTimeSeriesData/m112001ACSSUSBEFIMonthlyTSMissingDataDummy.RData");
    m12006ACSSUSBEFITimeSeriesMissingDataDummySavePath <- 
      c("C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/ExtractedTimeSeriesData/m12006ACSSUSBEFIMonthlyTSMissingDataDummy.RData");
    m102006ACSSUSBEFITimeSeriesMissingDataDummySavePath <- 
      c("C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/ExtractedTimeSeriesData/m102006ACSSUSBEFIMonthlyTSMissingDataDummy.RData");
    m112007ACSSUSBEFITimeSeriesMissingDataDummySavePath <- 
      c("C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/ExtractedTimeSeriesData/m112007ACSSUSBEFIMonthlyTSMissingDataDummy.RData");
    m122007ACSSUSBEFITimeSeriesMissingDataDummySavePath <- 
      c("C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/ExtractedTimeSeriesData/m122007ACSSUSBEFIMonthlyTSMissingDataDummy.RData");
     ACSSUSBEFITimeSeriesMissingDataDummySavePath<- 
      c("C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/ExtractedTimeSeriesData/ACSSUSBEFIMonthlyTSMissingDataDummy.RData");

    dataFrequencyColumnName <- "Year_Month";
    #if quarterly  
  }else if(dataFrequency == "quarterly"){
    minObservations <- 8; # 4*2 number of observations per period multiplied by 2 full periods of data minimum number of observations required for the collected time series
    ##The ARIMA analysis requires 2 full periods
#     ACSSUSBETXNsDataSeriesSentRecieved <- read.csv(c(paste("C:/Projects/RawData/ACSS-USBE/usbeACSSTransSentRecievedByFIXLSQuarterlySeries",
#                                                            startYr,"-",endYr,".csv", sep = "", collapse = NULL)));
    extractedACSSUSBEDataLoadPath <- c(paste("C:/Projects/FundingDataTables/Cleaned Transactions Data/acssusbeTXNsSentRecievedByFIQuarterlySeries",
                                         startYr,"-",endYr,".RData", sep = "", collapse = NULL));#acssusbeTXNsSentRecievedByFIQuarterlySeries
    
    allFIACSSUSBETXNsDataSeriesSentRecievedSavePath <- c(paste("C:/Projects/FundingDataTables/Cleaned Transactions Data/allFIQuarterlyACSSUSBETXNsDataSeriesSentRecieved",
                                                               startYr,"-",endYr,".RData", sep = "", collapse = NULL));
    # save(ACSSUSBETXNsDataSeriesSentRecieved,file=extractedACSSUSBEDataLoadPath);
    allFIACSSUSBETXNsDataSeriesSentRecievedCSVSavePath <- c(paste("C:/Projects/FundingDataTables/Cleaned Transactions Data/allFIQuarterlyACSSUSBETXNsDataSeriesSentRecieved",
                                                                  startYr,"-",endYr,".csv", sep = "", collapse = NULL));
    ACSSUSBEFITimeSeriesDataTableSavePath <- 
      c(paste("C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/ExtractedTimeSeriesData/ACSSUSBEFIQuarterlyTimeSeriesData",
              startYr,"-",endYr,".RData", sep = "", collapse = NULL));
    
    ACSSUSBEValueFITimeSeriesDataTableSavePath <- 
      c(paste("C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/ExtractedTimeSeriesData/ACSSUSBEFIQuarterlyValueTimeSeriesData",
              startYr,"-",endYr,".RData", sep = "", collapse = NULL));
    
    ACSSUSBEFITimeSeriesMissingDataDummySavePath <- 
      c("C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/ExtractedTimeSeriesData/ACSSUSBEFIQuarterlyTSMissingDataDummy.RData");
    
    dataFrequencyColumnName <- "Year_Quarter";
    #if annual 
  }else if(dataFrequency == "annual"){
    minObservations <- 2; #1*2 number of observations per period multiplied by 2 full periods of data minimum number of observations required for the collected time series
    ##The ARIMA analysis requires 2 full periods
#     ACSSUSBETXNsDataSeriesSentRecieved <- read.csv(c(paste("C:/Projects/RawData/ACSS-USBE/usbeACSSTransSentRecievedByFIXLSAnnualSeries",
#                                                            startYr,"-",endYr,".csv", sep = "", collapse = NULL)));
    extractedACSSUSBEDataLoadPath <- c(paste("C:/Projects/FundingDataTables/Cleaned Transactions Data/acssusbeTXNsSentRecievedByFIAnnualSeries",
                                         startYr,"-",endYr,".RData", sep = "", collapse = NULL));
    
    allFIACSSUSBETXNsDataSeriesSentRecievedSavePath <- c(paste("C:/Projects/FundingDataTables/Cleaned Transactions Data/allFIAnnualACSSUSBETXNsDataSeriesSentRecieved",
                                                               startYr,"-",endYr,".RData", sep = "", collapse = NULL));
    # save(ACSSUSBETXNsDataSeriesSentRecieved,file=extractedACSSUSBEDataLoadPath);
    ACSSUSBEFITimeSeriesDataTableSavePath <- 
      c(paste("C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/ExtractedTimeSeriesData/ACSSUSBEFIAnnualTimeSeriesData",
              startYr,"-",endYr,".RData", sep = "", collapse = NULL));
    
    ACSSUSBEValueFITimeSeriesDataTableSavePath <- 
      c(paste("C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/ExtractedTimeSeriesData/ACSSUSBEFIAnnualValueTimeSeriesData",
              startYr,"-",endYr,".RData", sep = "", collapse = NULL));
    
    ACSSUSBEFITimeSeriesMissingDataDummySavePath <- 
      c("C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/ExtractedTimeSeriesData/ACSSUSBEFIAnnualTSMissingDataDummy.RData");
    
    dataFrequencyColumnName <- "Year";
    #if daily   
  }else if(dataFrequency == "daily"){
    minObservations <- 500; #250*2 number of observations per period multiplied by 2 full periods of data minimum number of observations required for the collected time series
    ##The ARIMA analysis requires 2 full periods
#     ACSSUSBETXNsDataSeriesSentRecieved <- read.csv(c(paste("C:/Projects/RawData/ACSS-USBE/usbeACSSTransSentRecievedByFIXLSDailySeries",
#                                                            startYr,"-",endYr,".csv", sep = "", collapse = NULL)));
    extractedACSSUSBEDataLoadPath <- c(paste("C:/Projects/FundingDataTables/Cleaned Transactions Data/acssusbeTXNsSentRecievedByFIDailySeries",
                                         startYr,"-",endYr,".RData", sep = "", collapse = NULL));
    allFIACSSUSBETXNsDataSeriesSentRecievedSavePath <- c(paste("C:/Projects/FundingDataTables/Cleaned Transactions Data/allFIDailyACSSUSBETXNsDataSeriesSentRecieved",
                                                                startYr,"-",endYr,".RData", sep = "", collapse = NULL));
    # save(ACSSUSBETXNsDataSeriesSentRecieved,file=extractedACSSUSBEDataLoadPath);
    ACSSUSBEFITimeSeriesDataTableSavePath <- 
      c(paste("C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/ExtractedTimeSeriesData/ACSSUSBEFIDailyTimeSeriesData",
              startYr,"-",endYr,".RData", sep = "", collapse = NULL));
    
    ACSSUSBEValueFITimeSeriesDataTableSavePath <- 
      c(paste("C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/ExtractedTimeSeriesData/ACSSUSBEFIDailyValueTimeSeriesData",
              startYr,"-",endYr,".RData", sep = "", collapse = NULL));
    
    ACSSUSBEFITimeSeriesMissingDataDummySavePath <- 
      c("C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/ExtractedTimeSeriesData/ACSSUSBEFIDailyTSMissingDataDummyTS.RData");
    
    dataFrequencyColumnName <- "date";
  }
  
}


##Min obdservation overwrite
##minObservations <- 12;
##<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<START DATA/STATISTICAL COLLECTION PROCESSES>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
###Load used functions

gc();
print("Loading Used Functions");
source('C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/timeSeries_extraction_functions_V01.R', encoding = 'UTF-8', echo=TRUE);
gc();


if(onlyACSS){
  ###Build the Collection of Timeseries
  gc();
  print("Loading Used Functions");
  source('C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/acss_timeSeries_extraction_V02.R', encoding = 'UTF-8', echo=TRUE);
  gc();
  
}else{
  ###Build the Collection of Timeseries
  gc();
  print("Loading Used Functions");
  source('C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/acssusbe_timeSeries_extraction_V03.R', encoding = 'UTF-8', echo=TRUE);
  gc();
  

}




gc();#free up some memory


