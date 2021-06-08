##This script is used to run the post processing for LVTS data
##The .R files used are  
##post_processing_file_functions_V01.R for the functions used
##lvts_post_processing_file_Level_1_V03.R for LVTS data files
##lvts_post_processing_file_Level_2_V07.R
##
##
##
## Author : Segun Bewaji
## Creation Date : 05 Dec 2016
## Modified : Segun Bewaji
## Modifications Made: 05 Dec 2016
##        1) wrote lines to run ACSS and LVTS data files processing scripts
##        2) 
##        3)  
##           
##        4)  
##           
##        5) 
##        6)  
## Modified : Segun Bewaji
## Modifications Made:
## $Id$

##the usual
# gc();
# rm(list=ls());
# gc();
# 
# ##import required libraries
# require(plyr);# contains method and function calls to be used to filter and select subsets of the data
# require(lubridate);# handles that date elements of the data
# require(dplyr);# contains method and function calls to be used to filter and select subsets of the data
# require(data.table);#used to store and manipiulate data in the form of a data table similar to a mySQL database table
# require(xts);#used to manipulate date-time data into quarters
# require(ggplot2);#used to plot data
# require(magrittr);#contains method/function calls to use for chaining e.g. %>%
# require(scales);#component of ggplot2 that requires to be manually loaded to handle the scaling of various axies
# require(foreach);#handles computations for each element of a vector/data frame/array/etc required the use of %do% or %dopar% operators in base R
# require(iterators);#handles iteration through for each element of a vector/data frame/array/etc
# require(stringr);#handles strings. Will be used to remove all quotation marks from the data
# require(foreign);#to export data into formats readable by STATA and other statistical packages
# require(xlsx);#3-party package to export data into excel xlsx format


##<<<<<<<<<<<<<<<<<<<<<<<<<DECLARE GLOBAL VARIABLES>>>>>>>>>>>>>>>>>>>>>>>>
##Declare all global variables to be used in this class

##<<<<<<<<<<<<<<<<<<<<<<<<<DECLARE GLOBAL VARIABLES>>>>>>>>>>>>>>>>>>>>>>>>
##Declare all global variables to be used in this class
# startYr <- 2000;
# endYr <- 2017;

mergedACSSTXNsNameString <- NULL;
##Set Data Frequency
# dataFrequency <- "quarterly"; #the options are monthly, quarterly, annual, daily, dateTime (does not make sense since it does not distingush the year)
# dateTimeNettingFrequency <- "1 min"; #This is used only if the dataFrequency is dateTime.
# #Available options are "x min", "x hour", "x day" etc where x is the integer value of the interval
# 
# byStream <- FALSE;

ACSSTXNsDataSeriesSavePath <- NULL;
ACSSTXNsDataSeriesSentRecievedSavePath <- NULL;
#will save the data table: bicCodeUpdatedACSSTXNsYearsSpec
ACSSTXNsDataSeriesByPeriodSTATASavePath <- NULL;
ACSSTXNsDataSeriesBySenderSTATASavePath <- NULL;
ACSSTXNsDataSeriesSTATASavePath <- NULL;
ACSSTXNsDataSeriesSentRecievedSTATASavePath <-  NULL;


extractedACSSDataLoadPath <- NULL;


##NULL declarations
ACSSTXNsDataSeries <- NULL; #working datatable
ACSSTXNsDataSeriesBySender <- NULL; #working datatable
ACSSTXNsDataSeriesBySenderReceiver <- NULL;
ACSSTXNsDataSeriesByPeriod <- NULL;
ACSSTXNsDataSeriesSentRecieved <- NULL; 

ACSSTXNsDataSeriesRecieved_df <-  NULL;
ACSSTXNsDataSeriesSent_df <-  NULL;
ACSSTXNsDataSeriesSentRecieved_df <- NULL; 

mergedACSSTXNsYearsSpecYM_df <- NULL; #working ACSS data data.frame
fiStaticDataFile_df <- NULL; #working static data data.frame


###################################<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<RAW RDATA FILE PATHS>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
extractedACSSDataLoadPath <- c(paste("E:/Projects/FundingDataTables/Raw Transactions Data/YearsSpecificACSSTXNsYM",startYr,"-",endYr,"DataSeries.Rdata", sep = "", collapse = NULL));
fiStaticDataLoadPath <- c("E:/Projects/FundingDataTables/FIStaticData.Rdata");#will load the data table: fiStaticDataFile
RETAILStreamDataLoadPath <- c("E:/Projects/FundingDataTables/RETAILStreamData.Rdata");#will load the data table: ACSSStreamDataFile


if(dataFrequency == "monthly"){
  
  mergedACSSTXNsNameString <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/summaryMonthlyACSSTXNs",startYr,"-",endYr,"DataSeries.Rdata", sep = "", collapse = NULL));
  mergedACSSTXNsXLSNameString <- c(paste("E:/Projects/FundingDataTables/STATADataTables/summaryMonthlyACSSTXNs",startYr,"-",endYr,"DataSeries.csv", sep = "", collapse = NULL));
  
  ##Intput Load Path
  extractedSummaryACSSDataLoadPath <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/summaryMonthlyACSSTXNs",startYr,"-",endYr,"DataSeries.Rdata", sep = "", collapse = NULL));
  
  ##Output Save Paths
  if(byStream == FALSE){
    ACSSTXNsDataSeriesSavePath <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/monthlyACSSTXNsDataSeries",startYr,"-",endYr,".Rdata", sep = "", collapse = NULL));
    ACSSTXNsDataSeriesSentRecievedSavePath <-   c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/acssTXNsSentRecievedByFIMonthlySeries",startYr,"-",endYr,".Rdata", sep = "", collapse = NULL));
    ACSSTXNsDataSeriesBySenderSTATASavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/acssTXNsMonthlyBySenderSTATASeries",startYr,"-",endYr,".dta", sep = "", collapse = NULL));
    ACSSTXNsDataSeriesBySenderXLSSavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/acssTXNsMonthlyBySenderSTATASeries",startYr,"-",endYr,".csv", sep = "", collapse = NULL));
    ACSSTXNsDataSeriesSTATASavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/acssTXNsMonthlySTATASeries",startYr,"-",endYr,".dta", sep = "", collapse = NULL));
    ACSSTXNsDataSeriesByPeriodSavePath <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/acssTXNsMonthlySeries",startYr,"-",endYr,".Rdata", sep = "", collapse = NULL));
    ACSSTXNsDataSeriesSentRecievedSTATASavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/acssTXNsSentRecievedByFIMonthlySTATASeries",startYr,"-",endYr,".dta", sep = "", collapse = NULL));
    ACSSTXNsDataSeriesSentRecievedXLSSavePath <-  c(paste("E:/Projects/FundingDataTables/STATADataTables/acssTXNsSentRecievedByFIXLSMonthlySeries",startYr,"-",endYr,".csv", sep = "", collapse = NULL));
    ACSSTXNsDataSeriesByPeriodXLSSavePath <-  c(paste("E:/Projects/FundingDataTables/STATADataTables/acssTXNsXLSMonthlySeries",startYr,"-",endYr,".csv", sep = "", collapse = NULL));
    
  }else {
    ACSSTXNsDataSeriesSavePath <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/monthlyACSSStreamTXNsDataSeries",startYr,"-",endYr,".Rdata", sep = "", collapse = NULL));
    ACSSTXNsByPeriodDataSeriesSavePath <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/monthlyACSSTXNsByStreamDataSeries",startYr,"-",endYr,".Rdata", sep = "", collapse = NULL));
    ACSSTXNsDataSeriesSentRecievedSavePath <-   c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/acssStreamTXNsSentRecievedByFIMonthlySeries",startYr,"-",endYr,".Rdata", sep = "", collapse = NULL));
    ACSSTXNsDataSeriesBySenderSTATASavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/acssStreamTXNsMonthlyBySenderSTATASeries",startYr,"-",endYr,".dta", sep = "", collapse = NULL));
    ACSSTXNsDataSeriesBySenderXLSSavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/acssStreamTXNsMonthlyBySenderSTATASeries",startYr,"-",endYr,".csv", sep = "", collapse = NULL));
    ACSSTXNsDataSeriesSTATASavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/acssStreamTXNsMonthlySTATASeries",startYr,"-",endYr,".dta", sep = "", collapse = NULL));
    ACSSTXNsDataSeriesByPeriodSavePath <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/acssTXNsMonthlyByStreamSeries",startYr,"-",endYr,".Rdata", sep = "", collapse = NULL));
    ACSSTXNsDataSeriesSentRecievedSTATASavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/acssStreamTXNsSentRecievedByFIMonthlySTATASeries",startYr,"-",endYr,".dta", sep = "", collapse = NULL));
    ACSSTXNsDataSeriesSentRecievedXLSSavePath <-  c(paste("E:/Projects/FundingDataTables/STATADataTables/acssStreamTXNsSentRecievedByFIXLSMonthlySeries",startYr,"-",endYr,".csv", sep = "", collapse = NULL));
    ACSSTXNsDataSeriesByPeriodXLSSavePath <-  c(paste("E:/Projects/FundingDataTables/STATADataTables/acssTXNsByStreamXLSMonthlySeries",startYr,"-",endYr,".csv", sep = "", collapse = NULL));
  }
  
  
}else if(dataFrequency == "quarterly"){
  
  mergedACSSTXNsNameString <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/summaryQuarterlyACSSTXNs",startYr,"-",endYr,"DataSeries.Rdata", sep = "", collapse = NULL));
  mergedACSSTXNsXLSNameString <- c(paste("E:/Projects/FundingDataTables/STATADataTables/summaryQuarterlyACSSTXNs",startYr,"-",endYr,"DataSeries.csv", sep = "", collapse = NULL));
  
  ##Intput Load Path
  extractedSummaryACSSDataLoadPath <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/summaryQuarterlyACSSTXNs",startYr,"-",endYr,"DataSeries.Rdata", sep = "", collapse = NULL));
  
  ##Output Save Paths
  if(byStream == FALSE){
    ACSSTXNsDataSeriesSavePath <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/quarterlyACSSTXNsDataSeries",startYr,"-",endYr,".Rdata", sep = "", collapse = NULL));
    ACSSTXNsDataSeriesSentRecievedSavePath <-   c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/acssTXNsSentRecievedByFIQuarterlySeries",startYr,"-",endYr,".Rdata", sep = "", collapse = NULL));
    ACSSTXNsDataSeriesByPeriodSavePath <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/acssTXNsQuarterlySeries",startYr,"-",endYr,".Rdata", sep = "", collapse = NULL));
    ACSSTXNsDataSeriesBySenderSTATASavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/acssTXNsQuarterlyBySenderSTATASeries",startYr,"-",endYr,".dta", sep = "", collapse = NULL));
    ACSSTXNsDataSeriesBySenderXLSSavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/acssTXNsQuarterlyBySenderSTATASeries",startYr,"-",endYr,".csv", sep = "", collapse = NULL));
    ACSSTXNsDataSeriesSTATASavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/acssTXNsQuarterlySTATASeries",startYr,"-",endYr,".dta", sep = "", collapse = NULL));
    ACSSTXNsDataSeriesSentRecievedSTATASavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/acssTXNsSentRecievedByFIQuarterlySTATASeries",startYr,"-",endYr,".dta", sep = "", collapse = NULL));
    ACSSTXNsDataSeriesSentRecievedXLSSavePath <-  c(paste("E:/Projects/FundingDataTables/STATADataTables/acssTXNsSentRecievedByFIXLSQuarterlySeries",startYr,"-",endYr,".csv", sep = "", collapse = NULL));
    ACSSTXNsDataSeriesByPeriodXLSSavePath <-  c(paste("E:/Projects/FundingDataTables/STATADataTables/acssTXNsXLSQuarterlySeries",startYr,"-",endYr,".csv", sep = "", collapse = NULL));
    
  }else{
    ACSSTXNsDataSeriesSavePath <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/quarterlyACSSStreamTXNsDataSeries",startYr,"-",endYr,".Rdata", sep = "", collapse = NULL));
    ACSSTXNsDataSeriesSentRecievedSavePath <-   c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/acssStreamTXNsSentRecievedByFIQuarterlySeries",startYr,"-",endYr,".Rdata", sep = "", collapse = NULL));
    ACSSTXNsDataSeriesByPeriodSavePath <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/acssTXNsQuarterlyByStreamSeries",startYr,"-",endYr,".Rdata", sep = "", collapse = NULL));
    ACSSTXNsDataSeriesBySenderSTATASavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/acssStreamTXNsQuarterlyBySenderSTATASeries",startYr,"-",endYr,".dta", sep = "", collapse = NULL));
    ACSSTXNsDataSeriesBySenderXLSSavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/acssStreamTXNsQuarterlyBySenderSTATASeries",startYr,"-",endYr,".csv", sep = "", collapse = NULL));
    ACSSTXNsDataSeriesSTATASavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/acssStreamTXNsQuarterlySTATASeries",startYr,"-",endYr,".dta", sep = "", collapse = NULL));
    ACSSTXNsDataSeriesSentRecievedSTATASavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/acssStreamTXNsSentRecievedByFIQuarterlySTATASeries",startYr,"-",endYr,".dta", sep = "", collapse = NULL));
    ACSSTXNsDataSeriesSentRecievedXLSSavePath <-  c(paste("E:/Projects/FundingDataTables/STATADataTables/acssStreamTXNsSentRecievedByFIXLSQuarterlySeries",startYr,"-",endYr,".csv", sep = "", collapse = NULL));
    ACSSTXNsDataSeriesByPeriodXLSSavePath <-  c(paste("E:/Projects/FundingDataTables/STATADataTables/acssTXNsByStreamXLSQuarterlySeries",startYr,"-",endYr,".csv", sep = "", collapse = NULL));
  }
  
  
}else if(dataFrequency == "annual"){
  
  mergedACSSTXNsNameString <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/summaryAnnualACSSTXNs",startYr,"-",endYr,"DataSeries.Rdata", sep = "", collapse = NULL));
  mergedACSSTXNsXLSNameString <- c(paste("E:/Projects/FundingDataTables/STATADataTables/summaryAnnualACSSTXNs",startYr,"-",endYr,"DataSeries.csv", sep = "", collapse = NULL));
  
  ##Intput Load Path
  extractedSummaryACSSDataLoadPath <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/summaryAnnualACSSTXNs",startYr,"-",endYr,"DataSeries.Rdata", sep = "", collapse = NULL));
  
  ##Output Save Paths
  if(byStream == FALSE){
    ACSSTXNsDataSeriesSavePath <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/annualACSSTXNsDataSeries",startYr,"-",endYr,".Rdata", sep = "", collapse = NULL));
    ACSSTXNsDataSeriesSentRecievedSavePath <-   c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/acssTXNsSentRecievedByFIAnnualSeries",startYr,"-",endYr,".Rdata", sep = "", collapse = NULL));
    ACSSTXNsDataSeriesByPeriodSavePath <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/acssTXNsAnnnualSeries",startYr,"-",endYr,".Rdata", sep = "", collapse = NULL));
    ACSSTXNsDataSeriesBySenderSTATASavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/acssTXNsAnnualBySenderSTATASeries",startYr,"-",endYr,".dta", sep = "", collapse = NULL));
    ACSSTXNsDataSeriesBySenderXLSSavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/acssTXNsAnnualBySenderSTATASeries",startYr,"-",endYr,".csv", sep = "", collapse = NULL));
    ACSSTXNsDataSeriesSTATASavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/acssTXNsAnnualSTATASeries",startYr,"-",endYr,".dta", sep = "", collapse = NULL));
    ACSSTXNsDataSeriesSentRecievedSTATASavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/acssTXNsSentRecievedByFIAnnualSTATASeries",startYr,"-",endYr,".dta", sep = "", collapse = NULL));
    ACSSTXNsDataSeriesSentRecievedXLSSavePath <-  c(paste("E:/Projects/FundingDataTables/STATADataTables/acssTXNsSentRecievedByFIXLSAnnualSeries",startYr,"-",endYr,".csv", sep = "", collapse = NULL));
    ACSSTXNsDataSeriesByPeriodXLSSavePath <-  c(paste("E:/Projects/FundingDataTables/STATADataTables/acssTXNsXLSAnnualSeries",startYr,"-",endYr,".csv", sep = "", collapse = NULL));
  }else{
    ACSSTXNsDataSeriesSavePath <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/annualACSSStreamTXNsDataSeries",startYr,"-",endYr,".Rdata", sep = "", collapse = NULL));
    ACSSTXNsDataSeriesSentRecievedSavePath <-   c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/acssStreamTXNsSentRecievedByFIAnnualSeries",startYr,"-",endYr,".Rdata", sep = "", collapse = NULL));
    ACSSTXNsDataSeriesByPeriodSavePath <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/acssTXNsAnnualByStreamSeries",startYr,"-",endYr,".Rdata", sep = "", collapse = NULL));
    ACSSTXNsDataSeriesBySenderSTATASavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/acssStreamTXNsAnnualBySenderSTATASeries",startYr,"-",endYr,".dta", sep = "", collapse = NULL));
    ACSSTXNsDataSeriesBySenderXLSSavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/acssStreamTXNsAnnualBySenderSTATASeries",startYr,"-",endYr,".csv", sep = "", collapse = NULL));
    ACSSTXNsDataSeriesSTATASavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/acssStreamTXNsAnnualSTATASeries",startYr,"-",endYr,".dta", sep = "", collapse = NULL));
    ACSSTXNsDataSeriesSentRecievedSTATASavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/acssStreamTXNsSentRecievedByFIAnnualSTATASeries",startYr,"-",endYr,".dta", sep = "", collapse = NULL));
    ACSSTXNsDataSeriesSentRecievedXLSSavePath <-  c(paste("E:/Projects/FundingDataTables/STATADataTables/acssStreamTXNsSentRecievedByFIXLSAnnualSeries",startYr,"-",endYr,".csv", sep = "", collapse = NULL));
    ACSSTXNsDataSeriesByPeriodXLSSavePath <-  c(paste("E:/Projects/FundingDataTables/STATADataTables/acssTXNsByStreamXLSAnnualSeries",startYr,"-",endYr,".csv", sep = "", collapse = NULL));
  }
  
  
}else if(dataFrequency == "daily"){
  
  mergedACSSTXNsNameString <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/summaryDailyACSSTXNs",startYr,"-",endYr,"DataSeries.Rdata", sep = "", collapse = NULL));
  mergedACSSTXNsXLSNameString <- c(paste("E:/Projects/FundingDataTables/STATADataTables/summaryDailyACSSTXNs",startYr,"-",endYr,"DataSeries.csv", sep = "", collapse = NULL));
  
  ##Intput Load Path
  #extractedSummaryACSSDataLoadPath <- c(paste("E:/Projects/FundingDataTables/YearsSpecificACSSTXNsYM",startYr,"-",endYr,"DataSeries.Rdata", sep = "", collapse = NULL));
  extractedSummaryACSSDataLoadPath <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/summaryDailyACSSTXNs",startYr,"-",endYr,"DataSeries.Rdata", sep = "", collapse = NULL));
  
  ##Output Save Paths
  if(byStream == FALSE){
    ACSSTXNsDataSeriesSavePath <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/dailyACSSTXNsDataSeries",startYr,"-",endYr,".Rdata", sep = "", collapse = NULL));
    ACSSTXNsDataSeriesSentRecievedSavePath <-   c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/acssTXNsSentRecievedByFIDailySeries",startYr,"-",endYr,".Rdata", sep = "", collapse = NULL));
    ACSSTXNsDataSeriesByPeriodSavePath <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/acssTXNsDailySeries",startYr,"-",endYr,".Rdata", sep = "", collapse = NULL));
    ACSSTXNsDataSeriesBySenderSTATASavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/acssTXNsDailyBySenderSTATASeries",startYr,"-",endYr,".dta", sep = "", collapse = NULL));
    ACSSTXNsDataSeriesBySenderXLSSavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/acssTXNsDailyBySenderSTATASeries",startYr,"-",endYr,".csv", sep = "", collapse = NULL));
    ACSSTXNsDataSeriesSTATASavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/acssTXNsDailySTATASeries",startYr,"-",endYr,".dta", sep = "", collapse = NULL));
    ACSSTXNsDataSeriesSentRecievedSTATASavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/acssTXNsSentRecievedByFIDailySTATASeries",startYr,"-",endYr,".dta", sep = "", collapse = NULL));
    ACSSTXNsDataSeriesSentRecievedXLSSavePath <-  c(paste("E:/Projects/FundingDataTables/STATADataTables/acssTXNsSentRecievedByFIXLSDailySeries",startYr,"-",endYr,".csv", sep = "", collapse = NULL));
    ACSSTXNsDataSeriesByPeriodXLSSavePath <-  c(paste("E:/Projects/FundingDataTables/STATADataTables/acssTXNsXLSDailySeries",startYr,"-",endYr,".csv", sep = "", collapse = NULL));
  }else{
    ACSSTXNsDataSeriesSavePath <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/dailyACSSStreamTXNsDataSeries",startYr,"-",endYr,".Rdata", sep = "", collapse = NULL));
    ACSSTXNsDataSeriesSentRecievedSavePath <-   c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/acssStreamTXNsSentRecievedByFIDailySeries",startYr,"-",endYr,".Rdata", sep = "", collapse = NULL));
    ACSSTXNsDataSeriesByPeriodSavePath <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/acssTXNsDailyByStreamSeries",startYr,"-",endYr,".Rdata", sep = "", collapse = NULL));
    ACSSTXNsDataSeriesBySenderSTATASavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/acssStreamTXNsDailyBySenderSTATASeries",startYr,"-",endYr,".dta", sep = "", collapse = NULL));
    ACSSTXNsDataSeriesBySenderXLSSavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/acssStreamTXNsDailyBySenderSTATASeries",startYr,"-",endYr,".csv", sep = "", collapse = NULL));
    ACSSTXNsDataSeriesSTATASavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/acssStreamTXNsDailySTATASeries",startYr,"-",endYr,".dta", sep = "", collapse = NULL));
    ACSSTXNsDataSeriesSentRecievedSTATASavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/acssStreamTXNsSentRecievedByFIDailySTATASeries",startYr,"-",endYr,".dta", sep = "", collapse = NULL));
    ACSSTXNsDataSeriesSentRecievedXLSSavePath <-  c(paste("E:/Projects/FundingDataTables/STATADataTables/acssStreamTXNsSentRecievedByFIXLSDailySeries",startYr,"-",endYr,".csv", sep = "", collapse = NULL));
    ACSSTXNsDataSeriesByPeriodXLSSavePath <-  c(paste("E:/Projects/FundingDataTables/STATADataTables/acssTXNsByStreamXLSDailySeries",startYr,"-",endYr,".csv", sep = "", collapse = NULL));
  }
  
  
}else if(dataFrequency == "dateTime"){
  
  mergedACSSTXNsNameString <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/summaryDateTimeACSSTXNs",startYr,"-",endYr,"DataSeries.Rdata", sep = "", collapse = NULL));
  mergedACSSTXNsXLSNameString <- c(paste("E:/Projects/FundingDataTables/STATADataTables/summaryDateTimeACSSTXNs",startYr,"-",endYr,"DataSeries.csv", sep = "", collapse = NULL));
  
  ##Intput Load Path
  extractedSummaryACSSDataLoadPath <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/summaryDateTimeACSSTXNs",startYr,"-",endYr,"DataSeries.Rdata", sep = "", collapse = NULL));
  
  ##Output Save Paths
  if(byStream == FALSE){
    ACSSTXNsDataSeriesSavePath <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/dateTimeACSSTXNsDataSeries",startYr,"-",endYr,".Rdata", sep = "", collapse = NULL));
    ACSSTXNsDataSeriesSentRecievedSavePath <-   c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/acssTXNsSentRecievedByFIDateTimeSeries",startYr,"-",endYr,".Rdata", sep = "", collapse = NULL));
    ACSSTXNsDataSeriesByPeriodSavePath <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/acssTXNsDateTimeSeries",startYr,"-",endYr,".Rdata", sep = "", collapse = NULL));
    ACSSTXNsDataSeriesBySenderSTATASavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/acssTXNsDateTimeBySenderSTATASeries",startYr,"-",endYr,".dta", sep = "", collapse = NULL));
    ACSSTXNsDataSeriesBySenderXLSSavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/acssTXNsDateTimeBySenderSTATASeries",startYr,"-",endYr,".csv", sep = "", collapse = NULL));
    ACSSTXNsDataSeriesSTATASavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/acssTXNsDateTimeSTATASeries",startYr,"-",endYr,".dta", sep = "", collapse = NULL));
    ACSSTXNsDataSeriesSentRecievedSTATASavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/acssTXNsSentRecievedByFIDateTimeSTATASeries",startYr,"-",endYr,".dta", sep = "", collapse = NULL));
    ACSSTXNsDataSeriesSentRecievedXLSSavePath <-  c(paste("E:/Projects/FundingDataTables/STATADataTables/acssTXNsSentRecievedByFIXLSDateTimeSeries",startYr,"-",endYr,".csv", sep = "", collapse = NULL));
    ACSSTXNsDataSeriesByPeriodXLSSavePath <-  c(paste("E:/Projects/FundingDataTables/STATADataTables/acssTXNsXLSDateTimeSeries",startYr,"-",endYr,".csv", sep = "", collapse = NULL));
    
  }else{
    ACSSTXNsDataSeriesSavePath <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/dateTimeACSSStreamTXNsDataSeries",startYr,"-",endYr,".Rdata", sep = "", collapse = NULL));
    ACSSTXNsDataSeriesSentRecievedSavePath <-   c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/acssStreamTXNsSentRecievedByFIDateTimeSeries",startYr,"-",endYr,".Rdata", sep = "", collapse = NULL));
    ACSSTXNsDataSeriesByPeriodSavePath <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/acssTXNsDateTimeByStreamSeries",startYr,"-",endYr,".Rdata", sep = "", collapse = NULL));
    ACSSTXNsDataSeriesBySenderSTATASavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/acssStreamTXNsDateTimeBySenderSTATASeries",startYr,"-",endYr,".dta", sep = "", collapse = NULL));
    ACSSTXNsDataSeriesBySenderXLSSavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/acssStreamTXNsDateTimeBySenderSTATASeries",startYr,"-",endYr,".csv", sep = "", collapse = NULL));
    ACSSTXNsDataSeriesSTATASavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/acssStreamTXNsDateTimeSTATASeries",startYr,"-",endYr,".dta", sep = "", collapse = NULL));
    ACSSTXNsDataSeriesSentRecievedSTATASavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/acssStreamTXNsSentRecievedByFIDateTimeSTATASeries",startYr,"-",endYr,".dta", sep = "", collapse = NULL));
    ACSSTXNsDataSeriesSentRecievedXLSSavePath <-  c(paste("E:/Projects/FundingDataTables/STATADataTables/acssStreamTXNsSentRecievedByFIXLSDateTimeSeries",startYr,"-",endYr,".csv", sep = "", collapse = NULL));
    ACSSTXNsDataSeriesByPeriodXLSSavePath <-  c(paste("E:/Projects/FundingDataTables/STATADataTables/acssTXNsByStreamXLSDateTimeSeries",startYr,"-",endYr,".csv", sep = "", collapse = NULL));
  }
  
}



###Load used functions
# gc();
# print("Loading Used Functions");
# source('C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/DataCollection/post_processing_file_functions_V01.R', encoding = 'UTF-8', echo=TRUE);
# gc();
##Run Level 1 post porocessing
##NOTE: Save paths are located in the source/script files
gc();
print("Commencing Level 1 post processing");
source('G:/Development Workspaces/R-Workspace/DataCollection/acss_post_processing_file_Level_1_V02.R', encoding = 'UTF-8', echo=TRUE);
print("Level 1 post processing complete");
gc();
##Run Level 2 post porocessing 
gc();
print("Commencing Level 2 post processing");
source('G:/Development Workspaces/R-Workspace/DataCollection/acss_post_processing_file_Level_2_V06.R', encoding = 'UTF-8', echo=TRUE);
print("Level 2 post processing complete");
gc();
