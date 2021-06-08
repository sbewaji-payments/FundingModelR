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
gc();
rm(list=ls());
gc();

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


options(scipen=999);
##<<<<<<<<<<<<<<<<<<<<<<<<<DECLARE GLOBAL VARIABLES>>>>>>>>>>>>>>>>>>>>>>>>
##Declare all global variables to be used in this class

##<<<<<<<<<<<<<<<<<<<<<<<<<DECLARE GLOBAL VARIABLES>>>>>>>>>>>>>>>>>>>>>>>>
##Declare all global variables to be used in this class
startYr <- 2000;
endYr <- 2021;
segStartYr <- 2000;
segEndYr <- 2021;

mergedLVTSTXNsNameString <- NULL;

dataFrequency <- "monthly"; #the options are monthly, quarterly, annual, daily, dateTime (does not make sense since it does not distingush the year)
dateTimeNettingFrequency <- "1 hour"; #This is used only if the dataFrequency is dateTime.
#Available options are "x min", "x hour", "x day" etc where x is the integer value of the interval
byTranche <- FALSE; #FALSE TRUE

byStream <- FALSE; #FALSE TRUE

LVTSTXNsDataSeriesTranche1 <- NULL;
LVTSTXNsDataSeriesTranche2 <- NULL;

fiStaticDataLoadPath <- NULL;#will load the data table: fiStaticDataFile
LVTSStreamDataLoadPath <- NULL;#will load the data table: ACSSStreamDataFile
extractedLVTSDataLoadPath <- NULL;#will be used to load the collated raw data for level 1 processing
extractedSummaryLVTSDataLoadPath <- NULL; #will be used to the summarised/transfored data output from the level 1 processing


LVTSTXNsDataSeriesSavePath <- NULL;
#will save the data table: bicCodeUpdatedACSSTransYearsSpec
LVTSTXNsDataSeriesByPeriodSTATASavePath <- NULL;
LVTSTXNsDataSeriesBySenderSTATASavePath <- NULL;
LVTSTXNsDataSeriesSTATASavePath <- NULL;
LVTSTXNsDataSeriesSentRecievedSTATASavePath <-  NULL;


##NULL declarations
LVTSTXNsDataSeries <- NULL; #working datatable
LVTSTXNsDataSeriesBySender <- NULL; #working datatable
LVTSTXNsDataSeriesBySenderReceiver <- NULL;
LVTSTXNsDataSeriesByPeriod <- NULL;
LVTSTXNsDataSeriesRecieved <-  NULL;
LVTSTXNsDataSeriesTranche1 <- NULL;
LVTSTXNsDataSeriesTranche2 <- NULL;

mergedLVTSTXNsYearsSpecYM_df <- NULL; #working ACSS data data.frame
LVTSTXNsDataSeriesRecieved_df <-  NULL;
LVTSTXNsDataSeriesSent_df <-  NULL;
LVTSTXNsDataSeriesSentRecieved_df <- NULL; 
fiStaticDataFile_df <- NULL; #working static data data.frame

###################################<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<RAW RDATA FILE PATHS>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
extractedLVTSDataLoadPath <- c(paste("E:/Projects/FundingDataTables/Raw Transactions Data/YearsSpecificLVTSTXNsYM",startYr,"-",endYr,"DataSeries.Rdata", sep = "", collapse = NULL));
fiStaticDataLoadPath <- c("E:/Projects/FundingDataTables/FIStaticData.Rdata");#will load the data table: fiStaticDataFile
LVTSStreamDataLoadPath <- c("E:/Projects/FundingDataTables/LVTSStreamData.Rdata");#will load the data table: ACSSStreamDataFile



if(dataFrequency == "monthly"){

  if(byTranche==FALSE){
    mergedLVTSTXNsNameString <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/summaryMonthlyLVTSTXNs",startYr,"-",endYr,"DataSeries.Rdata", sep = "", collapse = NULL));
    mergedLVTSTXNsXLSNameString <- c(paste("E:/Projects/FundingDataTables/STATADataTables/summaryMonthlyLVTSTXNs",startYr,"-",endYr,"DataSeries.csv", sep = "", collapse = NULL));
    
    ##Intput Load Path
    extractedSummaryLVTSDataLoadPath <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/summaryMonthlyLVTSTXNs",startYr,"-",endYr,"DataSeries.Rdata", sep = "", collapse = NULL));
    
    ##Output Save Paths
    LVTSTXNsDataSeriesSavePath <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/monthlyLVTSTXNsDataSeries",startYr,"-",endYr,".Rdata", sep = "", collapse = NULL));
    LVTSTXNsDataSeriesSentRecievedSavePath <-   c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/lvtsTXNsSentRecievedByFIMonthlySeries",startYr,"-",endYr,".Rdata", sep = "", collapse = NULL));
    LVTSTXNsDataSeriesBySenderSTATASavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/lvtsTXNsMonthlyBySenderSTATASeries",startYr,"-",endYr,".dta", sep = "", collapse = NULL));
    LVTSTXNsDataSeriesSTATASavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/lvtsTXNsMonthlySTATASeries",startYr,"-",endYr,".dta", sep = "", collapse = NULL));
    LVTSTXNsDataSeriesSentRecievedSTATASavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/lvtsTXNsSentRecievedByFIMonthlySTATASeries",startYr,"-",endYr,".dta", sep = "", collapse = NULL));
    LVTSTXNsDataSeriesSentRecievedXLSSavePath <-  c(paste("E:/Projects/FundingDataTables/STATADataTables/lvtsTXNsSentRecievedByFIXLSMonthlySeries",startYr,"-",endYr,".csv", sep = "", collapse = NULL));
    
    LVTSTXNsDataSeriesTranche1SavePath <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/monthlyLVTSTXNsTranche1DataSeries",startYr,"-",endYr,".Rdata", sep = "", collapse = NULL));
    LVTSTXNsDataSeriesTranche2SavePath <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/monthlylLVTSTXNsTranche2DataSeries",startYr,"-",endYr,".Rdata", sep = "", collapse = NULL));
    
    
  }else{
    mergedLVTSTXNsNameString <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/summaryMonthlyLVTSTrancheTXNs",startYr,"-",endYr,"DataSeries.Rdata", sep = "", collapse = NULL));
    mergedLVTSTXNsXLSNameString <- c(paste("E:/Projects/FundingDataTables/STATADataTables/summaryMonthlyLVTSTrancheTXNs",startYr,"-",endYr,"DataSeries.csv", sep = "", collapse = NULL));
    ##Intput Load Path
    extractedSummaryLVTSDataLoadPath <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/summaryMonthlyLVTSTrancheTXNs",startYr,"-",endYr,"DataSeries.Rdata", sep = "", collapse = NULL));
    
    ##Output Save Paths
    LVTSTXNsDataSeriesSavePath <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/monthlyLVTSTrancheTXNsDataSeries",startYr,"-",endYr,".Rdata", sep = "", collapse = NULL));
    LVTSTXNsDataSeriesSentRecievedSavePath <-   c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/lvtsTrancheTXNsSentRecievedByFIMonthlySeries",startYr,"-",endYr,".Rdata", sep = "", collapse = NULL));
    LVTSTXNsDataSeriesBySenderSTATASavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/lvtsTrancheTXNsMonthlyBySenderSTATASeries",startYr,"-",endYr,".dta", sep = "", collapse = NULL));
    LVTSTXNsDataSeriesSTATASavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/lvtsTrancheTXNsMonthlySTATASeries",startYr,"-",endYr,".dta", sep = "", collapse = NULL));
    LVTSTXNsDataSeriesSentRecievedSTATASavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/lvtsTrancheTXNsSentRecievedByFIMonthlySTATASeries",startYr,"-",endYr,".dta", sep = "", collapse = NULL));
    LVTSTXNsDataSeriesSentRecievedXLSSavePath <-  c(paste("E:/Projects/FundingDataTables/STATADataTables/lvtsTrancheTXNsSentRecievedByFIXLSMonthlySeries",startYr,"-",endYr,".csv", sep = "", collapse = NULL));
    
    LVTSTXNsDataSeriesTranche1SavePath <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/monthlyLVTSTXNsTranche1DataSeries",startYr,"-",endYr,".Rdata", sep = "", collapse = NULL));
    LVTSTXNsDataSeriesTranche2SavePath <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/monthlylLVTSTXNsTranche2DataSeries",startYr,"-",endYr,".Rdata", sep = "", collapse = NULL));
    
  }
  
  
}else if(dataFrequency == "quarterly"){

  if(byTranche==FALSE){
    mergedLVTSTXNsNameString <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/summaryQuarterlyLVTSTXNs",startYr,"-",endYr,"DataSeries.Rdata", sep = "", collapse = NULL));
    mergedLVTSTXNsXLSNameString <- c(paste("E:/Projects/FundingDataTables/STATADataTables/summaryQuarterlyLVTSTXNs",startYr,"-",endYr,"DataSeries.csv", sep = "", collapse = NULL));
    
    ##Intput Load Path
    extractedSummaryLVTSDataLoadPath <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/summaryQuarterlyLVTSTXNs",startYr,"-",endYr,"DataSeries.Rdata", sep = "", collapse = NULL));
    
    ##Output Save Paths
    LVTSTXNsDataSeriesSavePath <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/quarterlyLVTSTXNsDataSeries",startYr,"-",endYr,".Rdata", sep = "", collapse = NULL));
    LVTSTXNsDataSeriesSentRecievedSavePath <-   c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/lvtsTXNsSentRecievedByFIQuarterlySeries",startYr,"-",endYr,".Rdata", sep = "", collapse = NULL));
    LVTSTXNsDataSeriesBySenderSTATASavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/lvtsTXNsQuarterlyBySenderSTATASeries",startYr,"-",endYr,".dta", sep = "", collapse = NULL));
    LVTSTXNsDataSeriesSTATASavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/lvtsTXNsQuarterlySTATASeries",startYr,"-",endYr,".dta", sep = "", collapse = NULL));
    LVTSTXNsDataSeriesSentRecievedSTATASavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/lvtsTXNsSentRecievedByFIQuarterlySTATASeries",startYr,"-",endYr,".dta", sep = "", collapse = NULL));
    LVTSTXNsDataSeriesSentRecievedXLSSavePath <-  c(paste("E:/Projects/FundingDataTables/STATADataTables/lvtsTXNsSentRecievedByFIXLSQuarterlySeries",startYr,"-",endYr,".csv", sep = "", collapse = NULL));
    
    LVTSTXNsDataSeriesTranche1SavePath <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/quarterlyLVTSTXNsTranche1DataSeries",startYr,"-",endYr,".Rdata", sep = "", collapse = NULL));
    LVTSTXNsDataSeriesTranche2SavePath <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/quarterlylLVTSTXNsTranche2DataSeries",startYr,"-",endYr,".Rdata", sep = "", collapse = NULL));
    
  } else{
    mergedLVTSTXNsNameString <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/summaryQuarterlyLVTSTrancheTXNs",startYr,"-",endYr,"DataSeries.Rdata", sep = "", collapse = NULL));
    mergedLVTSTXNsXLSNameString <- c(paste("E:/Projects/FundingDataTables/STATADataTables/summaryQuarterlyLVTSTrancheTXNs",startYr,"-",endYr,"DataSeries.csv", sep = "", collapse = NULL));
    
    ##Intput Load Path
    extractedSummaryLVTSDataLoadPath <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/summaryQuarterlyLVTSTrancheTXNs",startYr,"-",endYr,"DataSeries.Rdata", sep = "", collapse = NULL));
    
    ##Output Save Paths
    LVTSTXNsDataSeriesSavePath <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/quarterlyLVTSTrancheTXNsDataSeries",startYr,"-",endYr,".Rdata", sep = "", collapse = NULL));
    LVTSTXNsDataSeriesSentRecievedSavePath <-   c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/lvtsTrancheTXNsSentRecievedByFIQuarterlySeries",startYr,"-",endYr,".Rdata", sep = "", collapse = NULL));
    LVTSTXNsDataSeriesBySenderSTATASavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/lvtsTrancheTXNsQuarterlyBySenderSTATASeries",startYr,"-",endYr,".dta", sep = "", collapse = NULL));
    LVTSTXNsDataSeriesSTATASavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/lvtsTrancheTXNsQuarterlySTATASeries",startYr,"-",endYr,".dta", sep = "", collapse = NULL));
    LVTSTXNsDataSeriesSentRecievedSTATASavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/lvtsTrancheTXNsSentRecievedByFIQuarterlySTATASeries",startYr,"-",endYr,".dta", sep = "", collapse = NULL));
    LVTSTXNsDataSeriesSentRecievedXLSSavePath <-  c(paste("E:/Projects/FundingDataTables/STATADataTables/lvtsTrancheTXNsSentRecievedByFIXLSQuarterlySeries",startYr,"-",endYr,".csv", sep = "", collapse = NULL));
    
    LVTSTXNsDataSeriesTranche1SavePath <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/quarterlyLVTSTXNsTranche1DataSeries",startYr,"-",endYr,".Rdata", sep = "", collapse = NULL));
    LVTSTXNsDataSeriesTranche2SavePath <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/quarterlylLVTSTXNsTranche2DataSeries",startYr,"-",endYr,".Rdata", sep = "", collapse = NULL));
    
  }
  
  
}else if(dataFrequency == "annual"){
  
  
  if(byTranche==FALSE){
    mergedLVTSTXNsNameString <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/summaryAnnualLVTSTXNs",startYr,"-",endYr,"DataSeries.Rdata", sep = "", collapse = NULL));
    mergedLVTSTXNsXLSNameString <- c(paste("E:/Projects/FundingDataTables/STATADataTables/summaryAnnualLVTSTXNs",startYr,"-",endYr,"DataSeries.csv", sep = "", collapse = NULL));
    
    ##Intput Load Path
    extractedSummaryLVTSDataLoadPath <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/summaryAnnualLVTSTXNs",startYr,"-",endYr,"DataSeries.Rdata", sep = "", collapse = NULL));
    
    ##Output Save Paths
    LVTSTXNsDataSeriesSavePath <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/annualLVTSTXNsDataSeries",startYr,"-",endYr,".Rdata", sep = "", collapse = NULL));
    LVTSTXNsDataSeriesSentRecievedSavePath <-   c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/lvtsTXNsSentRecievedByFIAnnualSeries",startYr,"-",endYr,".Rdata", sep = "", collapse = NULL));
    LVTSTXNsDataSeriesBySenderSTATASavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/lvtsTXNsAnnualBySenderSTATASeries",startYr,"-",endYr,".dta", sep = "", collapse = NULL));
    LVTSTXNsDataSeriesSTATASavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/lvtsTXNsAnnualSTATASeries",startYr,"-",endYr,".dta", sep = "", collapse = NULL));
    LVTSTXNsDataSeriesSentRecievedSTATASavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/lvtsTXNsSentRecievedByFIAnnualSTATASeries",startYr,"-",endYr,".dta", sep = "", collapse = NULL));
    LVTSTXNsDataSeriesSentRecievedXLSSavePath <-  c(paste("E:/Projects/FundingDataTables/STATADataTables/lvtsTXNsSentRecievedByFIXLSAnnualSeries",startYr,"-",endYr,".csv", sep = "", collapse = NULL));
    
    LVTSTXNsDataSeriesTranche1SavePath <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/annualLVTSTXNsTranche1DataSeries",startYr,"-",endYr,".Rdata", sep = "", collapse = NULL));
    LVTSTXNsDataSeriesTranche2SavePath <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/annualLVTSTXNsTranche2DataSeries",startYr,"-",endYr,".Rdata", sep = "", collapse = NULL));
    
  } else{
    mergedLVTSTXNsNameString <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/summaryAnnualLVTSTrancheTXNs",startYr,"-",endYr,"DataSeries.Rdata", sep = "", collapse = NULL));
    mergedLVTSTXNsXLSNameString <- c(paste("E:/Projects/FundingDataTables/STATADataTables/summaryAnnualLVTSTrancheTXNs",startYr,"-",endYr,"DataSeries.csv", sep = "", collapse = NULL));
    
    ##Intput Load Path
    extractedSummaryLVTSDataLoadPath <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/summaryAnnualLVTSTrancheTXNs",startYr,"-",endYr,"DataSeries.Rdata", sep = "", collapse = NULL));
    
    ##Output Save Paths
    LVTSTXNsDataSeriesSavePath <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/annualLVTSTrancheTXNsDataSeries",startYr,"-",endYr,".Rdata", sep = "", collapse = NULL));
    LVTSTXNsDataSeriesSentRecievedSavePath <-   c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/lvtsTrancheTXNsSentRecievedByFIAnnualSeries",startYr,"-",endYr,".Rdata", sep = "", collapse = NULL));
    LVTSTXNsDataSeriesBySenderSTATASavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/lvtsTrancheTXNsAnnualBySenderSTATASeries",startYr,"-",endYr,".dta", sep = "", collapse = NULL));
    LVTSTXNsDataSeriesSTATASavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/lvtsTrancheTXNsAnnualSTATASeries",startYr,"-",endYr,".dta", sep = "", collapse = NULL));
    LVTSTXNsDataSeriesSentRecievedSTATASavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/lvtsTrancheTXNsSentRecievedByFIAnnualSTATASeries",startYr,"-",endYr,".dta", sep = "", collapse = NULL));
    LVTSTXNsDataSeriesSentRecievedXLSSavePath <-  c(paste("E:/Projects/FundingDataTables/STATADataTables/lvtsTrancheTXNsSentRecievedByFIXLSAnnualSeries",startYr,"-",endYr,".csv", sep = "", collapse = NULL));
    
    LVTSTXNsDataSeriesTranche1SavePath <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/annualLVTSTXNsTranche1DataSeries",startYr,"-",endYr,".Rdata", sep = "", collapse = NULL));
    LVTSTXNsDataSeriesTranche2SavePath <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/annualLVTSTXNsTranche2DataSeries",startYr,"-",endYr,".Rdata", sep = "", collapse = NULL));
    
  }

  
  
}else if(dataFrequency == "daily"){
  
  if(byTranche == FALSE){
    mergedLVTSTXNsNameString <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/summaryDailyLVTSTXNs",startYr,"-",endYr,"DataSeries.Rdata", sep = "", collapse = NULL));
    mergedLVTSTXNsXLSNameString <- c(paste("E:/Projects/FundingDataTables/STATADataTables/summaryDailyLVTSTXNs",startYr,"-",endYr,"DataSeries.csv", sep = "", collapse = NULL));
    
    ##Intput Load Path
    extractedSummaryLVTSDataLoadPath <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/summaryDailyLVTSTXNs",startYr,"-",endYr,"DataSeries.Rdata", sep = "", collapse = NULL));
    
    ##Output Save Paths
    LVTSTXNsDataSeriesSavePath <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/dailyLVTSTXNsDataSeries",startYr,"-",endYr,".Rdata", sep = "", collapse = NULL));
    LVTSTXNsDataSeriesSentRecievedSavePath <-   c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/lvtsTXNsSentRecievedByFIDailySeries",startYr,"-",endYr,".Rdata", sep = "", collapse = NULL));
    LVTSTXNsDataSeriesBySenderSTATASavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/lvtsTXNsDailyBySenderSTATASeries",startYr,"-",endYr,".dta", sep = "", collapse = NULL));
    LVTSTXNsDataSeriesSTATASavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/lvtsTXNsDailySTATASeries",startYr,"-",endYr,".dta", sep = "", collapse = NULL));
    LVTSTXNsDataSeriesSentRecievedSTATASavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/lvtsTXNsSentRecievedByFIDailySTATASeries",startYr,"-",endYr,".dta", sep = "", collapse = NULL));
    LVTSTXNsDataSeriesSentRecievedXLSSavePath <-  c(paste("E:/Projects/FundingDataTables/STATADataTables/lvtsTXNsSentRecievedByFIXLSDailySeries",startYr,"-",endYr,".csv", sep = "", collapse = NULL));
    
    LVTSTXNsDataSeriesTranche1SavePath <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/dailyLVTSTXNsTranche1DataSeries",startYr,"-",endYr,".Rdata", sep = "", collapse = NULL));
    LVTSTXNsDataSeriesTranche2SavePath <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/dailyLVTSTXNsTranche2DataSeries",startYr,"-",endYr,".Rdata", sep = "", collapse = NULL));
    
  } else{
    mergedLVTSTXNsNameString <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/summaryDailyLVTSTrancheTXNs",startYr,"-",endYr,"DataSeries.Rdata", sep = "", collapse = NULL));
    mergedLVTSTXNsXLSNameString <- c(paste("E:/Projects/FundingDataTables/STATADataTables/summaryDailyLVTSTrancheTXNs",startYr,"-",endYr,"DataSeries.csv", sep = "", collapse = NULL));
    
    ##Intput Load Path
    extractedSummaryLVTSDataLoadPath <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/summaryDailyLVTSTrancheTXNs",startYr,"-",endYr,"DataSeries.Rdata", sep = "", collapse = NULL));
    
    ##Output Save Paths
    LVTSTXNsDataSeriesSavePath <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/dailyLVTSTrancheTXNsDataSeries",startYr,"-",endYr,".Rdata", sep = "", collapse = NULL));
    LVTSTXNsDataSeriesSentRecievedSavePath <-   c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/lvtsTrancheTXNsSentRecievedByFIDailySeries",startYr,"-",endYr,".Rdata", sep = "", collapse = NULL));
    LVTSTXNsDataSeriesBySenderSTATASavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/lvtsTrancheTXNsDailyBySenderSTATASeries",startYr,"-",endYr,".dta", sep = "", collapse = NULL));
    LVTSTXNsDataSeriesSTATASavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/lvtsTrancheTXNsDailySTATASeries",startYr,"-",endYr,".dta", sep = "", collapse = NULL));
    LVTSTXNsDataSeriesSentRecievedSTATASavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/lvtsTrancheTXNsSentRecievedByFIDailySTATASeries",startYr,"-",endYr,".dta", sep = "", collapse = NULL));
    LVTSTXNsDataSeriesSentRecievedXLSSavePath <-  c(paste("E:/Projects/FundingDataTables/STATADataTables/lvtsTrancheTXNsSentRecievedByFIXLSDailySeries",startYr,"-",endYr,".csv", sep = "", collapse = NULL));
    
    LVTSTXNsDataSeriesTranche1SavePath <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/dailyLVTSTXNsTranche1DataSeries",startYr,"-",endYr,".Rdata", sep = "", collapse = NULL));
    LVTSTXNsDataSeriesTranche2SavePath <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/dailyLVTSTXNsTranche2DataSeries",startYr,"-",endYr,".Rdata", sep = "", collapse = NULL));
    
  }
  
  
}else if(dataFrequency == "dateTime"){
  
  if(byTranche == FALSE){
    mergedLVTSTXNsNameString <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/summaryDateTimeLVTSTXNs",startYr,"-",endYr,"DataSeries.Rdata", sep = "", collapse = NULL));
    mergedLVTSTXNsXLSNameString <- c(paste("E:/Projects/FundingDataTables/STATADataTables/summaryDateTimeLVTSTXNs",startYr,"-",endYr,"DataSeries.csv", sep = "", collapse = NULL));
    mergedLVTSTXNsNameSegmentString <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/summaryDateTimeLVTSTXNs",segStartYr,"-",segEndYr,"DataSeries.Rdata", sep = "", collapse = NULL));

    ##Intput Load Path
    #extractedSummaryLVTSDataLoadPath <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/summaryDateTimeLVTSTXNs",startYr,"-",endYr,"DataSeries.Rdata", sep = "", collapse = NULL));
    extractedSummaryLVTSDataLoadPath <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/summaryDateTimeLVTSTXNs",segStartYr,"-",segEndYr,"DataSeries.Rdata", sep = "", collapse = NULL));
    
    ##Output Save Paths
#    LVTSTXNsDataSeriesSavePath <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/dateTimeLVTSTXNsDataSeries",startYr,"-",endYr,".Rdata", sep = "", collapse = NULL));
#    LVTSTXNsDataSeriesSentRecievedSavePath <-   c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/lvtsTXNsSentRecievedByFIDateTimeSeries",startYr,"-",endYr,".Rdata", sep = "", collapse = NULL));
#    LVTSTXNsDataSeriesBySenderSTATASavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/lvtsTXNsDateTimeBySenderSTATASeries",startYr,"-",endYr,".dta", sep = "", collapse = NULL));
#    LVTSTXNsDataSeriesSTATASavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/lvtsTXNsDateTimeSTATASeries",startYr,"-",endYr,".dta", sep = "", collapse = NULL));
#    LVTSTXNsDataSeriesSentRecievedSTATASavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/lvtsTXNsSentRecievedByFIDateTimeSTATASeries",startYr,"-",endYr,".dta", sep = "", collapse = NULL));
#    LVTSTXNsDataSeriesSentRecievedXLSSavePath <-  c(paste("E:/Projects/FundingDataTables/STATADataTables/lvtsTXNsSentRecievedByFIXLSDateTimeSeries",startYr,"-",endYr,".csv", sep = "", collapse = NULL));
    
#    LVTSTXNsDataSeriesTranche1SavePath <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/dateTimeLVTSTXNsTranche1DataSeries",startYr,"-",endYr,".Rdata", sep = "", collapse = NULL));
#    LVTSTXNsDataSeriesTranche2SavePath <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/dateTimeLVTSTXNsTranche2DataSeries",startYr,"-",endYr,".Rdata", sep = "", collapse = NULL));

    
    LVTSTXNsDataSeriesSavePath <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/dateTimeLVTSTXNsDataSeries",segStartYr,"-",segEndYr,".Rdata", sep = "", collapse = NULL));
    LVTSTXNsDataSeriesSentRecievedSavePath <-   c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/lvtsTXNsSentRecievedByFIDateTimeSeries",segStartYr,"-",segEndYr,".Rdata", sep = "", collapse = NULL));
    LVTSTXNsDataSeriesBySenderSTATASavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/lvtsTXNsDateTimeBySenderSTATASeries",segStartYr,"-",segEndYr,".dta", sep = "", collapse = NULL));
    LVTSTXNsDataSeriesSTATASavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/lvtsTXNsDateTimeSTATASeries",segStartYr,"-",segEndYr,".dta", sep = "", collapse = NULL));
    LVTSTXNsDataSeriesSentRecievedSTATASavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/lvtsTXNsSentRecievedByFIDateTimeSTATASeries",segStartYr,"-",segEndYr,".dta", sep = "", collapse = NULL));
    LVTSTXNsDataSeriesSentRecievedXLSSavePath <-  c(paste("E:/Projects/FundingDataTables/STATADataTables/lvtsTXNsSentRecievedByFIXLSDateTimeSeries",segStartYr,"-",segEndYr,".csv", sep = "", collapse = NULL));
    
    LVTSTXNsDataSeriesTranche1SavePath <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/dateTimeLVTSTXNsTranche1DataSeries",segStartYr,"-",segEndYr,".Rdata", sep = "", collapse = NULL));
    LVTSTXNsDataSeriesTranche2SavePath <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/dateTimeLVTSTXNsTranche2DataSeries",segStartYr,"-",segEndYr,".Rdata", sep = "", collapse = NULL));
    
    
        
  } else {
    mergedLVTSTXNsNameString <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/summaryDateTimeLVTSTrancheTXNs",startYr,"-",endYr,"DataSeries.Rdata", sep = "", collapse = NULL));
    mergedLVTSTXNsXLSNameString <- c(paste("E:/Projects/FundingDataTables/STATADataTables/summaryDateTimeLVTSTrancheTXNs",startYr,"-",endYr,"DataSeries.csv", sep = "", collapse = NULL));
    mergedLVTSTXNsNameSegmentString <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/summaryDateTimeLVTSTrancheTXNs",segStartYr,"-",segEndYr,"DataSeries.Rdata", sep = "", collapse = NULL));
    
    
    ##Intput Load Path
    #extractedSummaryLVTSDataLoadPath <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/summaryDateTimeLVTSTrancheTXNs",startYr,"-",endYr,"DataSeries.Rdata", sep = "", collapse = NULL));
    
    ##Output Save Paths
    #LVTSTXNsDataSeriesSavePath <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/dateTimeLVTSTrancheTXNsDataSeries",startYr,"-",endYr,".Rdata", sep = "", collapse = NULL));
    #LVTSTXNsDataSeriesSentRecievedSavePath <-   c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/lvtsTrancheTXNsSentRecievedByFIDateTimeSeries",startYr,"-",endYr,".Rdata", sep = "", collapse = NULL));
    #LVTSTXNsDataSeriesBySenderSTATASavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/lvtsTrancheTXNsDateTimeBySenderSTATASeries",startYr,"-",endYr,".dta", sep = "", collapse = NULL));
    #LVTSTXNsDataSeriesSTATASavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/lvtsTrancheTXNsDateTimeSTATASeries",startYr,"-",endYr,".dta", sep = "", collapse = NULL));
    #LVTSTXNsDataSeriesSentRecievedSTATASavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/lvtsTrancheTXNsSentRecievedByFIDateTimeSTATASeries",startYr,"-",endYr,".dta", sep = "", collapse = NULL));
    #LVTSTXNsDataSeriesSentRecievedXLSSavePath <-  c(paste("E:/Projects/FundingDataTables/STATADataTables/lvtsTrancheTXNsSentRecievedByFIXLSDateTimeSeries",startYr,"-",endYr,".csv", sep = "", collapse = NULL));
    
    #LVTSTXNsDataSeriesTranche1SavePath <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/dateTimeLVTSTXNsTranche1DataSeries",startYr,"-",endYr,".Rdata", sep = "", collapse = NULL));
    #LVTSTXNsDataSeriesTranche2SavePath <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/dateTimeLVTSTXNsTranche2DataSeries",startYr,"-",endYr,".Rdata", sep = "", collapse = NULL));

    
    ##Intput Load Path
    extractedSummaryLVTSDataLoadPath <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/summaryDateTimeLVTSTrancheTXNs",segStartYr,"-",segEndYr,"DataSeries.Rdata", sep = "", collapse = NULL));
    
    ##Output Save Paths
    LVTSTXNsDataSeriesSavePath <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/dateTimeLVTSTrancheTXNsDataSeries",segStartYr,"-",segEndYr,".Rdata", sep = "", collapse = NULL));
    LVTSTXNsDataSeriesSentRecievedSavePath <-   c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/lvtsTrancheTXNsSentRecievedByFIDateTimeSeries",segStartYr,"-",segEndYr,".Rdata", sep = "", collapse = NULL));
    LVTSTXNsDataSeriesBySenderSTATASavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/lvtsTrancheTXNsDateTimeBySenderSTATASeries",segStartYr,"-",segEndYr,".dta", sep = "", collapse = NULL));
    LVTSTXNsDataSeriesSTATASavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/lvtsTrancheTXNsDateTimeSTATASeries",segStartYr,"-",segEndYr,".dta", sep = "", collapse = NULL));
    LVTSTXNsDataSeriesSentRecievedSTATASavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/lvtsTrancheTXNsSentRecievedByFIDateTimeSTATASeries",segStartYr,"-",segEndYr,".dta", sep = "", collapse = NULL));
    LVTSTXNsDataSeriesSentRecievedXLSSavePath <-  c(paste("E:/Projects/FundingDataTables/STATADataTables/lvtsTrancheTXNsSentRecievedByFIXLSDateTimeSeries",segStartYr,"-",segEndYr,".csv", sep = "", collapse = NULL));
    
    LVTSTXNsDataSeriesTranche1SavePath <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/dateTimeLVTSTXNsTranche1DataSeries",segStartYr,"-",segEndYr,".Rdata", sep = "", collapse = NULL));
    LVTSTXNsDataSeriesTranche2SavePath <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/dateTimeLVTSTXNsTranche2DataSeries",segStartYr,"-",segEndYr,".Rdata", sep = "", collapse = NULL));
    
    
        
  }

}



###Load used functions
gc();
print("Loading Used Functions");
source('G:/Development Workspaces/R-Workspace/DataCollection/post_processing_file_functions_V01.R', encoding = 'UTF-8', echo=TRUE);
gc();
##Run Level 1 post porocessing
##NOTE: Save paths are located in the source/script files
gc();
print("Commencing Level 1 post processing");
source('G:/Development Workspaces/R-Workspace/DataCollection/lvts_post_processing_file_Level_1_V03.R', encoding = 'UTF-8', echo=TRUE);
print("Level 1 post processing complete");
gc();
##Run Level 2 post porocessing 
gc();
print("Commencing Level 2 post processing");
source('G:/Development Workspaces/R-Workspace/DataCollection/lvts_post_processing_file_Level_2_V07.R', encoding = 'UTF-8', echo=TRUE);
print("Level 2 post processing complete");
gc();
