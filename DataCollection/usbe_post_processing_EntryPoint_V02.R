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
## Modifications MadC: 05 Dec 2016
##        1) wrote lines to run USBE and LVTS data files processing scripts
##        2) 
##        3)  
##           
##        4)  
##           
##        5) 
##        6)  
## Modified : Segun Bewaji
## Modifications MadC:
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

mergedUSBETXNsNameString <- NULL;
##Set Data Frequency
# dataFrequency <- "quarterly"; #the options are monthly, quarterly, annual, daily, dateTime (does not make sense since it does not distingush the year)
# dateTimeNettingFrequency <- "5 min"; #This is used only if the dataFrequency is dateTime.
#Available options are "x min", "x hour", "x day" etc where x is the integer value of the interval

# byStream <- FALSE;##Note that no stream level analysis can be done on USBE data prior to 2010 due to missing TXN data files

USBETXNsDataSeriesSavePath <- NULL;
USBETXNsDataSeriesSentRecievedSavePath <- NULL;
#will save the data tablC: bicCodeUpdatedUSBETXNsYearsSpec
USBETXNsDataSeriesByPeriodSTATASavePath <- NULL;
USBETXNsDataSeriesBySenderSTATASavePath <- NULL;
USBETXNsDataSeriesSTATASavePath <- NULL;
USBETXNsDataSeriesSentRecievedSTATASavePath <-  NULL;


extractedUSBEDataLoadPath <- NULL;


##NULL declarations
USBETXNsDataSeries <- NULL; #working datatable
USBETXNsDataSeriesBySender <- NULL; #working datatable
USBETXNsDataSeriesBySenderReceiver <- NULL;
USBETXNsDataSeriesByPeriod <- NULL;
USBETXNsDataSeriesSentRecieved <- NULL; 

USBETXNsDataSeriesRecieved_df <-  NULL;
USBETXNsDataSeriesSent_df <-  NULL;
USBETXNsDataSeriesSentRecieved_df <- NULL; 

mergedUSBETXNsYearsSpecYM_df <- NULL; #working USBE data data.frame
fiStaticDataFile_df <- NULL; #working static data data.frame


###################################<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<RAW RDATA FILE PATHS>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
extractedUSBEDataLoadPath <- c(paste("E:/Projects/FundingDataTables/Raw Transactions Data/YearsSpecificUSBETXNsYM",startYr,"-",endYr,"DataSeries.Rdata", sep = "", collapse = NULL));
fiStaticDataLoadPath <- c("E:/Projects/FundingDataTables/FIStaticData.Rdata");#will load the data tablC: fiStaticDataFile
RETAILStreamDataLoadPath <- c("E:/Projects/FundingDataTables/RETAILStreamData.Rdata");#will load the data tablC: USBEStreamDataFile


if(dataFrequency == "monthly"){

  ##THe following file house the USBE data between 2000 and 2010. 
  ##This file has been manually compiled from the Payment Ops monthly reporting files.
  paymentOpsDataFile <-  c("E:/Projects/FundingDataTables/Missing Pre2010 Transactions Data/monthlyUSBETXNsDataSeriesSentRecieved2000-2010.Rdata");
  
  mergedUSBETXNsNameString <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/summaryMonthlyUSBETXNs",startYr,"-",endYr,"DataSeries.Rdata", sep = "", collapse = NULL));
  mergedUSBETXNsXLSNameString <- c(paste("E:/Projects/FundingDataTables/STATADataTables/summaryMonthlyUSBETXNs",startYr,"-",endYr,"DataSeries.csv", sep = "", collapse = NULL));
  
  ##Intput Load Path
  extractedSummaryUSBEDataLoadPath <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/summaryMonthlyUSBETXNs",startYr,"-",endYr,"DataSeries.Rdata", sep = "", collapse = NULL));
  
  ##Output Save Paths
  if(byStream == FALSE){
    USBETXNsDataSeriesSavePath <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/monthlyUSBETXNsDataSeries",startYr,"-",endYr,".Rdata", sep = "", collapse = NULL));
    USBETXNsDataSeriesSentRecievedSavePath <-   c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/usbeTXNsSentRecievedByFIMonthlySeries",startYr,"-",endYr,".Rdata", sep = "", collapse = NULL));
    USBETXNsDataSeriesBySenderSTATASavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/usbeTXNsMonthlyBySenderSTATASeries",startYr,"-",endYr,".dta", sep = "", collapse = NULL));
    USBETXNsDataSeriesBySenderXLSSavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/usbeTXNsMonthlyBySenderSTATASeries",startYr,"-",endYr,".csv", sep = "", collapse = NULL));
    USBETXNsDataSeriesSTATASavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/usbeTXNsMonthlySTATASeries",startYr,"-",endYr,".dta", sep = "", collapse = NULL));
    #USBETXNsDataSeriesByPeriodSTATASavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/usbeTXNsMonthlySTATASeries",startYr,"-",endYr,".dta", sep = "", collapse = NULL));
    USBETXNsDataSeriesSentRecievedSTATASavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/usbeTXNsSentRecievedByFIMonthlySTATASeries",startYr,"-",endYr,".dta", sep = "", collapse = NULL));
    USBETXNsDataSeriesSentRecievedXLSSavePath <-  c(paste("E:/Projects/FundingDataTables/STATADataTables/usbeTXNsSentRecievedByFIXLSMonthlySeries",startYr,"-",endYr,".csv", sep = "", collapse = NULL));
    
  }else {
    USBETXNsDataSeriesSavePath <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/monthlyUSBEStreamTXNsDataSeries",startYr,"-",endYr,".Rdata", sep = "", collapse = NULL));
    USBETXNsDataSeriesSentRecievedSavePath <-   c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/usbeStreamTXNsSentRecievedByFIMonthlySeries",startYr,"-",endYr,".Rdata", sep = "", collapse = NULL));
    USBETXNsDataSeriesBySenderSTATASavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/usbeStreamTXNsMonthlyBySenderSTATASeries",startYr,"-",endYr,".dta", sep = "", collapse = NULL));
    USBETXNsDataSeriesBySenderXLSSavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/usbeStreamTXNsMonthlyBySenderSTATASeries",startYr,"-",endYr,".csv", sep = "", collapse = NULL));
    USBETXNsDataSeriesSTATASavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/usbeStreamTXNsMonthlySTATASeries",startYr,"-",endYr,".dta", sep = "", collapse = NULL));
    #USBETXNsDataSeriesByPeriodSTATASavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/usbeTXNsMonthlySTATASeries",startYr,"-",endYr,".dta", sep = "", collapse = NULL));
    USBETXNsDataSeriesSentRecievedSTATASavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/usbeStreamTXNsSentRecievedByFIMonthlySTATASeries",startYr,"-",endYr,".dta", sep = "", collapse = NULL));
    USBETXNsDataSeriesSentRecievedXLSSavePath <-  c(paste("E:/Projects/FundingDataTables/STATADataTables/usbeStreamTXNsSentRecievedByFIXLSMonthlySeries",startYr,"-",endYr,".csv", sep = "", collapse = NULL));
    
  }
  
  
}else if(dataFrequency == "quarterly"){

  ##THe following file house the USBE data between 2000 and 2010. 
  ##This file has been manually compiled from the Payment Ops monthly reporting files.
  paymentOpsDataFile <-  c("E:/Projects/FundingDataTables/Missing Pre2010 Transactions Data/quarterlyUSBETXNsDataSeriesSentRecieved2000-2010.Rdata");

  mergedUSBETXNsNameString <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/summaryQuarterlyUSBETXNs",startYr,"-",endYr,"DataSeries.Rdata", sep = "", collapse = NULL));
  mergedUSBETXNsXLSNameString <- c(paste("E:/Projects/FundingDataTables/STATADataTables/summaryQuarterlyUSBETXNs",startYr,"-",endYr,"DataSeries.csv", sep = "", collapse = NULL));
  
  ##Intput Load Path
  extractedSummaryUSBEDataLoadPath <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/summaryQuarterlyUSBETXNs",startYr,"-",endYr,"DataSeries.Rdata", sep = "", collapse = NULL));
  
  ##Output Save Paths
  if(byStream == FALSE){
    USBETXNsDataSeriesSavePath <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/quarterlyUSBETXNsDataSeries",startYr,"-",endYr,".Rdata", sep = "", collapse = NULL));
    USBETXNsDataSeriesSentRecievedSavePath <-   c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/usbeTXNsSentRecievedByFIQuarterlySeries",startYr,"-",endYr,".Rdata", sep = "", collapse = NULL));
    USBETXNsDataSeriesBySenderSTATASavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/usbeTXNsQuarterlyBySenderSTATASeries",startYr,"-",endYr,".dta", sep = "", collapse = NULL));
    USBETXNsDataSeriesBySenderXLSSavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/usbeTXNsQuarterlyBySenderSTATASeries",startYr,"-",endYr,".csv", sep = "", collapse = NULL));
    USBETXNsDataSeriesSTATASavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/usbeTXNsQuarterlySTATASeries",startYr,"-",endYr,".dta", sep = "", collapse = NULL));
    USBETXNsDataSeriesSentRecievedSTATASavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/usbeTXNsSentRecievedByFIQuarterlySTATASeries",startYr,"-",endYr,".dta", sep = "", collapse = NULL));
    USBETXNsDataSeriesSentRecievedXLSSavePath <-  c(paste("E:/Projects/FundingDataTables/STATADataTables/usbeTXNsSentRecievedByFIXLSQuarterlySeries",startYr,"-",endYr,".csv", sep = "", collapse = NULL));
    
  }else{
    USBETXNsDataSeriesSavePath <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/quarterlyUSBEStreamTXNsDataSeries",startYr,"-",endYr,".Rdata", sep = "", collapse = NULL));
    USBETXNsDataSeriesSentRecievedSavePath <-   c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/usbeStreamTXNsSentRecievedByFIQuarterlySeries",startYr,"-",endYr,".Rdata", sep = "", collapse = NULL));
    USBETXNsDataSeriesBySenderSTATASavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/usbeStreamTXNsQuarterlyBySenderSTATASeries",startYr,"-",endYr,".dta", sep = "", collapse = NULL));
    USBETXNsDataSeriesBySenderXLSSavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/usbeStreamTXNsQuarterlyBySenderSTATASeries",startYr,"-",endYr,".csv", sep = "", collapse = NULL));
    USBETXNsDataSeriesSTATASavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/usbeStreamTXNsQuarterlySTATASeries",startYr,"-",endYr,".dta", sep = "", collapse = NULL));
    USBETXNsDataSeriesSentRecievedSTATASavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/usbeStreamTXNsSentRecievedByFIQuarterlySTATASeries",startYr,"-",endYr,".dta", sep = "", collapse = NULL));
    USBETXNsDataSeriesSentRecievedXLSSavePath <-  c(paste("E:/Projects/FundingDataTables/STATADataTables/usbeStreamTXNsSentRecievedByFIXLSQuarterlySeries",startYr,"-",endYr,".csv", sep = "", collapse = NULL));
    
  }
  
  
}else if(dataFrequency == "annual"){

  ##THe following file house the USBE data between 2000 and 2010. 
  ##This file has been manually compiled from the Payment Ops monthly reporting files.
  paymentOpsDataFile <-  c("E:/Projects/FundingDataTables/Missing Pre2010 Transactions Data/annualUSBETXNsDataSeriesSentRecieved2000-2010.Rdata");
  
  mergedUSBETXNsNameString <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/summaryAnnualUSBETXNs",startYr,"-",endYr,"DataSeries.Rdata", sep = "", collapse = NULL));
  mergedUSBETXNsXLSNameString <- c(paste("E:/Projects/FundingDataTables/STATADataTables/summaryAnnualUSBETXNs",startYr,"-",endYr,"DataSeries.csv", sep = "", collapse = NULL));
  
  ##Intput Load Path
  extractedSummaryUSBEDataLoadPath <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/summaryAnnualUSBETXNs",startYr,"-",endYr,"DataSeries.Rdata", sep = "", collapse = NULL));
  
  ##Output Save Paths
  if(byStream == FALSE){
    USBETXNsDataSeriesSavePath <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/annualUSBETXNsDataSeries",startYr,"-",endYr,".Rdata", sep = "", collapse = NULL));
    USBETXNsDataSeriesSentRecievedSavePath <-   c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/usbeTXNsSentRecievedByFIAnnualSeries",startYr,"-",endYr,".Rdata", sep = "", collapse = NULL));
    USBETXNsDataSeriesBySenderSTATASavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/usbeTXNsAnnualBySenderSTATASeries",startYr,"-",endYr,".dta", sep = "", collapse = NULL));
    USBETXNsDataSeriesBySenderXLSSavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/usbeTXNsAnnualBySenderSTATASeries",startYr,"-",endYr,".csv", sep = "", collapse = NULL));
    USBETXNsDataSeriesSTATASavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/usbeTXNsAnnualSTATASeries",startYr,"-",endYr,".dta", sep = "", collapse = NULL));
    USBETXNsDataSeriesSentRecievedSTATASavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/usbeTXNsSentRecievedByFIAnnualSTATASeries",startYr,"-",endYr,".dta", sep = "", collapse = NULL));
    USBETXNsDataSeriesSentRecievedXLSSavePath <-  c(paste("E:/Projects/FundingDataTables/STATADataTables/usbeTXNsSentRecievedByFIXLSAnnualSeries",startYr,"-",endYr,".csv", sep = "", collapse = NULL));
  }else{
    USBETXNsDataSeriesSavePath <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/annualUSBEStreamTXNsDataSeries",startYr,"-",endYr,".Rdata", sep = "", collapse = NULL));
    USBETXNsDataSeriesSentRecievedSavePath <-   c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/usbeStreamTXNsSentRecievedByFIAnnualSeries",startYr,"-",endYr,".Rdata", sep = "", collapse = NULL));
    USBETXNsDataSeriesBySenderSTATASavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/usbeStreamTXNsAnnualBySenderSTATASeries",startYr,"-",endYr,".dta", sep = "", collapse = NULL));
    USBETXNsDataSeriesBySenderXLSSavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/usbeStreamTXNsAnnualBySenderSTATASeries",startYr,"-",endYr,".csv", sep = "", collapse = NULL));
    USBETXNsDataSeriesSTATASavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/usbeStreamTXNsAnnualSTATASeries",startYr,"-",endYr,".dta", sep = "", collapse = NULL));
    USBETXNsDataSeriesSentRecievedSTATASavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/usbeStreamTXNsSentRecievedByFIAnnualSTATASeries",startYr,"-",endYr,".dta", sep = "", collapse = NULL));
    USBETXNsDataSeriesSentRecievedXLSSavePath <-  c(paste("E:/Projects/FundingDataTables/STATADataTables/usbeStreamTXNsSentRecievedByFIXLSAnnualSeries",startYr,"-",endYr,".csv", sep = "", collapse = NULL));
  }
  
  
}else if(dataFrequency == "daily"){

   mergedUSBETXNsNameString <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/summaryDailyUSBETXNs",startYr,"-",endYr,"DataSeries.Rdata", sep = "", collapse = NULL));
   mergedUSBETXNsXLSNameString <- c(paste("E:/Projects/FundingDataTables/STATADataTables/summaryDailyUSBETXNs",startYr,"-",endYr,"DataSeries.csv", sep = "", collapse = NULL));

   ##Intput Load Path
   #extractedSummaryUSBEDataLoadPath <- c(paste("E:/Projects/FundingDataTables/YearsSpecificUSBETXNsYM",startYr,"-",endYr,"DataSeries.Rdata", sep = "", collapse = NULL));
   extractedSummaryUSBEDataLoadPath <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/summaryDailyUSBETXNs",startYr,"-",endYr,"DataSeries.Rdata", sep = "", collapse = NULL));
   
   ##Output Save Paths
   if(byStream == FALSE){
     USBETXNsDataSeriesSavePath <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/dailyUSBETXNsDataSeries",startYr,"-",endYr,".Rdata", sep = "", collapse = NULL));
     USBETXNsDataSeriesSentRecievedSavePath <-   c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/usbeTXNsSentRecievedByFIDailySeries",startYr,"-",endYr,".Rdata", sep = "", collapse = NULL));
     USBETXNsDataSeriesBySenderSTATASavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/usbeTXNsDailyBySenderSTATASeries",startYr,"-",endYr,".dta", sep = "", collapse = NULL));
     USBETXNsDataSeriesBySenderXLSSavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/usbeTXNsDailyBySenderSTATASeries",startYr,"-",endYr,".csv", sep = "", collapse = NULL));
     USBETXNsDataSeriesSTATASavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/usbeTXNsDailySTATASeries",startYr,"-",endYr,".dta", sep = "", collapse = NULL));
     USBETXNsDataSeriesSentRecievedSTATASavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/usbeTXNsSentRecievedByFIDailySTATASeries",startYr,"-",endYr,".dta", sep = "", collapse = NULL));
     USBETXNsDataSeriesSentRecievedXLSSavePath <-  c(paste("E:/Projects/FundingDataTables/STATADataTables/usbeTXNsSentRecievedByFIXLSDailySeries",startYr,"-",endYr,".csv", sep = "", collapse = NULL));
   }else{
     USBETXNsDataSeriesSavePath <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/dailyUSBEStreamTXNsDataSeries",startYr,"-",endYr,".Rdata", sep = "", collapse = NULL));
     USBETXNsDataSeriesSentRecievedSavePath <-   c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/usbeStreamTXNsSentRecievedByFIDailySeries",startYr,"-",endYr,".Rdata", sep = "", collapse = NULL));
     USBETXNsDataSeriesBySenderSTATASavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/usbeStreamTXNsDailyBySenderSTATASeries",startYr,"-",endYr,".dta", sep = "", collapse = NULL));
     USBETXNsDataSeriesBySenderXLSSavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/usbeStreamTXNsDailyBySenderSTATASeries",startYr,"-",endYr,".csv", sep = "", collapse = NULL));
     USBETXNsDataSeriesSTATASavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/usbeStreamTXNsDailySTATASeries",startYr,"-",endYr,".dta", sep = "", collapse = NULL));
     USBETXNsDataSeriesSentRecievedSTATASavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/usbeStreamTXNsSentRecievedByFIDailySTATASeries",startYr,"-",endYr,".dta", sep = "", collapse = NULL));
     USBETXNsDataSeriesSentRecievedXLSSavePath <-  c(paste("E:/Projects/FundingDataTables/STATADataTables/usbeStreamTXNsSentRecievedByFIXLSDailySeries",startYr,"-",endYr,".csv", sep = "", collapse = NULL));
   }
   
     
}else if(dataFrequency == "dateTime"){

  mergedUSBETXNsNameString <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/summaryDateTimeUSBETXNs",startYr,"-",endYr,"DataSeries.Rdata", sep = "", collapse = NULL));
  mergedUSBETXNsXLSNameString <- c(paste("E:/Projects/FundingDataTables/STATADataTables/summaryDateTimeUSBETXNs",startYr,"-",endYr,"DataSeries.csv", sep = "", collapse = NULL));
  
  ##Intput Load Path
  extractedSummaryUSBEDataLoadPath <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/summaryDateTimeUSBETXNs",startYr,"-",endYr,"DataSeries.Rdata", sep = "", collapse = NULL));
  
  ##Output Save Paths
  if(byStream == FALSE){
    USBETXNsDataSeriesSavePath <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/dateTimeUSBETXNsDataSeries",startYr,"-",endYr,".Rdata", sep = "", collapse = NULL));
    USBETXNsDataSeriesSentRecievedSavePath <-   c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/usbeTXNsSentRecievedByFIDateTimeSeries",startYr,"-",endYr,".Rdata", sep = "", collapse = NULL));
    USBETXNsDataSeriesBySenderSTATASavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/usbeTXNsDateTimeBySenderSTATASeries",startYr,"-",endYr,".dta", sep = "", collapse = NULL));
    USBETXNsDataSeriesBySenderXLSSavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/usbeTXNsDateTimeBySenderSTATASeries",startYr,"-",endYr,".csv", sep = "", collapse = NULL));
    USBETXNsDataSeriesSTATASavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/usbeTXNsDateTimeSTATASeries",startYr,"-",endYr,".dta", sep = "", collapse = NULL));
    USBETXNsDataSeriesSentRecievedSTATASavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/usbeTXNsSentRecievedByFIDateTimeSTATASeries",startYr,"-",endYr,".dta", sep = "", collapse = NULL));
    USBETXNsDataSeriesSentRecievedXLSSavePath <-  c(paste("E:/Projects/FundingDataTables/STATADataTables/usbeTXNsSentRecievedByFIXLSDateTimeSeries",startYr,"-",endYr,".csv", sep = "", collapse = NULL));
    
  }else{
    USBETXNsDataSeriesSavePath <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/dateTimeUSBEStreamTXNsDataSeries",startYr,"-",endYr,".Rdata", sep = "", collapse = NULL));
    USBETXNsDataSeriesSentRecievedSavePath <-   c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/usbeStreamTXNsSentRecievedByFIDateTimeSeries",startYr,"-",endYr,".Rdata", sep = "", collapse = NULL));
    USBETXNsDataSeriesBySenderSTATASavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/usbeStreamTXNsDateTimeBySenderSTATASeries",startYr,"-",endYr,".dta", sep = "", collapse = NULL));
    USBETXNsDataSeriesBySenderXLSSavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/usbeStreamTXNsDateTimeBySenderSTATASeries",startYr,"-",endYr,".csv", sep = "", collapse = NULL));
    USBETXNsDataSeriesSTATASavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/usbeStreamTXNsDateTimeSTATASeries",startYr,"-",endYr,".dta", sep = "", collapse = NULL));
    USBETXNsDataSeriesSentRecievedSTATASavePath <- c(paste("E:/Projects/FundingDataTables/STATADataTables/usbeStreamTXNsSentRecievedByFIDateTimeSTATASeries",startYr,"-",endYr,".dta", sep = "", collapse = NULL));
    USBETXNsDataSeriesSentRecievedXLSSavePath <-  c(paste("E:/Projects/FundingDataTables/STATADataTables/usbeStreamTXNsSentRecievedByFIXLSDateTimeSeries",startYr,"-",endYr,".csv", sep = "", collapse = NULL));
  }
  
}



###Load used functions
# gc();
# print("Loading Used Functions");
# source('G:/Development Workspaces/R-Workspace/DataCollection/post_processing_file_functions_V01.R', encoding = 'UTF-8', echo=TRUE);
# gc();
##Run Level 1 post porocessing
##NOTC: Save paths are located in the source/script files
gc();
print("Commencing Level 1 post processing");
source('G:/Development Workspaces/R-Workspace/DataCollection/usbe_post_processing_file_Level_1_V02.R', encoding = 'UTF-8', echo=TRUE);
print("Level 1 post processing complete");
gc();
##Run Level 2 post porocessing 
gc();
print("Commencing Level 2 post processing");
source('G:/Development Workspaces/R-Workspace/DataCollection/usbe_post_processing_file_Level_2_V05.R', encoding = 'UTF-8', echo=TRUE);
print("Level 2 post processing complete");
gc();
