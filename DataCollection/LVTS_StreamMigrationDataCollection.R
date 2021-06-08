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


##<<<<<<<<<<<<<<<<<<<<<<<<<DECLARE GLOBAL VARIABLES>>>>>>>>>>>>>>>>>>>>>>>>
##Declare all global variables to be used in this class

##<<<<<<<<<<<<<<<<<<<<<<<<<DECLARE GLOBAL VARIABLES>>>>>>>>>>>>>>>>>>>>>>>>
##Declare all global variables to be used in this class
startYr <- 2000;
endYr <- 2020;
segStartYr <- 2006;
segEndYr <- 2020;

mergedLVTSTXNsNameString <- NULL;

dataFrequency <- "daily"; #the options are monthly, quarterly, annual, daily, dateTime (does not make sense since it does not distingush the year)
dateTimeNettingFrequency <- "1 hour"; #This is used only if the dataFrequency is dateTime.
#Available options are "x min", "x hour", "x day" etc where x is the integer value of the interval
byTranche <- FALSE; #FALSE TRUE

byStream <- TRUE;

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
extractedLVTSDataLoadPath <- c(paste("E:/Projects/FundingDataTables/Raw Transactions Data/YearsSpecificLVTSTXNsYM",startYr,"-",endYr,
                                     "DataSeries.Rdata", sep = "", collapse = NULL));
fiStaticDataLoadPath <- c("E:/Projects/FundingDataTables/FIStaticData.Rdata");#will load the data table: fiStaticDataFile
LVTSStreamDataLoadPath <- c("E:/Projects/FundingDataTables/LVTSStreamData.Rdata");#will load the data table: ACSSStreamDataFile
streamMigrationLVTSMT103TXNsDataSavePath <- c(paste("E:/Projects/FundingDataTables/Raw Transactions Data/mergedLVTSMT103TXNs",startYr,
                                                    "-",segEndYr,".Rdata", sep = "", collapse = NULL));

streamMigrationLVTSMT205TXNsDataSavePath <- c(paste("E:/Projects/FundingDataTables/Raw Transactions Data/mergedLVTSMT205TXNs",startYr,
                                                    "-",segEndYr,".Rdata", sep = "", collapse = NULL));

LVTSStreamsInPlayRawDataSavePath <- 
  c(paste("E:/Projects/FundingDataTables/Raw Transactions Data/mergedLVTSMT103TXNs",
          segStartYr,"-",segEndYr,".RData", sep = "", collapse = NULL)); 

mergedLVTSTXNsNoBoCRawDataSavePath <- 
  c(paste("E:/Projects/FundingDataTables/Raw Transactions Data/mergedLVTSTXNsNoBoC",
          segStartYr,"-",segEndYr,".RData", sep = "", collapse = NULL)); 

mergedLVTSTXNsBoCOnlyRawDataSavePath <- 
  c(paste("E:/Projects/FundingDataTables/Raw Transactions Data/mergedLVTSTXNsBoCOnly",
          segStartYr,"-",segEndYr,".RData", sep = "", collapse = NULL)); 

mergedLVTSTXNsNoBoCKnownStreamsRawDataSavePath <- 
  c(paste("E:/Projects/FundingDataTables/Raw Transactions Data/mergedLVTSTXNsNoBoCStreamKnown",
          "2013","-",segEndYr,".RData", sep = "", collapse = NULL)); 





###################<<<<<<<<<<<<<<<<<<<<<<<<CSV File Save Paths DATA>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>#########################

streamMigrationLVTSMT103TXNsDataCSVSavePath <- c(paste("E:/Projects/FundingDataTables/Raw Transactions Data/mergedLVTSMT103TXNs",startYr,
                                                    "-",segEndYr,".csv", sep = "", collapse = NULL));

streamMigrationLVTSMT205TXNsDataCSVSavePath <- c(paste("E:/Projects/FundingDataTables/Raw Transactions Data/mergedLVTSMT205TXNs",startYr,
                                                    "-",segEndYr,".csv", sep = "", collapse = NULL));

LVTSStreamsInPlayRawDataCSVSavePath <- 
  c(paste("E:/Projects/FundingDataTables/Raw Transactions Data/mergedLVTSMT103TXNs",
          segStartYr,"-",segEndYr,".csv", sep = "", collapse = NULL)); 

mergedLVTSTXNsNoBoCRawDataCSVSavePath <- 
  c(paste("E:/Projects/FundingDataTables/Raw Transactions Data/mergedLVTSTXNsNoBoC",
          segStartYr,"-",segEndYr,".csv", sep = "", collapse = NULL)); 

mergedLVTSTXNsBoCOnlyRawDataCSVSavePath <- 
  c(paste("E:/Projects/FundingDataTables/Raw Transactions Data/mergedLVTSTXNsBoCOnly",
          segStartYr,"-",segEndYr,".csv", sep = "", collapse = NULL)); 

mergedLVTSTXNsNoBoCKnownStreamsRawDataCSVSavePath <- 
  c(paste("E:/Projects/FundingDataTables/Raw Transactions Data/mergedLVTSTXNsNoBoCStreamKnown",
          "2013","-",segEndYr,".csv", sep = "", collapse = NULL)); 



###################<<<<<<<<<<<<<<<<<<<<<<<<START LOADING DATA>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>#########################
load(extractedLVTSDataLoadPath);

#exclude unrequired mergedLVTSTXNsYearsSpecYM
mergedLVTSTXNsYearsSpecYM <- mergedLVTSTXNsYearsSpecYM[ ,c("MonthlyCountRec", "MonthlyCount","QuarterlyCountRec", "QuarterlyCount", 
                                                           "AnnualCountRec", "AnnualCount", "DailyCountRec", "DailyCount") := NULL];


##Compress data to analysis date range 
gc();
mergedLVTSTXNsAllFIs <- mergedLVTSTXNsYearsSpecYM[year(mergedLVTSTXNsYearsSpecYM$date.time)>=segStartYr & 
                                                    year(mergedLVTSTXNsYearsSpecYM$date.time)<=segEndYr,];
gc();
mergedLVTSTXNsAllFIs <- mergedLVTSTXNsAllFIs[ ,c("Year_Month", "Year_Quarter") := NULL];
mergedLVTSAllFIs103TXNs <- mergedLVTSTXNsAllFIs[stream!="MT205",];
mergedLVTSAllFIs205TXNs <- mergedLVTSTXNsAllFIs[stream!="MT103",];
mergedLVTSAllFIs103TXNsSent <- mergedLVTSAllFIs103TXNs;
mergedLVTSAllFIs103TXNsReceived <- mergedLVTSAllFIs103TXNs;

mergedLVTSAllFIs103TXNs_df <- as.data.frame(mergedLVTSAllFIs103TXNs);
mergedLVTSAllFIs205TXNs_df <- as.data.frame(mergedLVTSAllFIs205TXNs);


mergedLVTSAllFIs103TXNsAnnualSent_df <-
  mergedLVTSAllFIs103TXNs_df %>% 
  group_by(Year, sender) %>%
  summarise(VolumeSent = n(), ValueSent=sum(amount));

mergedLVTSAllFIs103TXNsAnnualReceived_df <-
  mergedLVTSAllFIs103TXNs_df %>% 
  group_by(Year, receiver) %>%
  summarise(VolumeReceived = n(), ValueReceived=sum(amount));

names(mergedLVTSAllFIs103TXNsAnnualReceived_df)[names(mergedLVTSAllFIs103TXNsAnnualReceived_df) == "receiver"] <- "sender";

mergedLVTSAllFIs103TXNsAnnual_df <- merge(mergedLVTSAllFIs103TXNsAnnualSent_df, 
                      mergedLVTSAllFIs103TXNsAnnualReceived_df, 
                      by = c("Year","sender"), all.y=TRUE);

mergedLVTSAllFIs103TXNsAnnual <- as.data.table(mergedLVTSAllFIs103TXNsAnnual_df);
mergedLVTSAllFIs103TXNsAnnual[is.na(mergedLVTSAllFIs103TXNsAnnual)] = 0;

mergedLVTSAllFIs103TXNsAnnual <-  mergedLVTSAllFIs103TXNsAnnual[, list(VolumeSent = VolumeSent,
                                                                       ValueSent = ValueSent,
                                                                       VolumeReceived = VolumeReceived,
                                                                       ValueReceived =ValueReceived,
                                                                       TotalVolume=sum(VolumeSent,VolumeReceived),
                                                                       TotalValue=sum(ValueSent,ValueReceived)), 
                                                                by=c("Year","sender")];


##Now to calculate the annual totals for MT205
mergedLVTSAllFIs205TXNsAnnualSent_df <-
  mergedLVTSAllFIs205TXNs_df %>% 
  group_by(Year, sender) %>%
  summarise(VolumeSent = n(), ValueSent=sum(amount));

mergedLVTSAllFIs205TXNsAnnualReceived_df <-
  mergedLVTSAllFIs205TXNs_df %>% 
  group_by(Year, receiver) %>%
  summarise(VolumeReceived = n(), ValueReceived=sum(amount));

names(mergedLVTSAllFIs205TXNsAnnualReceived_df)[names(mergedLVTSAllFIs205TXNsAnnualReceived_df) == "receiver"] <- "sender";

mergedLVTSAllFIs205TXNsAnnual_df <- merge(mergedLVTSAllFIs205TXNsAnnualSent_df, 
                                          mergedLVTSAllFIs205TXNsAnnualReceived_df, 
                                          by = c("Year","sender"), all.y=TRUE);

mergedLVTSAllFIs205TXNsAnnual <- as.data.table(mergedLVTSAllFIs205TXNsAnnual_df);
mergedLVTSAllFIs205TXNsAnnual[is.na(mergedLVTSAllFIs205TXNsAnnual)] = 0;


# mergedLVTSAllFIs205TXNsAnnual <-  mergedLVTSAllFIs205TXNsAnnual[, list(TotalVolume=sum(VolumeSent,VolumeReceived),
#                                                                        TotalValue=sum(ValueSent,ValueReceived)), 
#                                                                 by=c("Year","sender")];

mergedLVTSAllFIs205TXNsAnnual <-  mergedLVTSAllFIs205TXNsAnnual[, list(VolumeSent = VolumeSent,
                                                                       ValueSent = ValueSent,
                                                                       VolumeReceived = VolumeReceived,
                                                                       ValueReceived =ValueReceived,
                                                                       TotalVolume=sum(VolumeSent,VolumeReceived),
                                                                       TotalValue=sum(ValueSent,ValueReceived)), 
                                                                by=c("Year","sender")];

write.csv(mergedLVTSAllFIs103TXNsAnnual, file="mergedLVTSAllFIs103TXNsAnnual.csv");
write.csv(mergedLVTSAllFIs205TXNsAnnual, file="mergedLVTSAllFIs205TXNsAnnual.csv");


##Compress data to exclude BoC payments and payments outside analysis date range 
mergedLVTSTXNsNoBoC <- mergedLVTSTXNsYearsSpecYM[year(mergedLVTSTXNsYearsSpecYM$date.time)>=segStartYr & 
                                                   year(mergedLVTSTXNsYearsSpecYM$date.time)<=segEndYr &
                                                   sender!="BCANCA" & receiver!="BCANCA",];

mergedLVTSTXNsBoCOnly <- mergedLVTSTXNsYearsSpecYM[year(mergedLVTSTXNsYearsSpecYM$date.time)>=segStartYr & 
                                                     year(mergedLVTSTXNsYearsSpecYM$date.time)<=segEndYr &
                                                     (sender=="BCANCA" | receiver=="BCANCA"),];
#remove main data file
gc();
rm(mergedLVTSTXNsYearsSpecYM);
gc();
save(mergedLVTSTXNsNoBoC, file=mergedLVTSTXNsNoBoCRawDataSavePath);
save(mergedLVTSTXNsBoCOnly, file=mergedLVTSTXNsBoCOnlyRawDataSavePath);
write.csv(mergedLVTSTXNsNoBoC, file=mergedLVTSTXNsNoBoCRawDataCSVSavePath);
write.csv(mergedLVTSTXNsBoCOnly, file=mergedLVTSTXNsBoCOnlyRawDataCSVSavePath);

gc();

mergedLVTSMT103TXNs <- mergedLVTSTXNsNoBoC[stream!="MT205",];
mergedLVTSMT205TXNs <- mergedLVTSTXNsNoBoC[stream!="MT103",];

mergedLVTSTXNsNoBoCKnownMT103 <- mergedLVTSTXNsNoBoC[stream=="MT103",];

mergedLVTSTXNsNoBoCKnownMT205 <- mergedLVTSTXNsNoBoC[stream=="MT205",];

gc();
rm(mergedLVTSTXNsNoBoC);
gc();

mergedLVTSTXNsNoBoCStreamKnown <- rbind(mergedLVTSTXNsNoBoCKnownMT103,mergedLVTSTXNsNoBoCKnownMT205);

gc();
rm(mergedLVTSTXNsNoBoCKnownMT103);
rm(mergedLVTSTXNsNoBoCKnownMT205);
gc();



#Save compressed file
save(mergedLVTSTXNsNoBoCStreamKnown, file=mergedLVTSTXNsNoBoCKnownStreamsRawDataSavePath);
save(mergedLVTSMT103TXNs, file=streamMigrationLVTSMT103TXNsDataSavePath);
save(mergedLVTSMT205TXNs, file=streamMigrationLVTSMT205TXNsDataSavePath);

write.csv(mergedLVTSTXNsNoBoCStreamKnown, file=mergedLVTSTXNsNoBoCKnownStreamsRawDataCSVSavePath);
write.csv(mergedLVTSMT103TXNs, file=streamMigrationLVTSMT103TXNsDataCSVSavePath);
write.csv(mergedLVTSMT205TXNs, file=streamMigrationLVTSMT205TXNsDataCSVSavePath);

