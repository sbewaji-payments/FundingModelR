
##This script acts to merge both the ACSS and LVTS transaction data extracts. 
##The generated file will be used to provide finance with the sumarry details for each FI's sending an recieving habits
##
##Final input data tables that the code will work with are 
##        a: mergedACSSTransYearsSpecYM - The ACSS data file
##        b: fiStaticDataFile - The static data file
##
##
##
## Author : Segun Bewaji
## Creation Date : 18 Feb 2016
## Modified : Segun Bewaji
## Modifications Made: 
##        1) fixed factor convertion bug lines 184-194 should have used the data.frame object FILevelTXNsDataSeriesSentRecieved_df 
##           NOT FILevelTXNsDataSeriesSentRecieved
##        2) included line 174-181 to account for FI level transaction volume and value shares
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


##<<<<<<<<<<<<<<<<<<<<<<<<<DECLARE GLOBAL VARIABLES>>>>>>>>>>>>>>>>>>>>>>>>
##Declare all global variables to be used in this class
dataFrequency <- "monthly"; #the valid options are monthly, quarterly, annual, daily (does not make sense since it does not distingush the year)


extractedLVTSDataLoadPath <- NULL;
extractedACSSDataLoadPath <- NULL;
FILevelTXNsDataSeriesSentRecievedSavePath <- NULL;
FILevelTXNsDataSeriesSentRecievedSTATASavePath <- NULL;
FILevelTXNsDataSeriesSentRecievedEXCELSavePath <- NULL;
LVTSTransDataSeriesSentRecieved_df <- NULL; 
ACSSTransDataSeriesSentRecieved_df <- NULL;
FILevelTXNsDataSeriesSentRecieved_df <- NULL;
FILevelTXNsDataSeriesSentRecieved <- NULL;


#set paths for data load: 
#if monthly
if(dataFrequency == "monthly"){
  extractedACSSDataLoadPath <- c("C:/Projects/FundingDataTables/acssTransSentRecievedByFIMonthlySeries.Rdata");
  extractedLVTSDataLoadPath <- c("C:/Projects/FundingDataTables/lvtsTransSentRecievedByFIMonthlySeries.Rdata");
  #extractedACSSAggregationDataLoadPath <- c("C:/Projects/FundingDataTables/monthlyACSSTransDataSeries.Rdata");
  #extractedLVTSAggregationDataLoadPath <- c("C:/Projects/FundingDataTables/monthlyLVTSTransDataSeries.Rdata");
  FILevelTXNsDataSeriesSentRecievedSavePath <-  c("C:/Projects/FundingDataTables/FILevelTXNsSentRecievedByFIMonthlySeries.Rdata");
  FILevelTXNsDataSeriesSentRecievedSTATASavePath <-   c("C:/Projects/FundingDataTables/STATADataTables/FILevelTXNsSentRecievedByFIMonthlySTATASeries.dta");
  FILevelTXNsDataSeriesSentRecievedEXCELSavePath <-   c("C:/Projects/FundingDataTables/STATADataTables/FILevelTXNsSentRecievedByFIMonthlySeries.xls");
  AggregationLevelTXNsDataSeriesSavePath <- c("C:/Projects/FundingDataTables/AggregationLevelTXNsMonthlySeries.Rdata");
  AggregationLevelTXNsDataSeriesSTATASavePath <-  c("C:/Projects/FundingDataTables/STATADataTables/AggregationLevelTXNsMonthlySTATASeries.dta");
  AggregationLevelTXNsDataSeriesEXCELSavePath <-  c("C:/Projects/FundingDataTables/STATADataTables/AggregationLevelTXNsMonthlySeries.xls");
  #if quarterly  
}else if(dataFrequency == "quarterly"){
  extractedACSSDataLoadPath <- c("C:/Projects/FundingDataTables/acssTransSentRecievedByFIQuarterlySeries.Rdata");
  extractedLVTSDataLoadPath <- c("C:/Projects/FundingDataTables/lvtsTransSentRecievedByFIQuarterlySeries.Rdata");
  #extractedACSSAggregationDataLoadPath <- c("C:/Projects/FundingDataTables/quarterlyACSSTransDataSeries.Rdata");
  #extractedLVTSAggregationDataLoadPath <- c("C:/Projects/FundingDataTables/quarterlyLVTSTransDataSeries.Rdata");
  FILevelTXNsDataSeriesSentRecievedSavePath <- c("C:/Projects/FundingDataTables/FILevelTXNsSentRecievedByFIQuarterlySeries.Rdata");
  FILevelTXNsDataSeriesSentRecievedSTATASavePath <-  c("C:/Projects/FundingDataTables/STATADataTables/FILevelTXNsSentRecievedByFIQuarterlySTATASeries.dta");
  FILevelTXNsDataSeriesSentRecievedEXCELSavePath <-  c("C:/Projects/FundingDataTables/STATADataTables/FILevelTXNsSentRecievedByFIQuarterlySeries.xls");
  AggregationLevelTXNsDataSeriesSavePath <- c("C:/Projects/FundingDataTables/AggregationLevelTXNsQuarterlySeries.Rdata");
  AggregationLevelTXNsDataSeriesSTATASavePath <-  c("C:/Projects/FundingDataTables/STATADataTables/AggregationLevelTXNsQuarterlySTATASeries.dta");
  AggregationLevelTXNsDataSeriesEXCELSavePath <-  c("C:/Projects/FundingDataTables/STATADataTables/AggregationLevelTXNsQuarterlySeries.xls");
  #if annual 
}else if(dataFrequency == "annual"){
  extractedACSSDataLoadPath <- c("C:/Projects/FundingDataTables/acssTransSentRecievedByFIAnnualSeries.Rdata");
  extractedLVTSDataLoadPath <- c("C:/Projects/FundingDataTables/lvtsTransSentRecievedByFIAnnualSeries.Rdata");
  #extractedACSSAggregationDataLoadPath <- c("C:/Projects/FundingDataTables/annualACSSTransDataSeries.Rdata");
  #extractedLVTSAggregationDataLoadPath <- c("C:/Projects/FundingDataTables/annualLVTSTransDataSeries.Rdata");
  FILevelTXNsDataSeriesSentRecievedSavePath <- c("C:/Projects/FundingDataTables/FILevelTXNsSentRecievedByFIAnnualSeries.Rdata");
  FILevelTXNsDataSeriesSentRecievedSTATASavePath <-  c("C:/Projects/FundingDataTables/STATADataTables/FILevelTXNsSentRecievedByFIAnnualSTATASeries.dta");
  FILevelTXNsDataSeriesSentRecievedEXCELSavePath <-  c("C:/Projects/FundingDataTables/STATADataTables/FILevelTXNsSentRecievedByFIAnnualSeries.xls");
  AggregationLevelTXNsDataSeriesSavePath <- c("C:/Projects/FundingDataTables/AggregationLevelTXNsAnnualSeries.Rdata");
  AggregationLevelTXNsDataSeriesSTATASavePath <-  c("C:/Projects/FundingDataTables/STATADataTables/AggregationLevelTXNsAnnualSTATASeries.dta");
  AggregationLevelTXNsDataSeriesEXCELSavePath <-  c("C:/Projects/FundingDataTables/STATADataTables/AggregationLevelTXNsAnnualSeries.xls");
  #if daily   
}else if(dataFrequency == "daily"){
  
}




##<<<<<<<<<<<<<<<<<<<<<<<<<LOAD DATA>>>>>>>>>>>>>>>>>>>>>>>>
##Now load the individual FI's complete data tables
print("Loading data tables");#used to record speed of computations
print(now());#used to record speed of computations
load(extractedACSSDataLoadPath);
load(extractedLVTSDataLoadPath);
#load(extractedACSSAggregationDataLoadPath);
#load(extractedLVTSAggregationDataLoadPath);
gc();
print("data tables loaded");#used to record speed of computations
print(now());#used to record speed of computations
gc();


##convert the imported data tables into core R data frames
LVTSTransDataSeriesSentRecieved_df <- as.data.frame(LVTSTransDataSeriesSentRecieved);
ACSSTransDataSeriesSentRecieved_df <- as.data.frame(ACSSTransDataSeriesSentRecieved);
#LVTSTransAggregationDataSeries_df <- as.data.frame(LVTSTransDataSeries);
#ACSSTransAggregationDataSeries_df <- as.data.frame(ACSSTransDataSeries);

##Now merge the data frames into one larger data frame containing all data from both systems.
##In the implementation used here it ias assume that there will always be more particiants in LVTS than in ACSS
if(dataFrequency == "monthly"){
FILevelTXNsDataSeriesSentRecieved_df <-  merge(LVTSTransDataSeriesSentRecieved_df, ACSSTransDataSeriesSentRecieved_df, 
                                               by.x=c("sender","Year_Month"), by.y=c("sender","Year_Month"), all.x = TRUE, all.y = TRUE);

}else if(dataFrequency == "quarterly"){

  FILevelTXNsDataSeriesSentRecieved_df <-  merge(LVTSTransDataSeriesSentRecieved_df, ACSSTransDataSeriesSentRecieved_df, 
                                                 by.x=c("sender","Year_Quarter"), by.y=c("sender","Year_Quarter"), all.x = TRUE, all.y = TRUE);
}else if(dataFrequency == "annual"){
  FILevelTXNsDataSeriesSentRecieved_df <-  merge(LVTSTransDataSeriesSentRecieved_df, ACSSTransDataSeriesSentRecieved_df, 
                                                 by.x=c("sender","Year"), by.y=c("sender","Year"), all.x = TRUE, all.y = TRUE);
  
}else if(dataFrequency == "daily"){
  
}

##Rename Columns
##this is not the best approach but given the assumptions made above about the size of the LVTS data frame relative to that of the ACSS data 
##in terms of the number of particiapants and thus the implementation of the FILevelTXNsDataSeriesSentRecieved_df 
## x will always be LVTS and y will always be ACSS
colnames(FILevelTXNsDataSeriesSentRecieved_df)[colnames(FILevelTXNsDataSeriesSentRecieved_df)=="sender"] <- "member";
colnames(FILevelTXNsDataSeriesSentRecieved_df)[colnames(FILevelTXNsDataSeriesSentRecieved_df)=="SentVolume.x"] <- "SentVolume.LVTS";
colnames(FILevelTXNsDataSeriesSentRecieved_df)[colnames(FILevelTXNsDataSeriesSentRecieved_df)=="SentValue.x"] <- "SentValue.LVTS";
colnames(FILevelTXNsDataSeriesSentRecieved_df)[colnames(FILevelTXNsDataSeriesSentRecieved_df)=="RecievedVolume.x"] <- "RecievedVolume.LVTS";
colnames(FILevelTXNsDataSeriesSentRecieved_df)[colnames(FILevelTXNsDataSeriesSentRecieved_df)=="RecievedValue.x"] <- "RecievedValue.LVTS";
colnames(FILevelTXNsDataSeriesSentRecieved_df)[colnames(FILevelTXNsDataSeriesSentRecieved_df)=="SentVolume.y"] <- "SentVolume.ACSS";
colnames(FILevelTXNsDataSeriesSentRecieved_df)[colnames(FILevelTXNsDataSeriesSentRecieved_df)=="SentValue.y"] <- "SentValue.ACSS";
colnames(FILevelTXNsDataSeriesSentRecieved_df)[colnames(FILevelTXNsDataSeriesSentRecieved_df)=="RecievedVolume.y"] <- "RecievedVolume.ACSS";
colnames(FILevelTXNsDataSeriesSentRecieved_df)[colnames(FILevelTXNsDataSeriesSentRecieved_df)=="RecievedValue.y"] <- "RecievedValue.ACSS";

colnames(FILevelTXNsDataSeriesSentRecieved_df)[colnames(FILevelTXNsDataSeriesSentRecieved_df)=="ShareSentVolume.x"] <- "ShareSentVolume.LVTS";
colnames(FILevelTXNsDataSeriesSentRecieved_df)[colnames(FILevelTXNsDataSeriesSentRecieved_df)=="ShareSentValue.x"] <- "ShareSentValue.LVTS";
colnames(FILevelTXNsDataSeriesSentRecieved_df)[colnames(FILevelTXNsDataSeriesSentRecieved_df)=="ShareRecievedVolume.x"] <- "ShareRecievedVolume.LVTS";
colnames(FILevelTXNsDataSeriesSentRecieved_df)[colnames(FILevelTXNsDataSeriesSentRecieved_df)=="ShareRecievedValue.x"] <- "ShareRecievedValue.LVTS";
colnames(FILevelTXNsDataSeriesSentRecieved_df)[colnames(FILevelTXNsDataSeriesSentRecieved_df)=="ShareSentVolume.y"] <- "ShareSentVolume.ACSS";
colnames(FILevelTXNsDataSeriesSentRecieved_df)[colnames(FILevelTXNsDataSeriesSentRecieved_df)=="ShareSentValue.y"] <- "ShareSentValue.ACSS";
colnames(FILevelTXNsDataSeriesSentRecieved_df)[colnames(FILevelTXNsDataSeriesSentRecieved_df)=="ShareRecievedVolume.y"] <- "ShareRecievedVolume.ACSS";
colnames(FILevelTXNsDataSeriesSentRecieved_df)[colnames(FILevelTXNsDataSeriesSentRecieved_df)=="ShareRecievedValue.y"] <- "ShareRecievedValue.ACSS";


##Ensure that the member field is set as a string
FILevelTXNsDataSeriesSentRecieved_df$member <- as.character(FILevelTXNsDataSeriesSentRecieved_df$member)
##Ensure that the Year_Month, Year and Year_Quarter are represented as dates
if(dataFrequency == "monthly"){
  FILevelTXNsDataSeriesSentRecieved_df$Year_Month <- as.character.Date(FILevelTXNsDataSeriesSentRecieved_df$Year_Month);
}else if(dataFrequency == "quarterly"){
  FILevelTXNsDataSeriesSentRecieved_df$Year_Quarter <- as.character.Date(FILevelTXNsDataSeriesSentRecieved_df$Year_Quarter);
}else if(dataFrequency == "annual"){
  FILevelTXNsDataSeriesSentRecieved_df$Year <- as.character.Date(FILevelTXNsDataSeriesSentRecieved_df$Year);
}else if(dataFrequency == "daily"){
  
}
print(class(FILevelTXNsDataSeriesSentRecieved_df$member));

##convert FILevelTXNsDataSeriesSentRecieved_df into a data table.
options(stringsAsFactors=FALSE)
FILevelTXNsDataSeriesSentRecieved <- as.data.table(FILevelTXNsDataSeriesSentRecieved_df);
#FILevelTXNsDataSeriesSentRecieved <- data.table(FILevelTXNsDataSeriesSentRecieved_df);
print(class(FILevelTXNsDataSeriesSentRecieved[,member]));
##remove no longer required data tables
rm(ACSSTransDataSeriesSentRecieved_df);
rm(LVTSTransDataSeriesSentRecieved_df);
rm(FILevelTXNsDataSeriesSentRecieved_df);
gc();

##Replace N/A values with 0s
for (i in seq_along(FILevelTXNsDataSeriesSentRecieved)) 
  set(FILevelTXNsDataSeriesSentRecieved, i=which(is.na(FILevelTXNsDataSeriesSentRecieved[[i]])), j=i, value=0)

##This method carries out a deep copy of the passed data table
#dataTableCopy <- function(inputeDataTable){
#  outputDataTable <- copy(inputeDataTable);
#  return(outputDataTable);
#}

gc(); #free up memory



##order the data chronographically and alphabetically by FI
if(dataFrequency == "monthly"){
  
  FILevelTXNsDataSeriesSentRecieved  <-  FILevelTXNsDataSeriesSentRecieved[order(member, Year_Month), ];
  AggregationLevelTXNsDataSeries <-  FILevelTXNsDataSeriesSentRecieved[, list(SentVolume.LVTS=sum(na.omit(SentVolume.LVTS)), 
                                                                              RecievedVolume.LVTS=sum(na.omit(RecievedVolume.LVTS)),
                                                                    TotalVolume.LVTS=sum(na.omit(RecievedVolume.LVTS)),
                                                                    SentValue.LVTS=sum(na.omit(SentValue.LVTS)), 
                                                                                       RecievedValue.LVTS=sum(na.omit(RecievedValue.LVTS)),
                                                                    TotalValue.LVTS=sum(na.omit(SentValue.LVTS)),
                                                                    SentVolume.ACSS=sum(na.omit(SentVolume.ACSS)), 
                                                                                        RecievedVolume.ACSS=sum(na.omit(RecievedVolume.ACSS)),
                                                                    TotalVolume.ACSS=sum(na.omit(SentVolume.ACSS)),
                                                                    SentValue.ACSS=sum(na.omit(SentValue.ACSS)), 
                                                                                       RecievedValue.ACSS=sum(na.omit(RecievedValue.ACSS)),
                                                                    TotalValue.ACSS=sum(na.omit(SentValue.ACSS))
                                                                    ), 
                                                                    by=c("Year_Month")];
  AggregationLevelTXNsDataSeries  <-  AggregationLevelTXNsDataSeries[order(Year_Month), ];
  #if quarterly  
}else if(dataFrequency == "quarterly"){
  
  FILevelTXNsDataSeriesSentRecieved  <-  FILevelTXNsDataSeriesSentRecieved[order(member, Year_Quarter), ];
  AggregationLevelTXNsDataSeries <-  FILevelTXNsDataSeriesSentRecieved[, list(SentVolume.LVTS=sum(na.omit(SentVolume.LVTS)), 
                                                                              RecievedVolume.LVTS=sum(na.omit(RecievedVolume.LVTS)),
                                                                              TotalVolume.LVTS=sum(na.omit(RecievedVolume.LVTS)),
                                                                              SentValue.LVTS=sum(na.omit(SentValue.LVTS)), 
                                                                              RecievedValue.LVTS=sum(na.omit(RecievedValue.LVTS)),
                                                                              TotalValue.LVTS=sum(na.omit(SentValue.LVTS)),
                                                                              SentVolume.ACSS=sum(na.omit(SentVolume.ACSS)), 
                                                                              RecievedVolume.ACSS=sum(na.omit(RecievedVolume.ACSS)),
                                                                              TotalVolume.ACSS=sum(na.omit(SentVolume.ACSS)),
                                                                              SentValue.ACSS=sum(na.omit(SentValue.ACSS)), 
                                                                              RecievedValue.ACSS=sum(na.omit(RecievedValue.ACSS)),
                                                                              TotalValue.ACSS=sum(na.omit(SentValue.ACSS))
                                                                              ),
                                                                       by=c("Year_Quarter")];
  AggregationLevelTXNsDataSeries  <-  AggregationLevelTXNsDataSeries[order(Year_Quarter), ];
  #if annual 
}else if(dataFrequency == "annual"){
  
  FILevelTXNsDataSeriesSentRecieved  <-  FILevelTXNsDataSeriesSentRecieved[order(member,Year), ];
  AggregationLevelTXNsDataSeries <-  FILevelTXNsDataSeriesSentRecieved[, list(SentVolume.LVTS=sum(na.omit(SentVolume.LVTS)), 
                                                                              RecievedVolume.LVTS=sum(na.omit(RecievedVolume.LVTS)),
                                                                              TotalVolume.LVTS=sum(na.omit(RecievedVolume.LVTS)),
                                                                              SentValue.LVTS=sum(na.omit(SentValue.LVTS)), 
                                                                              RecievedValue.LVTS=sum(na.omit(RecievedValue.LVTS)),
                                                                              TotalValue.LVTS=sum(na.omit(SentValue.LVTS)),
                                                                              SentVolume.ACSS=sum(na.omit(SentVolume.ACSS)), 
                                                                              RecievedVolume.ACSS=sum(na.omit(RecievedVolume.ACSS)),
                                                                              TotalVolume.ACSS=sum(na.omit(SentVolume.ACSS)),
                                                                              SentValue.ACSS=sum(na.omit(SentValue.ACSS)), 
                                                                              RecievedValue.ACSS=sum(na.omit(RecievedValue.ACSS)),
                                                                              TotalValue.ACSS=sum(na.omit(SentValue.ACSS))
                                                                              ), 
                                                                       by=c("Year")];
  AggregationLevelTXNsDataSeries  <-  AggregationLevelTXNsDataSeries[order(Year), ];
  #if daily   
}else if(dataFrequency == "daily"){
  
}

##Save generated data table as RData, STATA and excel files
save(FILevelTXNsDataSeriesSentRecieved,file=FILevelTXNsDataSeriesSentRecievedSavePath);
write.dta(FILevelTXNsDataSeriesSentRecieved,file=FILevelTXNsDataSeriesSentRecievedSTATASavePath, convert.dates = TRUE, tz = "America/New_York", 
          convert.factors = c("labels", "string", "numeric", "codes"));
#write.xlsx(FILevelTXNsDataSeriesSentRecieved, file=FILevelTXNsDataSeriesSentRecievedEXCELSavePath);
save(AggregationLevelTXNsDataSeries,file=AggregationLevelTXNsDataSeriesSavePath);
write.dta(AggregationLevelTXNsDataSeries,file=AggregationLevelTXNsDataSeriesSTATASavePath, convert.dates = TRUE, tz = "America/New_York", 
          convert.factors = c("labels", "string", "numeric", "codes"));
#write.xlsx(AggregationLevelTXNsDataSeries, file=AggregationLevelTXNsDataSeriesEXCELSavePath);


gc();
