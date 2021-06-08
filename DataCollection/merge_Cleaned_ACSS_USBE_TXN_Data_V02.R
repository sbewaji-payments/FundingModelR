##This script is used to merge ACSS and USBE TXN data for the forecasting model once post processing is done
##There are no other script files used in this execution, just the ACSS and USBE RData files:  
##USBE
## usbeTXNsSentRecievedByFI(Monthly)Series2000-2017.Rdata
##ACSS
##acssTXNsSentRecievedByFIMonthlySeries2000-2017.Rdata
##
##
## Author : Segun Bewaji
## Creation Date : 19 Jun 2017
## Modified : Segun Bewaji
## Modifications Made: 19 Jun 2017
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


##<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<POST PROCESSING LOAD FUNCTIONS>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
###Load used functions
# gc();
# print("Loading Used Functions");
# source('C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/DataCollection/post_processing_file_functions_V01.R',
#        encoding = 'UTF-8', echo=TRUE);
 gc();


###<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<INput and Output Specify File Paths>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
#will load the data table: ACSSTransDataSeriesSentRecieved
#and set the name of the time column (dataFrequencyColumnName)
#if monthly
if(!byStream){
  if(dataFrequency == "monthly"){
    
    extractedACSSDataLoadPath <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/acssTXNsSentRecievedByFIMonthlySeries",
                                         startYr,"-",endYr,".Rdata", sep = "", collapse = NULL));
    
    extractedUSBEDataLoadPath <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/usbeTXNsSentRecievedByFIMonthlySeries",
                                         startYr,"-",endYr,".Rdata", sep = "", collapse = NULL));
    
    mergedACSSUSBEFITXNDataTableSavePath <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/acssusbeTXNsSentRecievedByFIMonthlySeries",
                                                    startYr,"-",endYr,".Rdata", sep = "", collapse = NULL));
    #if quarterly  
  }else if(dataFrequency == "quarterly"){
    extractedACSSDataLoadPath <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/acssTXNsSentRecievedByFIQuarterlySeries",
                                         startYr,"-",endYr,".Rdata", sep = "", collapse = NULL));
    
    extractedUSBEDataLoadPath <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/usbeTXNsSentRecievedByFIQuarterlySeries",
                                         startYr,"-",endYr,".Rdata", sep = "", collapse = NULL));
    
    mergedACSSUSBEFITXNDataTableSavePath <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/acssusbeTXNsSentRecievedByFIQuarterlySeries",
                                                    startYr,"-",endYr,".Rdata", sep = "", collapse = NULL));
    #if annual 
  }else if(dataFrequency == "annual"){
    extractedACSSDataLoadPath <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/acssTXNsSentRecievedByFIAnnualSeries",
                                         startYr,"-",endYr,".RData", sep = "", collapse = NULL));
    
    extractedUSBEDataLoadPath <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/usbeTXNsSentRecievedByFIAnnualSeries",
                                         startYr,"-",endYr,".Rdata", sep = "", collapse = NULL));
    
    mergedACSSUSBEFITXNDataTableSavePath <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/acssusbeTXNsSentRecievedByFIAnnualSeries",
                                                    startYr,"-",endYr,".Rdata", sep = "", collapse = NULL));
    #if daily   
  }else if(dataFrequency == "daily"){
    extractedACSSDataLoadPath <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/acssTXNsSentRecievedByFIDailySeries",
                                         startYr,"-",endYr,".RData", sep = "", collapse = NULL));
    extractedUSBEDataLoadPath <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/usbeTXNsSentRecievedByFIDailySeries",
                                         startYr,"-",endYr,".Rdata", sep = "", collapse = NULL));
    
    mergedACSSUSBEFITXNDataTableSavePath <- c(paste("E:/Projects/FundingDataTables/Cleaned Transactions Data/acssusbeTXNsSentRecievedByFIDailySeries",
                                                    startYr,"-",endYr,".Rdata", sep = "", collapse = NULL));
    
  }
}else{
  
}


##Check if the streamConditions and data range are consistent with the merge
if(byStream && startYr<2010){
  print("Data Cannot or will not be merged since stream level analysios cannot 
        be done on USBE prior to 2010 due to lack of data")
}else{
  
  ##Load ACSS and USBE input files
  load(extractedACSSDataLoadPath);
  load(extractedUSBEDataLoadPath);
  load(fiStaticDataLoadPath);
  
  
  ##merge the data.tables
  ##Note this approach is used because the input data.table objects housing the ACSS and USBE TNXs are not keys
  ##THe merge function x[y...] merges x into y so it is all the rows of y that will be used.
  ##in this instance y is the ACSS TXN data
  if(dataFrequency=="monthly"){
    ACSSUSBETXNsDataSeriesSentRecieved <- Reduce(function(x,y) x[y,on=c("sender","Year_Month")],
                                                 list(USBETXNsDataSeriesSentRecieved,ACSSTXNsDataSeriesSentRecieved));
  }else if(dataFrequency == "quarterly"){
    ACSSUSBETXNsDataSeriesSentRecieved <- Reduce(function(x,y) x[y,on=c("sender","Year_Quarter")],
                                                 list(USBETXNsDataSeriesSentRecieved,ACSSTXNsDataSeriesSentRecieved));
    
    #if annual 
  }else if(dataFrequency == "annual"){
    ACSSUSBETXNsDataSeriesSentRecieved <- Reduce(function(x,y) x[y,on=c("sender","Year")],
                                                 list(USBETXNsDataSeriesSentRecieved,ACSSTXNsDataSeriesSentRecieved));
    
    #if daily   
  }else if(dataFrequency == "daily"){
    ACSSUSBETXNsDataSeriesSentRecieved <- Reduce(function(x,y) x[y,on=c("sender","date")],
                                                 list(USBETXNsDataSeriesSentRecieved,ACSSTXNsDataSeriesSentRecieved));
    
  }
  
  ##Rename columns from the USBE TXN Data file
  colnames(ACSSUSBETXNsDataSeriesSentRecieved)[which(names(ACSSUSBETXNsDataSeriesSentRecieved) == "senderFullName")] <-
    "senderFullName.USBE";
  colnames(ACSSUSBETXNsDataSeriesSentRecieved)[which(names(ACSSUSBETXNsDataSeriesSentRecieved) == "SentVolume")] <-
    "SentVolume.USBE";
  colnames(ACSSUSBETXNsDataSeriesSentRecieved)[which(names(ACSSUSBETXNsDataSeriesSentRecieved) == "SentValue")] <-
    "SentValue.USBE"; 
  colnames(ACSSUSBETXNsDataSeriesSentRecieved)[which(names(ACSSUSBETXNsDataSeriesSentRecieved) == "RecievedVolume")] <-
    "RecievedVolume.USBE";
  colnames(ACSSUSBETXNsDataSeriesSentRecieved)[which(names(ACSSUSBETXNsDataSeriesSentRecieved) == "RecievedValue")] <-
    "RecievedValue.USBE";
  colnames(ACSSUSBETXNsDataSeriesSentRecieved)[which(names(ACSSUSBETXNsDataSeriesSentRecieved) == "ShareSentVolume")] <-
    "ShareSentVolume.USBE";
  colnames(ACSSUSBETXNsDataSeriesSentRecieved)[which(names(ACSSUSBETXNsDataSeriesSentRecieved) == "ShareRecievedValue")] <-
    "ShareRecievedValue.USBE";
  colnames(ACSSUSBETXNsDataSeriesSentRecieved)[which(names(ACSSUSBETXNsDataSeriesSentRecieved) == "ShareRecievedVolume")] <-
    "ShareRecievedVolume.USBE";
  colnames(ACSSUSBETXNsDataSeriesSentRecieved)[which(names(ACSSUSBETXNsDataSeriesSentRecieved) == "TotalVolume")] <-
    "TotalVolume.USBE";
  colnames(ACSSUSBETXNsDataSeriesSentRecieved)[which(names(ACSSUSBETXNsDataSeriesSentRecieved) == "TotalValue")] <-
    "TotalValue.USBE"; 
  
  ##Drop the USBE column names for the FIs and replace with those from the ACSS data
  ACSSUSBETXNsDataSeriesSentRecieved <- ACSSUSBETXNsDataSeriesSentRecieved[, c("senderID","senderFullName.USBE"):=NULL];
  colnames(ACSSUSBETXNsDataSeriesSentRecieved)[which(names(ACSSUSBETXNsDataSeriesSentRecieved) == "i.senderID")] <-
    "senderID"; 
  colnames(ACSSUSBETXNsDataSeriesSentRecieved)[which(names(ACSSUSBETXNsDataSeriesSentRecieved) == "i.senderFullName")] <-
    "senderFullName"; 
  
  ##Replace the NA values
  ACSSUSBETXNsDataSeriesSentRecieved[is.na(ACSSUSBETXNsDataSeriesSentRecieved)] <- 0; 

  
  ##Sum the value and volume columns
  if(dataFrequency == "monthly"){
    ACSSUSBETXNsDataSeriesSentRecieved[,c("TotalVolume"):=sum(TotalVolume.USBE, i.TotalVolume),by=c("Year_Month","sender")];
    ACSSUSBETXNsDataSeriesSentRecieved[,c("TotalValue"):=sum(TotalValue.USBE, i.TotalValue),by=c("Year_Month","sender")];
    ACSSUSBETXNsDataSeriesSentRecieved[,c("SentVolume"):=sum(SentVolume.USBE, i.SentVolume),by=c("Year_Month","sender")];
    ACSSUSBETXNsDataSeriesSentRecieved[,c("SentValue"):=sum(SentValue.USBE, i.SentValue),by=c("Year_Month","sender")];
    ACSSUSBETXNsDataSeriesSentRecieved[,c("RecievedVolume"):=sum(RecievedVolume.USBE, i.RecievedVolume),by=c("Year_Month","sender")];
    ACSSUSBETXNsDataSeriesSentRecieved[,c("RecievedValue"):=sum(RecievedValue.USBE, i.RecievedValue),by=c("Year_Month","sender")];
    
    ACSSUSBETXNsDataSeriesSentRecieved_df <- as.data.frame(ACSSUSBETXNsDataSeriesSentRecieved);
    
    ACSSUSBETXNsDataSeriesSentRecieved_df <- do.call("rbind", as.list(by(ACSSUSBETXNsDataSeriesSentRecieved_df, 
                                                                         ACSSUSBETXNsDataSeriesSentRecieved_df["Year_Month"], 
                                                                         transform, ShareSentVolume=fnShare(SentVolume))));
    ACSSUSBETXNsDataSeriesSentRecieved_df <- do.call("rbind", as.list(by(ACSSUSBETXNsDataSeriesSentRecieved_df, 
                                                                         ACSSUSBETXNsDataSeriesSentRecieved_df["Year_Month"], 
                                                                         transform, ShareSentValue=fnShare(SentValue))));
    ACSSUSBETXNsDataSeriesSentRecieved_df <- do.call("rbind", as.list(by(ACSSUSBETXNsDataSeriesSentRecieved_df, 
                                                                         ACSSUSBETXNsDataSeriesSentRecieved_df["Year_Month"], 
                                                                         transform, ShareRecievedVolume=fnShare(RecievedVolume))));
    ACSSUSBETXNsDataSeriesSentRecieved_df <- do.call("rbind", as.list(by(ACSSUSBETXNsDataSeriesSentRecieved_df, 
                                                                         ACSSUSBETXNsDataSeriesSentRecieved_df["Year_Month"], 
                                                                         transform, ShareRecievedValue=fnShare(RecievedValue))));
    
    ACSSUSBETXNsDataSeriesSentRecieved <-  as.data.table(ACSSUSBETXNsDataSeriesSentRecieved_df);
    
    
    #if quarterly  
  }else if(dataFrequency == "quarterly"){
    ACSSUSBETXNsDataSeriesSentRecieved[,c("TotalVolume"):=sum(TotalVolume.USBE, i.TotalVolume),by=c("Year_Quarter","sender")];
    ACSSUSBETXNsDataSeriesSentRecieved[,c("TotalValue"):=sum(TotalValue.USBE, i.TotalValue),by=c("Year_Quarter","sender")];
    ACSSUSBETXNsDataSeriesSentRecieved[,c("SentVolume"):=sum(SentVolume.USBE, i.SentVolume),by=c("Year_Quarter","sender")];
    ACSSUSBETXNsDataSeriesSentRecieved[,c("SentValue"):=sum(SentValue.USBE, i.SentValue),by=c("Year_Quarter","sender")];
    ACSSUSBETXNsDataSeriesSentRecieved[,c("RecievedVolume"):=sum(RecievedVolume.USBE, i.RecievedVolume),by=c("Year_Quarter","sender")];
    ACSSUSBETXNsDataSeriesSentRecieved[,c("RecievedValue"):=sum(RecievedValue.USBE, i.RecievedValue),by=c("Year_Quarter","sender")];
    
    ACSSUSBETXNsDataSeriesSentRecieved_df <- as.data.frame(ACSSUSBETXNsDataSeriesSentRecieved);
    
    ACSSUSBETXNsDataSeriesSentRecieved_df <- do.call("rbind", as.list(by(ACSSUSBETXNsDataSeriesSentRecieved_df, 
                                                                         ACSSUSBETXNsDataSeriesSentRecieved_df["Year_Quarter"], 
                                                                         transform, ShareSentVolume=fnShare(SentVolume))));
    ACSSUSBETXNsDataSeriesSentRecieved_df <- do.call("rbind", as.list(by(ACSSUSBETXNsDataSeriesSentRecieved_df, 
                                                                         ACSSUSBETXNsDataSeriesSentRecieved_df["Year_Quarter"], 
                                                                         transform, ShareSentValue=fnShare(SentValue))));
    ACSSUSBETXNsDataSeriesSentRecieved_df <- do.call("rbind", as.list(by(ACSSUSBETXNsDataSeriesSentRecieved_df, 
                                                                         ACSSUSBETXNsDataSeriesSentRecieved_df["Year_Quarter"], 
                                                                         transform, ShareRecievedVolume=fnShare(RecievedVolume))));
    ACSSUSBETXNsDataSeriesSentRecieved_df <- do.call("rbind", as.list(by(ACSSUSBETXNsDataSeriesSentRecieved_df, 
                                                                         ACSSUSBETXNsDataSeriesSentRecieved_df["Year_Quarter"], 
                                                                         transform, ShareRecievedValue=fnShare(RecievedValue))));
    
    ACSSUSBETXNsDataSeriesSentRecieved <-  as.data.table(ACSSUSBETXNsDataSeriesSentRecieved_df);
    
    
    #if annual 
  }else if(dataFrequency == "annual"){
    ACSSUSBETXNsDataSeriesSentRecieved[,c("TotalVolume"):=sum(TotalVolume.USBE, i.TotalVolume),by=c("Year","sender")];
    ACSSUSBETXNsDataSeriesSentRecieved[,c("TotalValue"):=sum(TotalValue.USBE, i.TotalValue),by=c("Year","sender")];
    ACSSUSBETXNsDataSeriesSentRecieved[,c("SentVolume"):=sum(SentVolume.USBE, i.SentVolume),by=c("Year","sender")];
    ACSSUSBETXNsDataSeriesSentRecieved[,c("SentValue"):=sum(SentValue.USBE, i.SentValue),by=c("Year","sender")];
    ACSSUSBETXNsDataSeriesSentRecieved[,c("RecievedVolume"):=sum(RecievedVolume.USBE, i.RecievedVolume),by=c("Year","sender")];
    ACSSUSBETXNsDataSeriesSentRecieved[,c("RecievedValue"):=sum(RecievedValue.USBE, i.RecievedValue),by=c("Year","sender")];
    
    ACSSUSBETXNsDataSeriesSentRecieved_df <- as.data.frame(ACSSUSBETXNsDataSeriesSentRecieved);
    
    ACSSUSBETXNsDataSeriesSentRecieved_df <- do.call("rbind", as.list(by(ACSSUSBETXNsDataSeriesSentRecieved_df, 
                                                                         ACSSUSBETXNsDataSeriesSentRecieved_df["Year"], 
                                                                         transform, ShareSentVolume=fnShare(SentVolume))));
    ACSSUSBETXNsDataSeriesSentRecieved_df <- do.call("rbind", as.list(by(ACSSUSBETXNsDataSeriesSentRecieved_df, 
                                                                         ACSSUSBETXNsDataSeriesSentRecieved_df["Year"], 
                                                                         transform, ShareSentValue=fnShare(SentValue))));
    ACSSUSBETXNsDataSeriesSentRecieved_df <- do.call("rbind", as.list(by(ACSSUSBETXNsDataSeriesSentRecieved_df, 
                                                                         ACSSUSBETXNsDataSeriesSentRecieved_df["Year"], 
                                                                         transform, ShareRecievedVolume=fnShare(RecievedVolume))));
    ACSSUSBETXNsDataSeriesSentRecieved_df <- do.call("rbind", as.list(by(ACSSUSBETXNsDataSeriesSentRecieved_df, 
                                                                         ACSSUSBETXNsDataSeriesSentRecieved_df["Year"], 
                                                                         transform, ShareRecievedValue=fnShare(RecievedValue))));
    
    ACSSUSBETXNsDataSeriesSentRecieved <-  as.data.table(ACSSUSBETXNsDataSeriesSentRecieved_df);
    
    #if daily   
  }else if(dataFrequency == "daily"){

  }

  ##remove unrequireed files ands clear memory
  rm(ACSSUSBETXNsDataSeriesSentRecieved_df);
  gc();
  ##Delete ubnrequired columns
  ACSSUSBETXNsDataSeriesSentRecieved <- ACSSUSBETXNsDataSeriesSentRecieved[, c("SentVolume.USBE",
                                                                               "SentValue.USBE","RecievedVolume.USBE",
                                                                               "RecievedValue.USBE",
                                                                               "ShareSentVolume.USBE","ShareSentValue" ,
                                                                               "ShareRecievedVolume.USBE",
                                                                               "ShareRecievedValue.USBE","TotalVolume.USBE",
                                                                               "TotalValue.USBE",
                                                                               "senderID","senderFullName","i.SentVolume",
                                                                               "i.SentValue",
                                                                               "i.RecievedVolume","i.RecievedValue",
                                                                               "i.ShareSentVolume","i.ShareSentValue",
                                                                               "i.ShareRecievedVolume",
                                                                               "i.ShareRecievedValue",
                                                                               "i.TotalVolume","i.TotalValue",
                                                                               "senderID.1", "senderFullName.1"):=NULL];
}


ACSSUSBETXNsDataSeriesSentRecieved_df  <-  as.data.frame(ACSSUSBETXNsDataSeriesSentRecieved);#convert ACSSUSBETXNsDataSeriesSentRecieved to a temporary data.frame object
fiStaticDataFile_df  <-  as.data.frame(fiStaticDataFile);
rm(fiStaticDataFile);#remove fiStaticDataFile to free up memory since not in use
ACSSUSBETXNsDataSeriesSentRecieved_df  <-  merge(ACSSUSBETXNsDataSeriesSentRecieved_df, fiStaticDataFile_df, by.x="sender", by.y="memberBIC");
colnames(ACSSUSBETXNsDataSeriesSentRecieved_df)[colnames(ACSSUSBETXNsDataSeriesSentRecieved_df)=="memberID"] <- "senderID";
colnames(ACSSUSBETXNsDataSeriesSentRecieved_df)[colnames(ACSSUSBETXNsDataSeriesSentRecieved_df)=="memberName"] <- "senderFullName";
ACSSUSBETXNsDataSeriesSentRecieved <- as.data.table(ACSSUSBETXNsDataSeriesSentRecieved_df);
ACSSUSBETXNsDataSeriesSentRecieved <- ACSSUSBETXNsDataSeriesSentRecieved[sender!="N",]; ##ensure that all no longer active FI data is removed
##Save the output file and clear memory
save(ACSSUSBETXNsDataSeriesSentRecieved, file = mergedACSSUSBEFITXNDataTableSavePath);
gc();
