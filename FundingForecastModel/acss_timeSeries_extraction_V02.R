
##This script is used to collect the FI data generated in the acss_post_processing_file_Level_2_V##.R file.
##The generated file will be used to provide finance with the sumarry details for each FI's sending an recieving habits
##
##Final input data tables that the code will work with are 
##        a: ACSSTXNsDataSeriesSentRecieved - The ACSS data file
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

##<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<START DATA/STATISTICAL ANALYSIS>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
##<<<<<<<<<<<<<<<<<<<<<<<<<LOAD DATA>>>>>>>>>>>>>>>>>>>>>>>>
##Now load the individual FI's complete data tables
print("Loading data table(s)");#used to record speed of computations
print(now());#used to record speed of computations
load(extractedACSSDataLoadPath);
gc();
print("data table(s) loaded");#used to record speed of computations
print(now());#used to record speed of computations
gc();


if(dataFrequency == "monthly"){
  ACSSTXNsDataSeriesSentRecieved <- ACSSTXNsDataSeriesSentRecieved[sender!="N",]; ##ensure that all no longer active FI data is removed
  ACSSTXNsDataSeriesSentRecieved$Year_Month <- as.yearmon(ACSSTXNsDataSeriesSentRecieved$Year_Month, '%YM%m');
  ACSSTXNsDataSeriesSentRecieved <- ACSSTXNsDataSeriesSentRecieved[order(Year_Month),];
  
  ##Compute aggregate total volume for the period
  ACSSTXNsDataSeriesSentRecievedSum <- 
    ACSSTXNsDataSeriesSentRecieved[,list(RecievedVolume=sum(RecievedVolume), RecievedValue=sum(RecievedValue), 
                                             SentVolume=sum(SentVolume), SentValue=sum(SentValue), 
                                             ShareSentVolume=sum(ShareSentVolume),
                                             ShareRecievedVolume=sum(ShareRecievedVolume), 
                                             ShareRecievedValue=sum(ShareRecievedValue), 
                                             TotalVolume=sum(TotalVolume), TotalValue=sum(TotalValue)),
                                       by=c("Year_Month")];
  ##sender name for aggregated data
  ACSSTXNsDataSeriesSentRecievedSum[,`:=`(sender="ALLFIS", senderID=9999, 
                                              senderFullName="All Financial Institutions")];
  ##merge created aggregated periodic total volumes with FI level volumes data
  ACSSTXNsDataSeriesSentRecieved <- rbindlist(list(ACSSTXNsDataSeriesSentRecieved, 
                                                       ACSSTXNsDataSeriesSentRecievedSum), 
                                                  use.names=TRUE, fill=TRUE, idcol=FALSE);
  
  #if quarterly  
}else if(dataFrequency == "quarterly"){#quarterly
  ACSSTXNsDataSeriesSentRecieved <- ACSSTXNsDataSeriesSentRecieved[sender!="N",]; ##ensure that all no longer active FI data is removed
  ACSSTXNsDataSeriesSentRecieved$Year_Quarter <- as.yearqtr(ACSSTXNsDataSeriesSentRecieved$Year_Quarter);
  ACSSTXNsDataSeriesSentRecieved <- ACSSTXNsDataSeriesSentRecieved[order(Year_Quarter),];
  ACSSTXNsDataSeriesSentRecievedSum <- 
    ACSSTXNsDataSeriesSentRecieved[,list(TotalVolume=sum(TotalVolume), TotalValue=sum(TotalValue),
                                             RecievedVolume=sum(RecievedVolume), RecievedValue=sum(RecievedValue), 
                                             SentVolume=sum(SentVolume), SentValue=sum(SentValue), 
                                             ShareSentVolume=sum(ShareSentVolume),
                                             ShareRecievedVolume=sum(ShareRecievedVolume), 
                                             ShareRecievedValue=sum(ShareRecievedValue) 
    ),
    by=c("Year_Quarter")];
  gc();
  ##sender name for aggregated data
  ACSSTXNsDataSeriesSentRecievedSum[,`:=`(sender="ALLFIS", senderID=9999, 
                                              senderFullName="All Financial Institutions")];
  gc();
  ##merge created aggregated periodic total volumes with FI level volumes data
  ACSSTXNsDataSeriesSentRecieved <- rbindlist(list(ACSSTXNsDataSeriesSentRecieved, 
                                                       ACSSTXNsDataSeriesSentRecievedSum), 
                                                  use.names=TRUE, fill=TRUE, idcol=FALSE);
  
  #if annual 
}else if(dataFrequency == "annual"){
  ACSSTXNsDataSeriesSentRecieved <- ACSSTXNsDataSeriesSentRecieved[sender!="N",]; ##ensure that all no longer active FI data is removed
  ACSSTXNsDataSeriesSentRecieved <- ACSSTXNsDataSeriesSentRecieved[order(Year),];
  ACSSTXNsDataSeriesSentRecievedSum <- 
    ACSSTXNsDataSeriesSentRecieved[,list(RecievedVolume=sum(RecievedVolume), RecievedValue=sum(RecievedValue), 
                                             SentVolume=sum(SentVolume), SentValue=sum(SentValue), 
                                             ShareSentVolume=sum(ShareSentVolume),
                                             ShareRecievedVolume=sum(ShareRecievedVolume), 
                                             ShareRecievedValue=sum(ShareRecievedValue), 
                                             TotalVolume=sum(TotalVolume), TotalValue=sum(TotalValue)),
                                       by=c("Year")];
  ##sender name for aggregated data
  ACSSTXNsDataSeriesSentRecievedSum[,`:=`(sender="ALLFIS", senderID=9999, 
                                              senderFullName="All Financial Institutions")];
  ##merge created aggregated periodic total volumes with FI level volumes data
  ACSSTXNsDataSeriesSentRecieved <- rbindlist(list(ACSSTXNsDataSeriesSentRecieved, 
                                                       ACSSTXNsDataSeriesSentRecievedSum), 
                                                  use.names=TRUE, fill=TRUE, idcol=FALSE);
  
  #if daily   
}else if(dataFrequency == "daily"){
  ACSSTXNsDataSeriesSentRecieved <- ACSSTXNsDataSeriesSentRecieved[sender!="N",]; ##ensure that all no longer active FI data is removed
  ACSSTXNsDataSeriesSentRecieved <- ACSSTXNsDataSeriesSentRecieved[order(date),];
  ACSSTXNsDataSeriesSentRecievedSum <- 
    ACSSTXNsDataSeriesSentRecieved[,list(RecievedVolume=sum(RecievedVolume), RecievedValue=sum(RecievedValue), 
                                             SentVolume=sum(SentVolume), SentValue=sum(SentValue), 
                                             ShareSentVolume=sum(ShareSentVolume),
                                             ShareRecievedVolume=sum(ShareRecievedVolume),
                                             ShareRecievedValue=sum(ShareRecievedValue), 
                                             TotalVolume=sum(TotalVolume), TotalValue=sum(TotalValue)),
                                       by=c("date")];
  ##sender name for aggregated data
  ACSSTXNsDataSeriesSentRecievedSum[,`:=`(sender="ALLFIS", senderID=9999, 
                                              senderFullName="All Financial Institutions")];
  ##merge created aggregated periodic total volumes with FI level volumes data
  ACSSTXNsDataSeriesSentRecieved <- rbindlist(list(ACSSTXNsDataSeriesSentRecieved, 
                                                       ACSSTXNsDataSeriesSentRecievedSum), 
                                                  use.names=TRUE, fill=TRUE, idcol=FALSE);
  
}


vectorColumnNames <- getColumnNames(ACSSTXNsDataSeriesSentRecieved);
vectorFINames <- getListOfFINames(ACSSTXNsDataSeriesSentRecieved, vectorColumnNames, "sender");
vectorOfTimeSeriesDates <- getListOfTImeSeriesDates(ACSSTXNsDataSeriesSentRecieved, vectorColumnNames);
vectorOfTimeSeriesDates <- as.yearqtr(vectorOfTimeSeriesDates);

print("creating timeSeries Tables/Collection");
print(now());#used to record speed of computations
#getFITimeSeriesDataTable <- function(inputDataTable, colNames, freqCol, seriesValue, keyFld)
#ferqCol can be "Year_Month", "Year_Quarter", "Year" "date"
ACSSFITimeSeriesDataTable <- 
  getFITimeSeriesDataTable(ACSSTXNsDataSeriesSentRecieved,vectorColumnNames, dataFrequencyColumnName, "Volume", "sender",minObservations);
ACSSFITimeSeriesDataTable <- list.names(ACSSFITimeSeriesDataTable,"ACSSFITimeSeriesList");
list.save(ACSSFITimeSeriesDataTable, ACSSFITimeSeriesDataTableSavePath);
gc();#free up some memory


