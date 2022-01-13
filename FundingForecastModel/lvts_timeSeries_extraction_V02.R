
##This script is used to collect the FI data generated in the lvts_post_processing_file_Level_2_V##.R file.
##The generated file will be used to provide finance with the sumarry details for each FI's sending an recieving habits
##
##Final input data tables that the code will work with are 
##        a: LVTSTXNsDataSeriesSentRecieved - The LVTS data file
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

##<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<START DATA/STATISTICAL COLLECTION>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
##<<<<<<<<<<<<<<<<<<<<<<<<<LOAD DATA>>>>>>>>>>>>>>>>>>>>>>>>
##Now load the individual FI's complete data tables
print("Loading data table(s)");#used to record speed of computations
print(now());#used to record speed of computations

##Manual intervention
if(manualIntervention==TRUE){
  ##Manual intervention
  LVTSTXNsDataSeriesSentRecieved <- read.csv(extractedLVTSDataLoadPath);
}else{
  load(extractedLVTSDataLoadPath);
}


##Include path to aggregated data for use in the validation of the sum of individual FI forecasts (This is part of the amendments requested following the 2017 Aduit)
if(dataFrequency == "monthly"){
  ##Compute aggregate total volume for the period
  LVTSTXNsDataSeriesSentRecievedSum <- 
    LVTSTXNsDataSeriesSentRecieved[,list(RecievedVolume=sum(RecievedVolume), RecievedValue=sum(RecievedValue), 
                                         SentVolume=sum(SentVolume), SentValue=sum(SentValue), 
                                         TotalVolume=sum(TotalVolume), TotalValue=sum(TotalValue)
                                         ),by=c("Year_Month")];
  ##sender name for aggregated data
   LVTSTXNsDataSeriesSentRecievedSum[,`:=`(sender="ALLFIS", senderID=9999, 
                                       senderFullName="All Financial Institutions")];
  ##merge created aggregated periodic total volumes with FI level volumes data
  LVTSTXNsDataSeriesSentRecieved <- rbindlist(list(LVTSTXNsDataSeriesSentRecieved, 
                                                       LVTSTXNsDataSeriesSentRecievedSum), 
                                                  use.names=TRUE, fill=TRUE, idcol=FALSE);

}else if(dataFrequency == "quarterly"){
  LVTSTXNsDataSeriesSentRecievedSum <- 
    LVTSTXNsDataSeriesSentRecieved[,list(RecievedVolume=sum(RecievedVolume), RecievedValue=sum(RecievedValue), 
                                         SentVolume=sum(SentVolume), SentValue=sum(SentValue), 
                                         TotalVolume=sum(TotalVolume), TotalValue=sum(TotalValue)),
                                         by=c("Year_Quarter")];
  ##sender name for aggregated data
  LVTSTXNsDataSeriesSentRecievedSum[,`:=`(sender="ALLFIS", senderID=9999, 
                                          senderFullName="All Financial Institutions")];
  ##merge created aggregated periodic total volumes with FI level volumes data
  LVTSTXNsDataSeriesSentRecieved <- rbindlist(list(LVTSTXNsDataSeriesSentRecieved, 
                                                       LVTSTXNsDataSeriesSentRecievedSum), 
                                                  use.names=TRUE, fill=TRUE, idcol=FALSE);
  
}else if(dataFrequency == "annual"){
  LVTSTXNsDataSeriesSentRecievedSum <- 
    LVTSTXNsDataSeriesSentRecieved[,list(RecievedVolume=sum(RecievedVolume), RecievedValue=sum(RecievedValue), 
                                         SentVolume=sum(SentVolume), SentValue=sum(SentValue), 
                                         TotalVolume=sum(TotalVolume), TotalValue=sum(TotalValue)),
                                   by=c("Year")];
  ##sender name for aggregated data
  LVTSTXNsDataSeriesSentRecievedSum[,`:=`(sender="ALLFIS", senderID=9999, 
                                          senderFullName="All Financial Institutions")];
  ##merge created aggregated periodic total volumes with FI level volumes data
  LVTSTXNsDataSeriesSentRecieved <- rbindlist(list(LVTSTXNsDataSeriesSentRecieved, 
                                                       LVTSTXNsDataSeriesSentRecievedSum), 
                                                  use.names=TRUE, fill=TRUE, idcol=FALSE);
  
}else if(dataFrequency == "daily"){
  LVTSTXNsDataSeriesSentRecievedSum <- 
    LVTSTXNsDataSeriesSentRecieved[,list(RecievedVolume=sum(RecievedVolume), RecievedValue=sum(RecievedValue), 
                                         SentVolume=sum(SentVolume), SentValue=sum(SentValue), 
                                         TotalVolume=sum(TotalVolume), TotalValue=sum(TotalValue)),
                                   by=c("date")];
  ##sender name for aggregated data
  LVTSTXNsDataSeriesSentRecievedSum[,`:=`(sender="ALLFIS", senderID=9999, 
                                          senderFullName="All Financial Institutions")];
  ##merge created aggregated periodic total volumes with FI level volumes data
  LVTSTXNsDataSeriesSentRecieved <- rbindlist(list(LVTSTXNsDataSeriesSentRecieved, 
                                                       LVTSTXNsDataSeriesSentRecievedSum), 
                                                  use.names=TRUE, fill=TRUE, idcol=FALSE);
  
}


gc();
print("data table(s) loaded");#used to record speed of computations
print(now());#used to record speed of computations
gc();



vectorColumnNames <- getColumnNames(LVTSTXNsDataSeriesSentRecieved);
vectorFINames <- getListOfFINames(LVTSTXNsDataSeriesSentRecieved, vectorColumnNames, "sender");

#vectorOfTimeSeriesDates <- getListOfTImeSeriesDates(LVTSTXNsDataSeriesSentRecieved, vectorColumnNames);
#vectorOfTimeSeriesDates <- as.yearqtr(vectorOfTimeSeriesDates);

print("creating timeSeries Tables/Collection");
print(now());#used to record speed of computations
#getFITimeSeriesDataTable <- function(inputDataTable, colNames, freqCol, seriesValue, keyFld)
#ferqCol can be "Year_Month", "Year_Quarter", "Year" "date"
##Extract Volumes into time series objects and save as a list
LVTSFITimeSeriesDataTable <- 
  getFITimeSeriesDataTable(LVTSTXNsDataSeriesSentRecieved,vectorColumnNames, dataFrequencyColumnName, "Volume", "sender",minObservations);
LVTSFITimeSeriesDataTable <- list.names(LVTSFITimeSeriesDataTable,"LVTSFITimeSeriesList");
list.save(LVTSFITimeSeriesDataTable, LVTSFITimeSeriesDataTableSavePath);

##Extract Volumes into time series objects and save as a list
LVTSValueFITimeSeriesDataTable <- 
  getFITimeSeriesDataTable(LVTSTXNsDataSeriesSentRecieved,vectorColumnNames, dataFrequencyColumnName, "Value", "sender",minObservations);
LVTSValueFITimeSeriesDataTable <- list.names(LVTSValueFITimeSeriesDataTable,"LVTSFITimeSeriesList");
list.save(LVTSValueFITimeSeriesDataTable, LVTSValueFITimeSeriesDataTableSavePath);

save(LVTSTXNsDataSeriesSentRecieved, file=allFILVTSTXNsDataSeriesSentRecievedSavePath);
write.csv(LVTSTXNsDataSeriesSentRecieved, file=allFILVTSTXNsDataSeriesSentRecievedCSVSavePath);
gc();#free up some memory


