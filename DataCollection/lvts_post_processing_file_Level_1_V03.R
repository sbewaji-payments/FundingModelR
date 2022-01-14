##This script acts to merge the LVTS data with the FI static data and assign the appropriate BIC Codes based on memberIDs.
##This is required because the LVTS data only records memberIDs and not their BIC code.
##Consequently, in order to maintain consistency with the LVTS dataset BIC codes must be assigned to the LVTS data
## 
##The Script will import the saved data.table extract from the "LVTS_processing_file_V##_Start-to-End_Year_Selective_Extraction.R" code
##as well as the saved StaticData file extracted using the "process_CPA-MemberNamesBICandIDs.R"code
##
##Final input data tables that the code will work with are 
##        a: mergedLVTSTXNsYearsSpecYM - The LVTS data file
##        b: fiStaticDataFile - The static data file
##
##
##
## Author : Segun Bewaji
## Creation Date : 03 Sept 2015
## Modified : Segun Bewaji
## Modifications Made: 03 Sept 2015
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
## 
## $Id$

##the usual

##<<<<<<<<<<<<<<<<<<<<<<<<<LOAD DATA>>>>>>>>>>>>>>>>>>>>>>>>
##Set Data Frequency
print(paste("data frequency is", dataFrequency, sep=" "));#used to record speed of computations
##Now load the individual FI's complete data tables
print("Loading data tables");#used to record speed of computations
print(now());#used to record speed of computations
load(extractedLVTSDataLoadPath);
gc();
print("data tables loaded");#used to record speed of computations
print(now());#used to record speed of computations
gc();
##Order the data.table import by date and sender
mergedLVTSTXNsYearsSpecYM <- mergedLVTSTXNsYearsSpecYM[order(date, sender)];
#mergedLVTSTXNsYearsSpecYMSummarizedSenderReceiver  <-  as.data.frame(mergedLVTSTXNsYearsSpecYM);#convert mergedLVTSTXNsYearsSpecYM to a temporary data.frame object
#rm(mergedLVTSTXNsYearsSpecYM);#remove mergedLVTSTXNsYearsSpecYM to free up memory since not in use
gc(); #free up memory


##The following if-else statements are used to set the set the value for "total value" and "total volume" by the data frequency value.
##This has been taken out of the initial data collation script because of the resource intensive nature of the collation script as the data range increases
##The statment simply assignes the TotalValue and TotalVolume as the sum of the amount field and maximum value of the monthly, quarterly, annual and daily 
##count fields depending on the data frequency required
if(dataFrequency == "monthly"){
  ##Remove unused columns
  mergedLVTSTXNsYearsSpecYM[ ,c("QuarterlyCountRec", "QuarterlyCount", "AnnualCountRec", "AnnualCount", "DailyCountRec", "DailyCount", 
                                 "date.time", "Year_Quarter", "Year") := NULL];
  
  #If aggregation should be at tranche level or overall payment
  if(byTranche & !byStream){
    mergedLVTSTXNsYearsSpecYM  <-  mergedLVTSTXNsYearsSpecYM[, list(TotalValue=sum(amount), TotalVolume=max(MonthlyCount), TotalVolumeRec=max(MonthlyCountRec)), 
                                                             by=c("Year_Month","sender","receiver","tranche", "settlementMechanism")];
  }else if(byTranche & byStream){
    mergedLVTSTXNsYearsSpecYM  <-  mergedLVTSTXNsYearsSpecYM[, list(TotalValue=sum(amount), TotalVolume=max(MonthlyCount), TotalVolumeRec=max(MonthlyCountRec)), 
                                                             by=c("Year_Month","sender","receiver","tranche", "settlementMechanism", "stream")];
  }else{
    mergedLVTSTXNsYearsSpecYM  <-  mergedLVTSTXNsYearsSpecYM[, list(TotalValue=sum(amount), TotalVolume=max(MonthlyCount), TotalVolumeRec=max(MonthlyCountRec)), 
                                                             by=c("Year_Month","sender","receiver")];
  }
  
 
}else if(dataFrequency == "quarterly"){
  ##Remove unused columns
  mergedLVTSTXNsYearsSpecYM[ ,c("MonthlyCountRec", "MonthlyCount",  "AnnualCountRec", "AnnualCount", "DailyCountRec", "DailyCount",
                                 "date.time", "Year_Month", "Year") := NULL];
  #If aggregation should be at tranche level or overall payment
  if(byTranche & !byStream){
    mergedLVTSTXNsYearsSpecYM  <-  mergedLVTSTXNsYearsSpecYM[, list(TotalValue=sum(amount), TotalVolume=max(QuarterlyCount), TotalVolumeRec=max(QuarterlyCountRec)), 
                                                             by=c("Year_Quarter","sender","receiver","tranche", "settlementMechanism")];
  }else if(byTranche & byStream){
    mergedLVTSTXNsYearsSpecYM  <-  mergedLVTSTXNsYearsSpecYM[, list(TotalValue=sum(amount), TotalVolume=max(QuarterlyCount), TotalVolumeRec=max(QuarterlyCountRec)), 
                                                             by=c("Year_Quarter","sender","receiver","tranche", "settlementMechanism", "stream")];
  }else{
    mergedLVTSTXNsYearsSpecYM  <-  mergedLVTSTXNsYearsSpecYM[, list(TotalValue=sum(amount), TotalVolume=max(QuarterlyCount), TotalVolumeRec=max(QuarterlyCountRec)), 
                                                             by=c("Year_Quarter","sender","receiver")];
  }


}else if(dataFrequency == "annual"){
  ##Remove unused columns
  mergedLVTSTXNsYearsSpecYM[ ,c("MonthlyCountRec", "MonthlyCount", "QuarterlyCountRec", "QuarterlyCount", "DailyCountRec", "DailyCount", 
                                 "date.time", "Year_Month", "Year_Quarter") := NULL];
  #If aggregation should be at tranche level or overall payment
  if(byTranche & !byStream){
    mergedLVTSTXNsYearsSpecYM  <-  mergedLVTSTXNsYearsSpecYM[, list(TotalValue=sum(amount), TotalVolume=max(AnnualCount), TotalVolumeRec=max(AnnualCountRec)), 
                                                             by=c("Year","sender","receiver","tranche", "settlementMechanism")];
  }else if(byTranche & byStream){
    mergedLVTSTXNsYearsSpecYM  <-  mergedLVTSTXNsYearsSpecYM[, list(TotalValue=sum(amount), TotalVolume=max(AnnualCount), TotalVolumeRec=max(AnnualCountRec)), 
                                                             by=c("Year","sender","receiver","tranche", "settlementMechanism", "stream")];
  }else{
    mergedLVTSTXNsYearsSpecYM  <-  mergedLVTSTXNsYearsSpecYM[, list(TotalValue=sum(amount), TotalVolume=max(AnnualCount), TotalVolumeRec=max(AnnualCountRec)), 
                                                             by=c("Year","sender","receiver")];
  }
  

}else if(dataFrequency == "daily"){
  ##Remove unused columns
  mergedLVTSTXNsYearsSpecYM[ ,c("MonthlyCountRec", "MonthlyCount", "QuarterlyCountRec", "QuarterlyCount", "AnnualCountRec", "AnnualCount",
                                 "Year_Month", "Year_Quarter", "Year") := NULL];
  mergedLVTSTXNsYearsSpecYM$Date <- as.Date(mergedLVTSTXNsYearsSpecYM$date.time);
  mergedLVTSTXNsYearsSpecYM[ ,c("date.time") := NULL];
  #If aggregation should be at tranche level or overall payment
  if(byTranche & !byStream){
    mergedLVTSTXNsYearsSpecYM  <-  mergedLVTSTXNsYearsSpecYM[, list(TotalValue=sum(amount), TotalVolume=max(DailyCount), TotalVolumeRec=max(DailyCountRec)), 
                                                               by=c("date","Date","sender","receiver","tranche", "settlementMechanism")];
  }else if(byTranche & byStream){
    mergedLVTSTXNsYearsSpecYM  <-  mergedLVTSTXNsYearsSpecYM[, list(TotalValue=sum(amount), TotalVolume=max(DailyCount), TotalVolumeRec=max(DailyCountRec)), 
                                                             by=c("date","Date","sender","receiver","tranche", "settlementMechanism", "stream")];
  }else{
    mergedLVTSTXNsYearsSpecYM  <-  mergedLVTSTXNsYearsSpecYM[, list(TotalValue=sum(amount), TotalVolume=max(DailyCount), TotalVolumeRec=max(DailyCountRec)), 
                                                               by=c("date","Date","sender","receiver")];
  }


}else if(dataFrequency == "dateTime"){
  mergedLVTSTXNsYearsSpecYM[ ,c("MonthlyCount", "QuarterlyCount", "AnnualCountRec", "AnnualCount", "date", "Year_Month", "Year_Quarter", "Year") := NULL];
  mergedLVTSTXNsYearsSpecYM$Date <- as.Date(mergedLVTSTXNsYearsSpecYM$date.time);
  #If aggregation should be at tranche level or overall payment
  if(byTranche & !byStream){
    mergedLVTSTXNsYearsSpecYM  <-  mergedLVTSTXNsYearsSpecYM[, list(TotalValue=sum(amount), TotalVolume=max(DailyCount), TotalVolumeRec=max(DailyCountRec)), 
                                                             by=c("date.time","sender","receiver","tranche", "settlementMechanism")];
  }else if(byTranche & byStream){
    mergedLVTSTXNsYearsSpecYM  <-  mergedLVTSTXNsYearsSpecYM[, list(TotalValue=sum(amount), TotalVolume=max(DailyCount), TotalVolumeRec=max(DailyCountRec)), 
                                                             by=c("date.time","sender","receiver","tranche", "settlementMechanism", "stream")];
  }else{
    mergedLVTSTXNsYearsSpecYM  <-  mergedLVTSTXNsYearsSpecYM[, list(TotalValue=sum(amount), TotalVolume=max(DailyCount), TotalVolumeRec=max(DailyCountRec)), 
                                                             by=c("date.time","sender","receiver")];
  }
  
}



gc(); #free up memory
gc(); #free up memory
save(mergedLVTSTXNsYearsSpecYM,file=mergedLVTSTXNsNameString);
if(dataFrequency!="dateTime"){
  write.csv2(mergedLVTSTXNsYearsSpecYM, file=mergedLVTSTXNsXLSNameString);
}else{
  mergedLVTSTXNsYearsSpecYM <- mergedLVTSTXNsYearsSpecYM[(year(Date)>=segStartYr & year(Date)<=segEndYr),];
  save(mergedLVTSTXNsYearsSpecYM,file=mergedLVTSTXNsNameSegmentString);
  write.csv2(mergedLVTSTXNsYearsSpecYM, file=mergedLVTSTXNsXLSNameString);
}

##Remove the working data table once processing is complete and free up memory
rm(mergedLVTSTXNsYearsSpecYM);
gc();
