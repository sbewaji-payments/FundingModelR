##This script acts to merge the USBE data with the FI static data and assign the appropriate BIC Codes based on memberIDs.
##This is required because the USBE data only records memberIDs and not their BIC code.
##Consequently, in order to maintain consistency with the USBE dataset BIC codes must be assigned to the USBE data
## 
##The Script will import the saved data.table extract from the "USBE_processing_file_V##_Start-to-End_Year_Selective_Extraction.R" code
##as well as the saved StaticData file extracted using the "process_CPA-MemberNamesBICandIDs.R"code
##
##Final input data tables that the code will work with are 
##        a: mergedUSBETXNsYearsSpecYM - The USBE data file
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
print(paste("data frequency is", dataFrequency, sep=" "));#used to record speed of computations
##Now load the individual FI's complete data tables
print("Loading data tables");#used to record speed of computations
print(now());#used to record speed of computations
load(extractedUSBEDataLoadPath);
gc();
print("data tables loaded");#used to record speed of computations
print(now());#used to record speed of computations
gc();
#mergedUSBETXNsYearsSpecYMSummarizedSenderReceiver  <-  as.data.frame(mergedUSBETXNsYearsSpecYM);#convert mergedUSBETXNsYearsSpecYM to a temporary data.frame object
#rm(mergedUSBETXNsYearsSpecYM);#remove mergedUSBETXNsYearsSpecYM to free up memory since not in use
gc(); #free up memory


#ensure the numeric fields are indeed numeric and not strings
mergedUSBETXNsYearsSpecYM$valueRecieved <- as.double(mergedUSBETXNsYearsSpecYM$valueRecieved);
mergedUSBETXNsYearsSpecYM$volumeRecieved <- as.double(mergedUSBETXNsYearsSpecYM$volumeRecieved);
mergedUSBETXNsYearsSpecYM$valueSent <- as.double(mergedUSBETXNsYearsSpecYM$valueSent);
mergedUSBETXNsYearsSpecYM$volumeSent <- as.double(mergedUSBETXNsYearsSpecYM$volumeSent);

##The following if-else statements are used to set the set the value for "total value" and "total volume" by the data frequency value.
##This has been taken out of the initial data collation script because of the resource intensive nature of the collation script as the data range increases
##The statment simply assignes the TotalValue and TotalVolume as the sum of the valueSent field and maximum value of the monthly, quarterly, annual and daily 
##count fields depending on the data frequency required
if(dataFrequency == "monthly"){
  mergedUSBETXNsYearsSpecYM  <-  mergedUSBETXNsYearsSpecYM[, list(volumeSent=sum(as.numeric(volumeSent)), valueSent=sum(as.numeric(valueSent)),
                                                                    volumeRecieved=sum(as.numeric(volumeRecieved)),valueRecieved=sum(as.numeric(valueRecieved)),
                                                                    TotalVolume=sum(as.numeric(volumeSent)), 
                                                                    TotalValue=sum(as.numeric(valueSent))), 
                                                             by=c("Year_Month","stream","senderID","receiverID")];

}else if(dataFrequency == "quarterly"){
  mergedUSBETXNsYearsSpecYM  <-  mergedUSBETXNsYearsSpecYM[, list(volumeSent=sum(as.numeric(volumeSent)), valueSent=sum(as.numeric(valueSent)),
                                                                    volumeRecieved=sum(as.numeric(volumeRecieved)),valueRecieved=sum(as.numeric(valueRecieved)),
                                                                    TotalVolume=sum(as.numeric(volumeSent)), 
                                                                    TotalValue=sum(as.numeric(valueSent))), 
                                                             by=c("Year_Quarter","stream","senderID","receiverID")];


}else if(dataFrequency == "annual"){
  mergedUSBETXNsYearsSpecYM  <-  mergedUSBETXNsYearsSpecYM[, list(volumeSent=sum(as.numeric(volumeSent)), valueSent=sum(as.numeric(valueSent)),
                                                                    volumeRecieved=sum(as.numeric(volumeRecieved)),valueRecieved=sum(as.numeric(valueRecieved)),
                                                                    TotalVolume=sum(as.numeric(volumeSent)), 
                                                                    TotalValue=sum(as.numeric(valueSent))), 
                                                             by=c("Year","stream","senderID","receiverID")];


}else if(dataFrequency == "daily"){
  mergedUSBETXNsYearsSpecYM$Day <- yday(mergedUSBETXNsYearsSpecYM$date);
  mergedUSBETXNsYearsSpecYM  <-  mergedUSBETXNsYearsSpecYM[, list(volumeSent=sum(as.numeric(volumeSent)), valueSent=sum(as.numeric(valueSent)),
                                                                    volumeRecieved=sum(as.numeric(volumeRecieved)),valueRecieved=sum(as.numeric(valueRecieved)),
                                                                    TotalVolume=sum(as.numeric(volumeSent)), 
                                                                    TotalValue=sum(as.numeric(valueSent))), 
                                                             by=c("Day","stream","senderID","receiverID")];


}else if(dataFrequency == "dateTime"){
  ##Commented out because the data is a netted daily value and volume and does not contain a time stamp
  #mergedUSBETXNsYearsSpecYM$Date <- yday(mergedUSBETXNsYearsSpecYM$date.time);
  #mergedUSBETXNsYearsSpecYM  <-  mergedUSBETXNsYearsSpecYM[, list(TotalValue=sum(valueSent), TotalVolume=sum(volumeSent)), 
  #                                                           by=c("date.time","senderID","receiverID")];

}


gc(); #free up memory
gc(); #free up memory
save(mergedUSBETXNsYearsSpecYM,file=mergedUSBETXNsNameString);
if(dataFrequency!="dateTime"){
  write.csv2(mergedUSBETXNsYearsSpecYM, file=mergedUSBETXNsXLSNameString);
}else{
  write.csv2(mergedUSBETXNsYearsSpecYM, file=mergedUSBETXNsXLSNameString);
}


##return to the original directory
rm(mergedUSBETXNsYearsSpecYM);