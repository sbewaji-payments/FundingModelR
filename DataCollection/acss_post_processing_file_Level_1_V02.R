##This script acts to merge the ACSS data with the FI static data and assign the appropriate BIC Codes based on memberIDs.
##This is required because the ACSS data only records memberIDs and not their BIC code.
##Consequently, in order to maintain consistency with the ACSS dataset BIC codes must be assigned to the ACSS data
## 
##The Script will import the saved data.table extract from the "ACSS_processing_file_V##_Start-to-End_Year_Selective_Extraction.R" code
##as well as the saved StaticData file extracted using the "process_CPA-MemberNamesBICandIDs.R"code
##
##Final input data tables that the code will work with are 
##        a: mergedACSSTXNsYearsSpecYM - The ACSS data file
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
load(extractedACSSDataLoadPath);
gc();
print("data tables loaded");#used to record speed of computations
print(now());#used to record speed of computations
gc();
#mergedACSSTXNsYearsSpecYMSummarizedSenderReceiver  <-  as.data.frame(mergedACSSTXNsYearsSpecYM);#convert mergedACSSTXNsYearsSpecYM to a temporary data.frame object
#rm(mergedACSSTXNsYearsSpecYM);#remove mergedACSSTXNsYearsSpecYM to free up memory since not in use
gc(); #free up memory


#ensure the numeric fields are indeed numeric and not strings
mergedACSSTXNsYearsSpecYM$valueRecieved <- as.double(mergedACSSTXNsYearsSpecYM$valueRecieved);
mergedACSSTXNsYearsSpecYM$volumeRecieved <- as.double(mergedACSSTXNsYearsSpecYM$volumeRecieved);
mergedACSSTXNsYearsSpecYM$valueSent <- as.double(mergedACSSTXNsYearsSpecYM$valueSent);
mergedACSSTXNsYearsSpecYM$volumeSent <- as.double(mergedACSSTXNsYearsSpecYM$volumeSent);

##The following if-else statements are used to set the set the value for "total value" and "total volume" by the data frequency value.
##This has been taken out of the initial data collation script because of the resource intensive nature of the collation script as the data range increases
##The statment simply assignes the TotalValue and TotalVolume as the sum of the valueSent field and maximum value of the monthly, quarterly, annual and daily 
##count fields depending on the data frequency required
if(dataFrequency == "monthly"){
  mergedACSSTXNsYearsSpecYM <- mergedACSSTXNsYearsSpecYM[order(Year_Month),];
  mergedACSSTXNsYearsSpecYM  <-  mergedACSSTXNsYearsSpecYM[, list(volumeSent=sum(as.numeric(volumeSent)), valueSent=sum(as.numeric(valueSent)),
                                                                    volumeRecieved=sum(as.numeric(volumeRecieved)),valueRecieved=sum(as.numeric(valueRecieved)),
                                                                    TotalVolume=sum(as.numeric(volumeSent)), 
                                                                    TotalValue=sum(as.numeric(valueSent))), 
                                                             by=c("Year_Month","stream","senderID","receiverID")];

}else if(dataFrequency == "quarterly"){
  mergedACSSTXNsYearsSpecYM <- mergedACSSTXNsYearsSpecYM[order(Year_Quarter),];
  mergedACSSTXNsYearsSpecYM  <-  mergedACSSTXNsYearsSpecYM[, list(volumeSent=sum(as.numeric(volumeSent)), valueSent=sum(as.numeric(valueSent)),
                                                                    volumeRecieved=sum(as.numeric(volumeRecieved)),valueRecieved=sum(as.numeric(valueRecieved)),
                                                                    TotalVolume=sum(as.numeric(volumeSent)), 
                                                                    TotalValue=sum(as.numeric(valueSent))), 
                                                             by=c("Year_Quarter","stream","senderID","receiverID")];


}else if(dataFrequency == "annual"){
  mergedACSSTXNsYearsSpecYM <- mergedACSSTXNsYearsSpecYM[order(Year),];
  mergedACSSTXNsYearsSpecYM  <-  mergedACSSTXNsYearsSpecYM[, list(volumeSent=sum(as.numeric(volumeSent)), valueSent=sum(as.numeric(valueSent)),
                                                                    volumeRecieved=sum(as.numeric(volumeRecieved)),valueRecieved=sum(as.numeric(valueRecieved)),
                                                                    TotalVolume=sum(as.numeric(volumeSent)), 
                                                                    TotalValue=sum(as.numeric(valueSent))), 
                                                             by=c("Year","stream","senderID","receiverID")];


}else if(dataFrequency == "daily"){
  mergedACSSTXNsYearsSpecYM <- mergedACSSTXNsYearsSpecYM[order(date),];
  mergedACSSTXNsYearsSpecYM$Day <- yday(mergedACSSTXNsYearsSpecYM$date);
  mergedACSSTXNsYearsSpecYM  <-  mergedACSSTXNsYearsSpecYM[, list(volumeSent=sum(as.numeric(volumeSent)), valueSent=sum(as.numeric(valueSent)),
                                                                    volumeRecieved=sum(as.numeric(volumeRecieved)),valueRecieved=sum(as.numeric(valueRecieved)),
                                                                    TotalVolume=sum(as.numeric(volumeSent)), 
                                                                    TotalValue=sum(as.numeric(valueSent))), 
                                                             by=c("date","stream","senderID","receiverID")];


}else if(dataFrequency == "dateTime"){
  ##Commented out because the data is a netted daily value and volume and does not contain a time stamp
  #mergedACSSTXNsYearsSpecYM$Date <- yday(mergedACSSTXNsYearsSpecYM$date.time);
  #mergedACSSTXNsYearsSpecYM  <-  mergedACSSTXNsYearsSpecYM[, list(TotalValue=sum(valueSent), TotalVolume=sum(volumeSent)), 
  #                                                           by=c("date.time","senderID","receiverID")];

}


gc(); #free up memory
gc(); #free up memory
save(mergedACSSTXNsYearsSpecYM,file=mergedACSSTXNsNameString);
# if(dataFrequency!="dateTime"){
#   write.csv(mergedACSSTXNsYearsSpecYM, file=mergedACSSTXNsXLSNameString);
# }else{
#   write.csv(mergedACSSTXNsYearsSpecYM, file=mergedACSSTXNsXLSNameString);
# }


##return to the original directory
rm(mergedACSSTXNsYearsSpecYM);