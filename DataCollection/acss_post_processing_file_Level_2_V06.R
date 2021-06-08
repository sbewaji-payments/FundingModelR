##This script acts to merge the ACSS data with the FI static data and assign the appropriate BIC Codes based on memberIDs.
##This is required because the ACSS data only records memberIDs and not their BIC code.
##Consequently, in order to maintain consistency with the ACSS dataset BIC codes must be assigned to the ACSS data
## 
##The Script will import the saved data.table extract from the "acss_processing_file_V##_Start-to-End_Year_Selective_Extraction.R" code
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
## Modifications Made: 
##        1) inclusion of the TotalValue and TotalVolume 
##          Code
##          (ACSSTXNsDataSeriesSentRecieved<- ACSSTXNsDataSeriesSentRecieved[, c("Total_Volume") := sum(RecievedVolume, SentVolume), 
##            with = FALSE, by=c("sender","Year_Quarter")];)
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


##<<<<<<<<<<<<<<<<<<<<<<<<<LOAD DATA>>>>>>>>>>>>>>>>>>>>>>>>
##Now load the individual FI's complete data tables
print("Loading data tables");#used to record speed of computations
print(now());#used to record speed of computations
load(fiStaticDataLoadPath);
load(RETAILStreamDataLoadPath);
load(extractedSummaryACSSDataLoadPath);
gc();
print("data tables loaded");#used to record speed of computations
print(now());#used to record speed of computations
gc();
mergedACSSTXNsYearsSpecYM_df  <-  as.data.frame(mergedACSSTXNsYearsSpecYM);#convert mergedACSSTXNsYearsSpecYM to a temporary data.frame object
fiStaticDataFile_df  <-  as.data.frame(fiStaticDataFile);#convert fiStaticDataFile to a temporary data.frame object
ACSSStreamDataFile_df  <- as.data.frame(RETAILStreamData);
rm(mergedACSSTXNsYearsSpecYM);#remove mergedACSSTXNsYearsSpecYM to free up memory since not in use
rm(fiStaticDataFile);#remove fiStaticDataFile to free up memory since not in use
rm(RETAILStreamData);#remove RETAILStreamData to free up memory since not in use


##merge mergedACSSTXNsYearsSpecYM_df and fiStaticDataFile_df data.frame objects into the final mergedACSSTXNsYearsSpecYM data table object
##starting by merging using the recieving member ID numbers followed by the sending member ID numbers.
##after each merge, the memberBIC and memberName columns will be renamed to 
##"receiver"("sender") and "receiverFullName"("senderFullName") as appropriate
##starting with the "receiver"
mergedACSSTXNsYearsSpecYM  <-  merge(mergedACSSTXNsYearsSpecYM_df, fiStaticDataFile_df, by.x="receiverID", by.y="memberID");
colnames(mergedACSSTXNsYearsSpecYM)[colnames(mergedACSSTXNsYearsSpecYM)=="memberBIC"] <- "receiver";
colnames(mergedACSSTXNsYearsSpecYM)[colnames(mergedACSSTXNsYearsSpecYM)=="memberName"] <- "receiverFullName";

rm(mergedACSSTXNsYearsSpecYM_df);#remove mergedACSSTXNsYearsSpecYM_df to free up memory since not in use
gc(); #free up memory

##next the "sender"
##Note that for the sender details the code uses the main working data.frame object mergedACSSTXNsYearsSpecYM and not
##the temporary one mergedACSSTXNsYearsSpecYM_df
mergedACSSTXNsYearsSpecYM  <-  merge(mergedACSSTXNsYearsSpecYM, fiStaticDataFile_df, by.x="senderID", by.y="memberID");
colnames(mergedACSSTXNsYearsSpecYM)[colnames(mergedACSSTXNsYearsSpecYM)=="memberBIC"] <- "sender";
colnames(mergedACSSTXNsYearsSpecYM)[colnames(mergedACSSTXNsYearsSpecYM)=="memberName"] <- "senderFullName";

rm(fiStaticDataFile_df);#remove fiStaticDataFile_df to free up memory since not in use
gc(); #free up memory

##Now merge the stream description data with the transaction data
mergedACSSTXNsYearsSpecYM  <-  merge(mergedACSSTXNsYearsSpecYM, ACSSStreamDataFile_df, by.x="stream", by.y="streamID", all.x = TRUE, all.y = TRUE);
gc(); #free up memory

mergedACSSTXNsYearsSpecYM  <-  as.data.table(mergedACSSTXNsYearsSpecYM);#convert mergedACSSTXNsYearsSpecYM to a data.table object
##drop any cases where the rows contain NAs
##commented out because the NAs seem to be only related to the stream discription and stream type columns
##Will need to update this in the stream data file
##This will break if the RetailStreamData file is not propoerly updated to reflect all possible streams in the retail system 
mergedACSSTXNsYearsSpecYM  <-  mergedACSSTXNsYearsSpecYM[complete.cases(mergedACSSTXNsYearsSpecYM),];
##remove extra columns added
#mergedACSSTXNsYearsSpecYM  <-  mergedACSSTXNsYearsSpecYM[ ,c("Year_Month","Year_Quarter","Year") := NULL];
gc(); #free up memory


#ensure the numeric fields are indeed numeric and not strings
mergedACSSTXNsYearsSpecYM$valueRecieved <- as.double(mergedACSSTXNsYearsSpecYM$valueRecieved);
mergedACSSTXNsYearsSpecYM$volumeRecieved <- as.double(mergedACSSTXNsYearsSpecYM$volumeRecieved);
mergedACSSTXNsYearsSpecYM$valueSent <- as.double(mergedACSSTXNsYearsSpecYM$valueSent);
mergedACSSTXNsYearsSpecYM$volumeSent <- as.double(mergedACSSTXNsYearsSpecYM$volumeSent);
gc(); #free up memory


## Now create tables for each data frequency
## NOTE: 
## With the ACSS data, "sent" and "recieved" refer to messages "sent" for payment and messaged "recieved" for payment.
## So the column names must be switched around to match the actual flow of funds/payments.
## Hence "SentVolume=sum(volumeRecieved)" "RecievedVolume=sum(volumeSent)" etc
if(byStream == FALSE){
  
  if(dataFrequency == "monthly"){
    mergedACSSTXNsYearsSpecYM <- mergedACSSTXNsYearsSpecYM[order(Year_Month),];
    
    ACSSTXNsDataSeriesBySender <-  mergedACSSTXNsYearsSpecYM[, list(SentVolume=sum(volumeRecieved), RecievedVolume=sum(volumeSent),
                                                                      TotalVolume=sum(volumeRecieved, volumeSent),
                                                                      SentValue=sum(valueRecieved), RecievedValue=sum(valueSent),
                                                                      TotalValue=sum(valueRecieved, valueSent)
    ), 
    by=c("sender","senderID","senderFullName","Year_Month")];
    ACSSTXNsDataSeriesBySender  <-  ACSSTXNsDataSeriesBySender[order(Year_Month, sender), ];
    
    
    ACSSTXNsDataSeriesByPeriod <-  mergedACSSTXNsYearsSpecYM[, list(SentVolume=sum(volumeRecieved), RecievedVolume=sum(volumeSent),
                                                                      TotalVolume=sum(volumeRecieved, volumeSent),
                                                                      SentValue=sum(valueRecieved), RecievedValue=sum(valueSent),
                                                                      TotalValue=sum(valueRecieved, valueSent)
    ), 
    by=c("Year_Month")];
    
    ACSSTXNsDataSeriesByPeriod  <-  ACSSTXNsDataSeriesByPeriod[order(Year_Month), ];
    
    ACSSTXNsDataSeriesBySenderReceiver <-  mergedACSSTXNsYearsSpecYM[, list(SentVolume=sum(volumeRecieved), RecievedVolume=sum(volumeSent),
                                                                              TotalVolume=sum(volumeRecieved, volumeSent),
                                                                              SentValue=sum(valueRecieved), RecievedValue=sum(valueSent),
                                                                              TotalValue=sum(valueRecieved, valueSent)
    ), 
    by=c("Year_Month","sender","senderID","senderFullName","receiver","receiverID","receiverFullName")];
    
    ACSSTXNsDataSeriesBySenderReceiver  <-  ACSSTXNsDataSeriesBySenderReceiver[order(Year_Month, sender, receiver), ];
    
    ##Note that the by FI sent and recieved data has to be collated by way of the sender because of the way the ACSS extract files are generated
    ##i.e. each line record in the extract is from the persepcetive of the sender at a point in time. Hence in each line sender X sends and recieves 
    ##some volume and value of payments to and from reciever Y
    ACSSTXNsDataSeriesSent  <-  mergedACSSTXNsYearsSpecYM[, list(SentVolume=sum(volumeRecieved), SentValue=sum(valueRecieved)
    ), 
    by=c("sender","senderID","senderFullName","Year_Month")];
    
    ACSSTXNsDataSeriesRecieved  <-  mergedACSSTXNsYearsSpecYM[, list(RecievedVolume=sum(volumeSent), RecievedValue=sum(valueSent)
    ), 
    by=c("sender","senderID","senderFullName","Year_Month")];
    
    
    ACSSTXNsDataSeriesSent_df <-  as.data.frame(ACSSTXNsDataSeriesSent);
    ACSSTXNsDataSeriesRecieved_df <-  as.data.frame(ACSSTXNsDataSeriesRecieved);
    ACSSTXNsDataSeriesSentRecieved_df  <-  merge(ACSSTXNsDataSeriesSent_df, ACSSTXNsDataSeriesRecieved_df, by.x=c("sender","Year_Month"), 
                                                  by.y=c("sender","Year_Month"), all.x = TRUE, all.y = TRUE);
    
    
    ACSSTXNsDataSeriesSentRecieved_df <- do.call("rbind", as.list(by(ACSSTXNsDataSeriesSentRecieved_df, 
                                                                      ACSSTXNsDataSeriesSentRecieved_df["Year_Month"], 
                                                                      transform, ShareSentVolume=fnShare(SentVolume))));
    ACSSTXNsDataSeriesSentRecieved_df <- do.call("rbind", as.list(by(ACSSTXNsDataSeriesSentRecieved_df, 
                                                                      ACSSTXNsDataSeriesSentRecieved_df["Year_Month"], 
                                                                      transform, ShareSentValue=fnShare(SentValue))));
    ACSSTXNsDataSeriesSentRecieved_df <- do.call("rbind", as.list(by(ACSSTXNsDataSeriesSentRecieved_df, 
                                                                      ACSSTXNsDataSeriesSentRecieved_df["Year_Month"], 
                                                                      transform, ShareRecievedVolume=fnShare(RecievedVolume))));
    ACSSTXNsDataSeriesSentRecieved_df <- do.call("rbind", as.list(by(ACSSTXNsDataSeriesSentRecieved_df, 
                                                                      ACSSTXNsDataSeriesSentRecieved_df["Year_Month"], 
                                                                      transform, ShareRecievedValue=fnShare(RecievedValue))));
    
    ACSSTXNsDataSeriesSentRecieved <-  as.data.table(ACSSTXNsDataSeriesSentRecieved_df);
    ACSSTXNsDataSeriesSentRecieved <- ACSSTXNsDataSeriesSentRecieved[, c("TotalVolume") := sum(RecievedVolume, SentVolume), 
                                                                       with = FALSE, by=c("sender","Year_Month")];
    ACSSTXNsDataSeriesSentRecieved <- ACSSTXNsDataSeriesSentRecieved[, c("TotalValue") := sum(RecievedValue, SentValue), 
                                                                       with = FALSE, by=c("sender","Year_Month")];
    
    ##Remove and rename duplicated fields
    ACSSTXNsDataSeriesSentRecieved[ ,c("senderID.y", "senderFullName.y") := NULL];
    setnames(ACSSTXNsDataSeriesSentRecieved,"senderID.x","senderID");
    setnames(ACSSTXNsDataSeriesSentRecieved,"senderFullName.x","senderFullName");
    
    
    rm(ACSSTXNsDataSeriesSent_df);
    rm(ACSSTXNsDataSeriesRecieved_df);
    rm(ACSSTXNsDataSeriesSentRecieved_df);
    
    
  }else if(dataFrequency == "quarterly"){
    mergedACSSTXNsYearsSpecYM <- mergedACSSTXNsYearsSpecYM[order(Year_Quarter),];
    
    ACSSTXNsDataSeriesBySender <-  mergedACSSTXNsYearsSpecYM[, list(SentVolume=sum(volumeRecieved), RecievedVolume=sum(volumeSent),
                                                                      TotalVolume=sum(volumeRecieved, volumeSent),
                                                                      SentValue=sum(valueRecieved), RecievedValue=sum(valueSent),
                                                                      TotalValue=sum(valueRecieved, valueSent)
    ), 
    by=c("sender","senderFullName","Year_Quarter")];
    
    ACSSTXNsDataSeriesBySender <-  ACSSTXNsDataSeriesBySender[order(Year_Quarter, sender), ];
    
    ACSSTXNsDataSeriesByPeriod <-  mergedACSSTXNsYearsSpecYM[, list(SentVolume=sum(volumeRecieved), RecievedVolume=sum(volumeSent),
                                                                      TotalVolume=sum(volumeRecieved, volumeSent),
                                                                      SentValue=sum(valueRecieved), RecievedValue=sum(valueSent),
                                                                      TotalValue=sum(valueRecieved, valueSent)
    ), 
    by=c("Year_Quarter")];
    ACSSTXNsDataSeriesByPeriod <-  ACSSTXNsDataSeriesByPeriod[order(Year_Quarter), ];
    
    ACSSTXNsDataSeriesBySenderReceiver <-  mergedACSSTXNsYearsSpecYM[, list(SentVolume=sum(volumeRecieved), RecievedVolume=sum(volumeSent),
                                                                              TotalVolume=sum(volumeRecieved, volumeSent),
                                                                              SentValue=sum(valueRecieved), RecievedValue=sum(valueSent),
                                                                              TotalValue=sum(valueRecieved, valueSent)
    ), 
    by=c("Year_Quarter","sender","senderID","receiver","receiverID")];
    
    ACSSTXNsDataSeriesBySenderReceiver <-  ACSSTXNsDataSeriesBySenderReceiver[order(Year_Quarter, sender, receiver), ];
    
    ##Note that the by FI sent and recieved data has to be collated by way of the sender because of the way the ACSS extract files are generated
    ##i.e. each line record in the extract is from the persepcetive of the sender at a point in time. Hence in each line sender X sends and recieves 
    ##some volume and value of payments to and from reciever Y
    ACSSTXNsDataSeriesSent  <-  mergedACSSTXNsYearsSpecYM[, list(SentVolume=sum(volumeRecieved), SentValue=sum(valueRecieved)
    ), 
    by=c("sender","senderID","senderFullName","Year_Quarter")];
    
    ACSSTXNsDataSeriesRecieved  <-  mergedACSSTXNsYearsSpecYM[, list(RecievedVolume=sum(volumeSent), RecievedValue=sum(valueSent)
    ), 
    by=c("sender","senderID","senderFullName","Year_Quarter")];
    
    
    
    
    ACSSTXNsDataSeriesSent_df <-  as.data.frame(ACSSTXNsDataSeriesSent);
    ACSSTXNsDataSeriesRecieved_df <-  as.data.frame(ACSSTXNsDataSeriesRecieved);
    ACSSTXNsDataSeriesSentRecieved_df <-  merge(ACSSTXNsDataSeriesSent_df, ACSSTXNsDataSeriesRecieved_df, by.x="sender", by.y="sender");
    ACSSTXNsDataSeriesSentRecieved_df  <-  merge(ACSSTXNsDataSeriesSent_df, ACSSTXNsDataSeriesRecieved_df, by.x=c("sender","Year_Quarter"), 
                                                  by.y=c("sender","Year_Quarter"), all.x = TRUE, all.y = TRUE);
    
    
    ACSSTXNsDataSeriesSentRecieved_df <- do.call("rbind", as.list(by(ACSSTXNsDataSeriesSentRecieved_df, 
                                                                      ACSSTXNsDataSeriesSentRecieved_df["Year_Quarter"], 
                                                                      transform, ShareSentVolume=fnShare(SentVolume))));
    ACSSTXNsDataSeriesSentRecieved_df <- do.call("rbind", as.list(by(ACSSTXNsDataSeriesSentRecieved_df, 
                                                                      ACSSTXNsDataSeriesSentRecieved_df["Year_Quarter"], 
                                                                      transform, ShareSentValue=fnShare(SentValue))));
    ACSSTXNsDataSeriesSentRecieved_df <- do.call("rbind", as.list(by(ACSSTXNsDataSeriesSentRecieved_df, 
                                                                      ACSSTXNsDataSeriesSentRecieved_df["Year_Quarter"], 
                                                                      transform, ShareRecievedVolume=fnShare(RecievedVolume))));
    ACSSTXNsDataSeriesSentRecieved_df <- do.call("rbind", as.list(by(ACSSTXNsDataSeriesSentRecieved_df, 
                                                                      ACSSTXNsDataSeriesSentRecieved_df["Year_Quarter"], 
                                                                      transform, ShareRecievedValue=fnShare(RecievedValue))));
    
    
    ACSSTXNsDataSeriesSentRecieved <-  as.data.table(ACSSTXNsDataSeriesSentRecieved_df);
    ACSSTXNsDataSeriesSentRecieved <- ACSSTXNsDataSeriesSentRecieved[, c("TotalVolume") := sum(RecievedVolume, SentVolume), 
                                                                       with = FALSE, by=c("sender","Year_Quarter")];
    ACSSTXNsDataSeriesSentRecieved <- ACSSTXNsDataSeriesSentRecieved[, c("TotalValue") := sum(RecievedValue, SentValue), 
                                                                       with = FALSE, by=c("sender","Year_Quarter")];
    
    ##Remove and rename duplicated fields
    ACSSTXNsDataSeriesSentRecieved[ ,c("senderID.y", "senderFullName.y") := NULL];
    setnames(ACSSTXNsDataSeriesSentRecieved,"senderID.x","senderID");
    setnames(ACSSTXNsDataSeriesSentRecieved,"senderFullName.x","senderFullName");
    
    rm(ACSSTXNsDataSeriesSent_df);
    rm(ACSSTXNsDataSeriesRecieved_df);
    rm(ACSSTXNsDataSeriesSentRecieved_df);
    
  }else if(dataFrequency == "annual"){
    mergedACSSTXNsYearsSpecYM <- mergedACSSTXNsYearsSpecYM[order(Year),];
    
    ACSSTXNsDataSeriesBySender  <-  mergedACSSTXNsYearsSpecYM[, list(SentVolume=sum(volumeRecieved), RecievedVolume=sum(volumeSent),
                                                                       TotalVolume=sum(volumeRecieved, volumeSent),
                                                                       SentValue=sum(valueRecieved), RecievedValue=sum(valueSent),
                                                                       TotalValue=sum(valueRecieved, valueSent)
    ), 
    by=c("sender","senderID","senderFullName","Year")];
    
    ACSSTXNsDataSeriesBySender <-  ACSSTXNsDataSeriesBySender[order(Year, sender), ];
    
    ACSSTXNsDataSeriesByPeriod  <-  mergedACSSTXNsYearsSpecYM[, list(SentVolume=sum(volumeRecieved), RecievedVolume=sum(volumeSent),
                                                                       TotalVolume=sum(volumeRecieved, volumeSent),
                                                                       SentValue=sum(valueRecieved), RecievedValue=sum(valueSent),
                                                                       TotalValue=sum(valueRecieved, valueSent)
    ), 
    by=c("Year")];
    ACSSTXNsDataSeriesByPeriod <-  ACSSTXNsDataSeriesByPeriod[order(Year), ];
    
    ACSSTXNsDataSeriesBySenderReceiver  <-  mergedACSSTXNsYearsSpecYM[, list(SentVolume=sum(volumeRecieved), RecievedVolume=sum(volumeSent),
                                                                               TotalVolume=sum(volumeRecieved, volumeSent),
                                                                               SentValue=sum(valueRecieved), RecievedValue=sum(valueSent),
                                                                               TotalValue=sum(valueRecieved, valueSent)
    ), 
    by=c("Year","sender","senderID","senderFullName","receiver","receiverID","receiverFullName")];
    
    ACSSTXNsDataSeriesBySenderReceiver <-  ACSSTXNsDataSeriesBySenderReceiver[order(Year, sender, receiver), ];
    
    ##Note that the by FI sent and recieved data has to be collated by way of the sender because of the way the ACSS extract files are generated
    ##i.e. each line record in the extract is from the persepcetive of the sender at a point in time. Hence in each line sender X sends and recieves 
    ##some volume and value of payments to and from reciever Y
    ACSSTXNsDataSeriesSent  <-  mergedACSSTXNsYearsSpecYM[, list(SentVolume=sum(volumeRecieved), SentValue=sum(valueRecieved)
    ), 
    by=c("sender","senderID","senderFullName","Year")];
    
    ACSSTXNsDataSeriesRecieved  <-  mergedACSSTXNsYearsSpecYM[, list(RecievedVolume=sum(volumeSent), RecievedValue=sum(valueSent)
    ), 
    by=c("sender","senderID","senderFullName","Year")];
    
    
    
    ACSSTXNsDataSeriesSent_df <-  as.data.frame(ACSSTXNsDataSeriesSent);
    ACSSTXNsDataSeriesRecieved_df <-  as.data.frame(ACSSTXNsDataSeriesRecieved);
    ACSSTXNsDataSeriesSentRecieved_df <-  merge(ACSSTXNsDataSeriesSent_df, ACSSTXNsDataSeriesRecieved_df, by.x="sender", by.y="sender");
    ACSSTXNsDataSeriesSentRecieved_df  <-  merge(ACSSTXNsDataSeriesSent_df, ACSSTXNsDataSeriesRecieved_df, by.x=c("sender","Year"), 
                                                  by.y=c("sender","Year"), all.x = TRUE, all.y = TRUE);
    
    
    
    ACSSTXNsDataSeriesSentRecieved_df <- do.call("rbind", as.list(by(ACSSTXNsDataSeriesSentRecieved_df, 
                                                                      ACSSTXNsDataSeriesSentRecieved_df["Year"], 
                                                                      transform, ShareSentVolume=fnShare(SentVolume))));
    ACSSTXNsDataSeriesSentRecieved_df <- do.call("rbind", as.list(by(ACSSTXNsDataSeriesSentRecieved_df, 
                                                                      ACSSTXNsDataSeriesSentRecieved_df["Year"], 
                                                                      transform, ShareSentValue=fnShare(SentValue))));
    ACSSTXNsDataSeriesSentRecieved_df <- do.call("rbind", as.list(by(ACSSTXNsDataSeriesSentRecieved_df, 
                                                                      ACSSTXNsDataSeriesSentRecieved_df["Year"], 
                                                                      transform, ShareRecievedVolume=fnShare(RecievedVolume))));
    ACSSTXNsDataSeriesSentRecieved_df <- do.call("rbind", as.list(by(ACSSTXNsDataSeriesSentRecieved_df, 
                                                                      ACSSTXNsDataSeriesSentRecieved_df["Year"], 
                                                                      transform, ShareRecievedValue=fnShare(RecievedValue))));
    
    
    ACSSTXNsDataSeriesSentRecieved <-  as.data.table(ACSSTXNsDataSeriesSentRecieved_df);
    
    ACSSTXNsDataSeriesSentRecieved <- ACSSTXNsDataSeriesSentRecieved[, c("TotalVolume") := sum(RecievedVolume, SentVolume), 
                                                                       with = FALSE, by=c("sender","Year")];
    ACSSTXNsDataSeriesSentRecieved <- ACSSTXNsDataSeriesSentRecieved[, c("TotalValue") := sum(RecievedValue, SentValue), 
                                                                       with = FALSE, by=c("sender","Year")];
    ##Remove and rename duplicated fields
    ACSSTXNsDataSeriesSentRecieved[ ,c("senderID.y", "senderFullName.y") := NULL];
    setnames(ACSSTXNsDataSeriesSentRecieved,"senderID.x","senderID");
    setnames(ACSSTXNsDataSeriesSentRecieved,"senderFullName.x","senderFullName");
    
    
    rm(ACSSTXNsDataSeriesSent_df);
    rm(ACSSTXNsDataSeriesRecieved_df);
    rm(ACSSTXNsDataSeriesSentRecieved_df);
    
  }else if(dataFrequency == "daily"){
    mergedACSSTXNsYearsSpecYM <- mergedACSSTXNsYearsSpecYM[order(Day),];
    ACSSTXNsDataSeriesBySender  <-  mergedACSSTXNsYearsSpecYM[, list(SentVolume=sum(TotalVolume),RecievedVolume=sum(TotalVolume),
                                                                       TotalVolume=sum(volumeRecieved, volumeSent),
                                                                       SentValue=sum(TotalValue), RecievedValue=sum(TotalValue),
                                                                       TotalValue=sum(valueRecieved, valueSent)
    ), 
    by=c("sender","senderID","senderFullName","date")];

    ACSSTXNsDataSeriesBySender <-   ACSSTXNsDataSeriesBySender[order(date, sender), ];
    
    ACSSTXNsDataSeriesByPeriod  <-  mergedACSSTXNsYearsSpecYM[, list(SentVolume=sum(TotalVolume),RecievedVolume=sum(TotalVolume),
                                                                       TotalVolume=sum(volumeRecieved, volumeSent),
                                                                       SentValue=sum(TotalValue), RecievedValue=sum(TotalValue),
                                                                       TotalValue=sum(valueRecieved, valueSent)
    ),
    by=c("date")];
    ACSSTXNsDataSeriesByPeriod <-   ACSSTXNsDataSeriesByPeriod[order(date), ];
    
    ACSSTXNsDataSeriesBySenderReceiver  <-  mergedACSSTXNsYearsSpecYM[, list(SentVolume=sum(TotalVolume),RecievedVolume=sum(TotalVolume),
                                                                               TotalVolume=sum(volumeRecieved, volumeSent),
                                                                               SentValue=sum(TotalValue), RecievedValue=sum(TotalValue),
                                                                               TotalValue=sum(valueRecieved, valueSent)
    ), 
    by=c("date","sender","senderID","senderFullName","receiver","receiverID","receiverFullName")];
    
    ACSSTXNsDataSeriesBySenderReceiver <- ACSSTXNsDataSeriesBySenderReceiver[order(date, sender, receiver), ];
    
    
    ACSSTXNsDataSeriesSent  <-  mergedACSSTXNsYearsSpecYM[, list(SentVolume=sum(TotalVolume), SentValue=sum(TotalValue)
    ), 
    by=c("sender","date")];
    
    ACSSTXNsDataSeriesRecieved  <-  mergedACSSTXNsYearsSpecYM[, list(RecievedVolume=sum(TotalVolume), RecievedValue=sum(TotalValue)
    ), 
    by=c("sender","date")];
    
    
    ACSSTXNsDataSeriesSent_df <-  as.data.frame(ACSSTXNsDataSeriesSent);
    ACSSTXNsDataSeriesRecieved_df <-  as.data.frame(ACSSTXNsDataSeriesRecieved);
    ACSSTXNsDataSeriesSentRecieved_df  <-  merge(ACSSTXNsDataSeriesSent_df, ACSSTXNsDataSeriesRecieved_df, by.x=c("sender","date"), 
                                                  by.y=c("sender","date"),all.x = TRUE, all.y = TRUE);
    
    ACSSTXNsDataSeriesSentRecieved_df <- do.call("rbind", as.list(by(ACSSTXNsDataSeriesSentRecieved_df, 
                                                                      ACSSTXNsDataSeriesSentRecieved_df["date"], 
                                                                      transform, ShareSentVolume=fnShare(SentVolume))));
    ACSSTXNsDataSeriesSentRecieved_df <- do.call("rbind", as.list(by(ACSSTXNsDataSeriesSentRecieved_df, 
                                                                      ACSSTXNsDataSeriesSentRecieved_df["date"], 
                                                                      transform, ShareSentValue=fnShare(SentValue))));
    ACSSTXNsDataSeriesSentRecieved_df <- do.call("rbind", as.list(by(ACSSTXNsDataSeriesSentRecieved_df, 
                                                                      ACSSTXNsDataSeriesSentRecieved_df["date"], 
                                                                      transform, ShareRecievedVolume=fnShare(RecievedVolume))));
    ACSSTXNsDataSeriesSentRecieved_df <- do.call("rbind", as.list(by(ACSSTXNsDataSeriesSentRecieved_df, 
                                                                      ACSSTXNsDataSeriesSentRecieved_df["date"], 
                                                                      transform, ShareRecievedValue=fnShare(RecievedValue))));
    
    
    ACSSTXNsDataSeriesSentRecieved <-  as.data.table(ACSSTXNsDataSeriesSentRecieved_df);
    ACSSTXNsDataSeriesSentRecieved<- ACSSTXNsDataSeriesSentRecieved[, c("TotalVolume") := sum(RecievedVolume, SentVolume), 
                                                                      with = FALSE, by=c("sender","date")];
    ACSSTXNsDataSeriesSentRecieved<- ACSSTXNsDataSeriesSentRecieved[, c("TotalValue") := sum(RecievedValue, SentValue), 
                                                                      with = FALSE, by=c("sender","date")];
    ##Remove and rename duplicated fields
    ACSSTXNsDataSeriesSentRecieved[ ,c("senderID.y", "senderFullName.y") := NULL];
    setnames(ACSSTXNsDataSeriesSentRecieved,"senderID.x","senderID");
    setnames(ACSSTXNsDataSeriesSentRecieved,"senderFullName.x","senderFullName");
    
    
    #Remove temporary working datasets from memory
    rm(ACSSTXNsDataSeriesSent_df);
    rm(ACSSTXNsDataSeriesRecieved_df);
    rm(ACSSTXNsDataSeriesSentRecieved_df);
    
  }else if(dataFrequency == "dateTime"){
    ##Unlike LVTS, ACSS and USBE data are netted and thus do not have the time dimension to them.
    ##This place holder has been left to mark where the code will go if at a later date this dimenssion is included
  }
  
}else{
  if(dataFrequency == "monthly"){
    
    ACSSTXNsDataSeriesBySender <-  mergedACSSTXNsYearsSpecYM[, list(SentVolume=sum(volumeRecieved), RecievedVolume=sum(volumeSent),
                                                                      TotalVolume=sum(volumeRecieved, volumeSent),
                                                                      SentValue=sum(valueRecieved), RecievedValue=sum(valueSent),
                                                                      TotalValue=sum(valueRecieved, valueSent)
    ), 
    by=c("sender","senderID","senderFullName","stream","Year_Month")];
    ACSSTXNsDataSeriesBySender  <-  ACSSTXNsDataSeriesBySender[order(Year_Month, sender), ];
    
    
    ACSSTXNsDataSeriesByPeriod <-  mergedACSSTXNsYearsSpecYM[, list(SentVolume=sum(volumeRecieved), RecievedVolume=sum(volumeSent),
                                                                      TotalVolume=sum(volumeRecieved, volumeSent),
                                                                      SentValue=sum(valueRecieved), RecievedValue=sum(valueSent),
                                                                      TotalValue=sum(valueRecieved, valueSent)
    ), 
    by=c("Year_Month","stream")];
    
    ACSSTXNsDataSeriesByPeriod  <-  ACSSTXNsDataSeriesByPeriod[order(Year_Month), ];
    
    ACSSTXNsDataSeriesBySenderReceiver <-  mergedACSSTXNsYearsSpecYM[, list(SentVolume=sum(volumeRecieved), RecievedVolume=sum(volumeSent),
                                                                              TotalVolume=sum(volumeRecieved, volumeSent),
                                                                              SentValue=sum(valueRecieved), RecievedValue=sum(valueSent),
                                                                              TotalValue=sum(valueRecieved, valueSent)
    ), 
    by=c("Year_Month","stream","sender","senderID","senderFullName","receiver","receiverID","receiverFullName")];
    
    ACSSTXNsDataSeriesBySenderReceiver  <-  ACSSTXNsDataSeriesBySenderReceiver[order(Year_Month, sender, receiver), ];
    
    ##Note that the by FI sent and recieved data has to be collated by way of the sender because of the way the ACSS extract files are generated
    ##i.e. each line record in the extract is from the persepcetive of the sender at a point in time. Hence in each line sender X sends and recieves 
    ##some volume and value of payments to and from reciever Y
    ACSSTXNsDataSeriesSent  <-  mergedACSSTXNsYearsSpecYM[, list(SentVolume=sum(volumeRecieved), SentValue=sum(valueRecieved)
    ), 
    by=c("sender","senderID","senderFullName","stream","Year_Month")];
    
    ACSSTXNsDataSeriesRecieved  <-  mergedACSSTXNsYearsSpecYM[, list(RecievedVolume=sum(volumeSent), RecievedValue=sum(valueSent)
    ), 
    by=c("sender","senderID","senderFullName","stream","Year_Month")];
    
    
    ACSSTXNsDataSeriesSent_df <-  as.data.frame(ACSSTXNsDataSeriesSent);
    ACSSTXNsDataSeriesRecieved_df <-  as.data.frame(ACSSTXNsDataSeriesRecieved);
    ACSSTXNsDataSeriesSentRecieved_df  <-  merge(ACSSTXNsDataSeriesSent_df, ACSSTXNsDataSeriesRecieved_df, by.x=c("sender","stream","Year_Month"), 
                                                  by.y=c("sender","stream","Year_Month"), all.x = TRUE, all.y = TRUE);
    
    
    ACSSTXNsDataSeriesSentRecieved_df <- do.call("rbind", as.list(by(ACSSTXNsDataSeriesSentRecieved_df, 
                                                                      ACSSTXNsDataSeriesSentRecieved_df["Year_Month"], 
                                                                      transform, ShareSentVolume=fnShare(SentVolume))));
    ACSSTXNsDataSeriesSentRecieved_df <- do.call("rbind", as.list(by(ACSSTXNsDataSeriesSentRecieved_df, 
                                                                      ACSSTXNsDataSeriesSentRecieved_df["Year_Month"], 
                                                                      transform, ShareSentValue=fnShare(SentValue))));
    ACSSTXNsDataSeriesSentRecieved_df <- do.call("rbind", as.list(by(ACSSTXNsDataSeriesSentRecieved_df, 
                                                                      ACSSTXNsDataSeriesSentRecieved_df["Year_Month"], 
                                                                      transform, ShareRecievedVolume=fnShare(RecievedVolume))));
    ACSSTXNsDataSeriesSentRecieved_df <- do.call("rbind", as.list(by(ACSSTXNsDataSeriesSentRecieved_df, 
                                                                      ACSSTXNsDataSeriesSentRecieved_df["Year_Month"], 
                                                                      transform, ShareRecievedValue=fnShare(RecievedValue))));
    
    ACSSTXNsDataSeriesSentRecieved <-  as.data.table(ACSSTXNsDataSeriesSentRecieved_df);
    ACSSTXNsDataSeriesSentRecieved <- ACSSTXNsDataSeriesSentRecieved[, c("TotalVolume") := sum(RecievedVolume, SentVolume), 
                                                                       with = FALSE, by=c("sender","stream","Year_Month")];
    ACSSTXNsDataSeriesSentRecieved <- ACSSTXNsDataSeriesSentRecieved[, c("TotalValue") := sum(RecievedValue, SentValue), 
                                                                       with = FALSE, by=c("sender","stream","Year_Month")];
    
    ##Remove and rename duplicated fields
    ACSSTXNsDataSeriesSentRecieved[ ,c("senderID.y", "senderFullName.y") := NULL];
    setnames(ACSSTXNsDataSeriesSentRecieved,"senderID.x","senderID");
    setnames(ACSSTXNsDataSeriesSentRecieved,"senderFullName.x","senderFullName");
    
    
    
    rm(ACSSTXNsDataSeriesSent_df);
    rm(ACSSTXNsDataSeriesRecieved_df);
    rm(ACSSTXNsDataSeriesSentRecieved_df);
    
    
  }else if(dataFrequency == "quarterly"){
    
    ACSSTXNsDataSeriesBySender  <-  mergedACSSTXNsYearsSpecYM[, list(SentVolume=sum(TotalVolume),RecievedVolume=sum(TotalVolume),
                                                                     TotalVolume=sum(volumeRecieved, volumeSent),
                                                                     SentValue=sum(TotalValue), RecievedValue=sum(TotalValue),
                                                                     TotalValue=sum(valueRecieved, valueSent)
    ), 
    by=c("sender","senderID","senderFullName","stream","StreamDescription","StreamType","Year_Quarter")];
    
    ACSSTXNsDataSeriesBySender <-   ACSSTXNsDataSeriesBySender[order(Year_Quarter, sender), ];
    
    ACSSTXNsDataSeriesByPeriod  <-  mergedACSSTXNsYearsSpecYM[, list(SentVolume=sum(TotalVolume),RecievedVolume=sum(TotalVolume),
                                                                     TotalVolume=sum(volumeRecieved, volumeSent),
                                                                     SentValue=sum(TotalValue), RecievedValue=sum(TotalValue),
                                                                     TotalValue=sum(valueRecieved, valueSent)
    ),
    by=c("Year_Quarter","stream","StreamDescription","StreamType")];
    ACSSTXNsDataSeriesByPeriod <-   ACSSTXNsDataSeriesByPeriod[order(Year_Quarter), ];
    
    ACSSTXNsDataSeriesBySenderReceiver  <-  mergedACSSTXNsYearsSpecYM[, list(SentVolume=sum(TotalVolume),RecievedVolume=sum(TotalVolume),
                                                                             TotalVolume=sum(volumeRecieved, volumeSent),
                                                                             SentValue=sum(TotalValue), RecievedValue=sum(TotalValue),
                                                                             TotalValue=sum(valueRecieved, valueSent)
    ), 
    by=c("Year_Quarter","stream","StreamDescription","StreamType","sender","senderID","senderFullName","receiver","receiverID","receiverFullName")];
    
    ACSSTXNsDataSeriesBySenderReceiver <- ACSSTXNsDataSeriesBySenderReceiver[order(Year_Quarter, sender, receiver), ];
    
    
    ACSSTXNsDataSeriesSent  <-  mergedACSSTXNsYearsSpecYM[, list(SentVolume=sum(TotalVolume), SentValue=sum(TotalValue)
    ), 
    by=c("sender","senderID","senderFullName","stream","StreamDescription","StreamType","Year_Quarter")];
    
    ACSSTXNsDataSeriesRecieved  <-  mergedACSSTXNsYearsSpecYM[, list(RecievedVolume=sum(TotalVolume), RecievedValue=sum(TotalValue)
    ), 
    by=c("sender","senderID","senderFullName","stream","StreamDescription","StreamType","Year_Quarter")];
    
    
    ACSSTXNsDataSeriesSent_df <-  as.data.frame(ACSSTXNsDataSeriesSent);
    ACSSTXNsDataSeriesRecieved_df <-  as.data.frame(ACSSTXNsDataSeriesRecieved);
    ACSSTXNsDataSeriesSentRecieved_df  <-  merge(ACSSTXNsDataSeriesSent_df, ACSSTXNsDataSeriesRecieved_df, by.x=c("sender","stream","StreamDescription","StreamType","Year_Quarter"), 
                                                 by.y=c("sender","stream","StreamDescription","StreamType","Year_Quarter"),all.x = TRUE, all.y = TRUE);
    
    ACSSTXNsDataSeriesSentRecieved_df <- do.call("rbind", as.list(by(ACSSTXNsDataSeriesSentRecieved_df, 
                                                                     ACSSTXNsDataSeriesSentRecieved_df["Year_Quarter"], 
                                                                     transform, ShareSentVolume=fnShare(SentVolume))));
    ACSSTXNsDataSeriesSentRecieved_df <- do.call("rbind", as.list(by(ACSSTXNsDataSeriesSentRecieved_df, 
                                                                     ACSSTXNsDataSeriesSentRecieved_df["Year_Quarter"], 
                                                                     transform, ShareSentValue=fnShare(SentValue))));
    ACSSTXNsDataSeriesSentRecieved_df <- do.call("rbind", as.list(by(ACSSTXNsDataSeriesSentRecieved_df, 
                                                                     ACSSTXNsDataSeriesSentRecieved_df["Year_Quarter"], 
                                                                     transform, ShareRecievedVolume=fnShare(RecievedVolume))));
    ACSSTXNsDataSeriesSentRecieved_df <- do.call("rbind", as.list(by(ACSSTXNsDataSeriesSentRecieved_df, 
                                                                     ACSSTXNsDataSeriesSentRecieved_df["Year_Quarter"], 
                                                                     transform, ShareRecievedValue=fnShare(RecievedValue))));

    ACSSTXNsDataSeriesSentRecieved <-  as.data.table(ACSSTXNsDataSeriesSentRecieved_df);
    ACSSTXNsDataSeriesSentRecieved<- ACSSTXNsDataSeriesSentRecieved[, c("TotalVolume") := sum(RecievedVolume, SentVolume), 
                                                                    with = FALSE, by=c("sender","stream","StreamDescription","StreamType","Year_Quarter")];
    ACSSTXNsDataSeriesSentRecieved<- ACSSTXNsDataSeriesSentRecieved[, c("TotalValue") := sum(RecievedValue, SentValue), 
                                                                    with = FALSE, by=c("sender","stream","StreamDescription","StreamType","Year_Quarter")];
    ##Remove and rename duplicated fields
    ACSSTXNsDataSeriesSentRecieved[ ,c("senderID.y", "senderFullName.y") := NULL];
    setnames(ACSSTXNsDataSeriesSentRecieved,"senderID.x","senderID");
    setnames(ACSSTXNsDataSeriesSentRecieved,"senderFullName.x","senderFullName");

    
    rm(ACSSTXNsDataSeriesSent_df);
    rm(ACSSTXNsDataSeriesRecieved_df);
    rm(ACSSTXNsDataSeriesSentRecieved_df);
    
  }else if(dataFrequency == "annual"){
    
    ACSSTXNsDataSeriesBySender  <-  mergedACSSTXNsYearsSpecYM[, list(SentVolume=sum(volumeRecieved), RecievedVolume=sum(volumeSent),
                                                                       TotalVolume=sum(volumeRecieved, volumeSent),
                                                                       SentValue=sum(valueRecieved), RecievedValue=sum(valueSent),
                                                                       TotalValue=sum(valueRecieved, valueSent)
    ), 
    by=c("sender","senderID","senderFullName","stream","Year")];
    
    ACSSTXNsDataSeriesBySender <-  ACSSTXNsDataSeriesBySender[order(Year, sender), ];
    
    ACSSTXNsDataSeriesByPeriod  <-  mergedACSSTXNsYearsSpecYM[, list(SentVolume=sum(volumeRecieved), RecievedVolume=sum(volumeSent),
                                                                       TotalVolume=sum(volumeRecieved, volumeSent),
                                                                       SentValue=sum(valueRecieved), RecievedValue=sum(valueSent),
                                                                       TotalValue=sum(valueRecieved, valueSent)
    ), 
    by=c("Year","stream")];
    ACSSTXNsDataSeriesByPeriod <-  ACSSTXNsDataSeriesByPeriod[order(Year), ];
    
    ACSSTXNsDataSeriesBySenderReceiver  <-  mergedACSSTXNsYearsSpecYM[, list(SentVolume=sum(volumeRecieved), RecievedVolume=sum(volumeSent),
                                                                               TotalVolume=sum(volumeRecieved, volumeSent),
                                                                               SentValue=sum(valueRecieved), RecievedValue=sum(valueSent),
                                                                               TotalValue=sum(valueRecieved, valueSent)
    ), 
    by=c("Year","stream","sender","senderID","senderFullName","receiver","receiverID","receiverFullName")];
    
    ACSSTXNsDataSeriesBySenderReceiver <-  ACSSTXNsDataSeriesBySenderReceiver[order(Year, sender, receiver), ];
    
    ##Note that the by FI sent and recieved data has to be collated by way of the sender because of the way the ACSS extract files are generated
    ##i.e. each line record in the extract is from the persepcetive of the sender at a point in time. Hence in each line sender X sends and recieves 
    ##some volume and value of payments to and from reciever Y
    ACSSTXNsDataSeriesSent  <-  mergedACSSTXNsYearsSpecYM[, list(SentVolume=sum(volumeRecieved), SentValue=sum(valueRecieved)
    ), 
    by=c("sender","senderID","senderFullName","stream","Year")];
    
    ACSSTXNsDataSeriesRecieved  <-  mergedACSSTXNsYearsSpecYM[, list(RecievedVolume=sum(volumeSent), RecievedValue=sum(valueSent)
    ), 
    by=c("sender","senderID","senderFullName","stream","Year")];
    
    
    
    ACSSTXNsDataSeriesSent_df <-  as.data.frame(ACSSTXNsDataSeriesSent);
    ACSSTXNsDataSeriesRecieved_df <-  as.data.frame(ACSSTXNsDataSeriesRecieved);
    ACSSTXNsDataSeriesSentRecieved_df <-  merge(ACSSTXNsDataSeriesSent_df, ACSSTXNsDataSeriesRecieved_df, by.x="sender", by.y="sender");
    ACSSTXNsDataSeriesSentRecieved_df  <-  merge(ACSSTXNsDataSeriesSent_df, ACSSTXNsDataSeriesRecieved_df, by.x=c("sender","stream","Year"), 
                                                  by.y=c("sender","stream","Year"), all.x = TRUE, all.y = TRUE);
    
    
    
    ACSSTXNsDataSeriesSentRecieved_df <- do.call("rbind", as.list(by(ACSSTXNsDataSeriesSentRecieved_df, 
                                                                      ACSSTXNsDataSeriesSentRecieved_df["Year"], 
                                                                      transform, ShareSentVolume=fnShare(SentVolume))));
    ACSSTXNsDataSeriesSentRecieved_df <- do.call("rbind", as.list(by(ACSSTXNsDataSeriesSentRecieved_df, 
                                                                      ACSSTXNsDataSeriesSentRecieved_df["Year"], 
                                                                      transform, ShareSentValue=fnShare(SentValue))));
    ACSSTXNsDataSeriesSentRecieved_df <- do.call("rbind", as.list(by(ACSSTXNsDataSeriesSentRecieved_df, 
                                                                      ACSSTXNsDataSeriesSentRecieved_df["Year"], 
                                                                      transform, ShareRecievedVolume=fnShare(RecievedVolume))));
    ACSSTXNsDataSeriesSentRecieved_df <- do.call("rbind", as.list(by(ACSSTXNsDataSeriesSentRecieved_df, 
                                                                      ACSSTXNsDataSeriesSentRecieved_df["Year"], 
                                                                      transform, ShareRecievedValue=fnShare(RecievedValue))));
    
    
    ACSSTXNsDataSeriesSentRecieved <-  as.data.table(ACSSTXNsDataSeriesSentRecieved_df);
    
    ACSSTXNsDataSeriesSentRecieved <- ACSSTXNsDataSeriesSentRecieved[, c("TotalVolume") := sum(RecievedVolume, SentVolume), 
                                                                       with = FALSE, by=c("sender","stream","Year")];
    ACSSTXNsDataSeriesSentRecieved <- ACSSTXNsDataSeriesSentRecieved[, c("TotalValue") := sum(RecievedValue, SentValue), 
                                                                       with = FALSE, by=c("sender","stream","Year")];
    
    ##Remove and rename duplicated fields
    ACSSTXNsDataSeriesSentRecieved[ ,c("senderID.y", "senderFullName.y") := NULL];
    setnames(ACSSTXNsDataSeriesSentRecieved,"senderID.x","senderID");
    setnames(ACSSTXNsDataSeriesSentRecieved,"senderFullName.x","senderFullName");
    
    rm(ACSSTXNsDataSeriesSent_df);
    rm(ACSSTXNsDataSeriesRecieved_df);
    rm(ACSSTXNsDataSeriesSentRecieved_df);
    
  }else if(dataFrequency == "daily"){
    ACSSTXNsDataSeriesBySender  <-  mergedACSSTXNsYearsSpecYM[, list(SentVolume=sum(TotalVolume),RecievedVolume=sum(TotalVolume),
                                                                       TotalVolume=sum(volumeRecieved, volumeSent),
                                                                       SentValue=sum(TotalValue), RecievedValue=sum(TotalValue),
                                                                       TotalValue=sum(valueRecieved, valueSent)
    ), 
    by=c("sender","senderID","senderFullName","stream","StreamDescription","StreamType","date")];
    
    ACSSTXNsDataSeriesBySender <-   ACSSTXNsDataSeriesBySender[order(date, sender), ];
    
    ACSSTXNsDataSeriesByPeriod  <-  mergedACSSTXNsYearsSpecYM[, list(SentVolume=sum(TotalVolume),RecievedVolume=sum(TotalVolume),
                                                                       TotalVolume=sum(volumeRecieved, volumeSent),
                                                                       SentValue=sum(TotalValue), RecievedValue=sum(TotalValue),
                                                                       TotalValue=sum(valueRecieved, valueSent)
    ),
    by=c("date","stream","StreamDescription","StreamType")];
    ACSSTXNsDataSeriesByPeriod <-   ACSSTXNsDataSeriesByPeriod[order(date), ];
    
    ACSSTXNsDataSeriesBySenderReceiver  <-  mergedACSSTXNsYearsSpecYM[, list(SentVolume=sum(TotalVolume),RecievedVolume=sum(TotalVolume),
                                                                               TotalVolume=sum(volumeRecieved, volumeSent),
                                                                               SentValue=sum(TotalValue), RecievedValue=sum(TotalValue),
                                                                               TotalValue=sum(valueRecieved, valueSent)
    ), 
    by=c("date","stream","StreamDescription","StreamType","sender","senderID","senderFullName","receiver","receiverID","receiverFullName")];
    
    ACSSTXNsDataSeriesBySenderReceiver <- ACSSTXNsDataSeriesBySenderReceiver[order(date, sender, receiver), ];
    
    
    ACSSTXNsDataSeriesSent  <-  mergedACSSTXNsYearsSpecYM[, list(SentVolume=sum(TotalVolume), SentValue=sum(TotalValue)
    ), 
    by=c("sender","senderID","senderFullName","stream","StreamDescription","StreamType","date")];
    
    ACSSTXNsDataSeriesRecieved  <-  mergedACSSTXNsYearsSpecYM[, list(RecievedVolume=sum(TotalVolume), RecievedValue=sum(TotalValue)
    ), 
    by=c("sender","senderID","senderFullName","stream","StreamDescription","StreamType","date")];
    
    
    ACSSTXNsDataSeriesSent_df <-  as.data.frame(ACSSTXNsDataSeriesSent);
    ACSSTXNsDataSeriesRecieved_df <-  as.data.frame(ACSSTXNsDataSeriesRecieved);
    ACSSTXNsDataSeriesSentRecieved_df  <-  merge(ACSSTXNsDataSeriesSent_df, ACSSTXNsDataSeriesRecieved_df, by.x=c("sender","stream","StreamDescription","StreamType","date"), 
                                                  by.y=c("sender","stream","StreamDescription","StreamType","date"),all.x = TRUE, all.y = TRUE);
    
    ACSSTXNsDataSeriesSentRecieved_df <- do.call("rbind", as.list(by(ACSSTXNsDataSeriesSentRecieved_df, 
                                                                      ACSSTXNsDataSeriesSentRecieved_df["date"], 
                                                                      transform, ShareSentVolume=fnShare(SentVolume))));
    ACSSTXNsDataSeriesSentRecieved_df <- do.call("rbind", as.list(by(ACSSTXNsDataSeriesSentRecieved_df, 
                                                                      ACSSTXNsDataSeriesSentRecieved_df["date"], 
                                                                      transform, ShareSentValue=fnShare(SentValue))));
    ACSSTXNsDataSeriesSentRecieved_df <- do.call("rbind", as.list(by(ACSSTXNsDataSeriesSentRecieved_df, 
                                                                      ACSSTXNsDataSeriesSentRecieved_df["date"], 
                                                                      transform, ShareRecievedVolume=fnShare(RecievedVolume))));
    ACSSTXNsDataSeriesSentRecieved_df <- do.call("rbind", as.list(by(ACSSTXNsDataSeriesSentRecieved_df, 
                                                                      ACSSTXNsDataSeriesSentRecieved_df["date"], 
                                                                      transform, ShareRecievedValue=fnShare(RecievedValue))));
    
    
    ACSSTXNsDataSeriesSentRecieved <-  as.data.table(ACSSTXNsDataSeriesSentRecieved_df);
    ACSSTXNsDataSeriesSentRecieved<- ACSSTXNsDataSeriesSentRecieved[, c("TotalVolume") := sum(RecievedVolume, SentVolume), 
                                                                      with = FALSE, by=c("sender","stream","StreamDescription","StreamType","date")];
    ACSSTXNsDataSeriesSentRecieved<- ACSSTXNsDataSeriesSentRecieved[, c("TotalValue") := sum(RecievedValue, SentValue), 
                                                                      with = FALSE, by=c("sender","stream","StreamDescription","StreamType","date")];
    ##Remove and rename duplicated fields
    ACSSTXNsDataSeriesSentRecieved[ ,c("senderID.y", "senderFullName.y") := NULL];
    setnames(ACSSTXNsDataSeriesSentRecieved,"senderID.x","senderID");
    setnames(ACSSTXNsDataSeriesSentRecieved,"senderFullName.x","senderFullName");
    
    
    #Remove temporary working datasets from memory
    rm(ACSSTXNsDataSeriesSent_df);
    rm(ACSSTXNsDataSeriesRecieved_df);
    rm(ACSSTXNsDataSeriesSentRecieved_df);
    
  }else if(dataFrequency == "dateTime"){
    ##Unlike LVTS, ACSS and USBE data are netted and thus do not have the time dimension to them.
    ##This place holder has been left to mark where the code will go if at a later date this dimenssion is included
  }
  
}##end of the stream level collation
 
gc();

ACSSTXNsDataSeries <- ACSSTXNsDataSeriesBySenderReceiver;

save(ACSSTXNsDataSeries,file=ACSSTXNsDataSeriesSavePath);
save(ACSSTXNsDataSeriesSentRecieved, file=ACSSTXNsDataSeriesSentRecievedSavePath);
if(byStream){
  ##save(ACSSTXNsDataSeriesByPeriod,file=ACSSTXNsByPeriodDataSeriesSavePath);  ## ACSSTXNsDataSeriesByPeriodSavePath
  save(ACSSTXNsDataSeriesByPeriod,file=ACSSTXNsDataSeriesByPeriodSavePath); 
}

##write data into STATA readable files
write.dta(ACSSTXNsDataSeriesByPeriod,file=ACSSTXNsDataSeriesSTATASavePath);
write.dta(ACSSTXNsDataSeriesBySender,file=ACSSTXNsDataSeriesBySenderSTATASavePath);
write.csv(ACSSTXNsDataSeriesBySender,file=ACSSTXNsDataSeriesBySenderXLSSavePath);
#write.dta(ACSSTXNsDataSeriesByPeriod, file=ACSSTXNsDataSeriesByPeriodSTATASavePath);
write.dta(ACSSTXNsDataSeriesSentRecieved, file=ACSSTXNsDataSeriesSentRecievedSTATASavePath);
write.csv(ACSSTXNsDataSeriesSentRecieved, file=ACSSTXNsDataSeriesSentRecievedXLSSavePath);
write.csv(ACSSTXNsDataSeriesByPeriod, file=ACSSTXNsDataSeriesByPeriodXLSSavePath);
gc();
##return to the original directory
#setwd(cur.dir);