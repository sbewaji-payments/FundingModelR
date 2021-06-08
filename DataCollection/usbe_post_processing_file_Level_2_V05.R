##This script acts to merge the USBE data with the FI static data and assign the appropriate BIC Codes based on memberIDs.
##This is required because the USBE data only records memberIDs and not their BIC code.
##Consequently, in order to maintain consistency with the USBE dataset BIC codes must be assigned to the USBE data
## 
##The Script will import the saved data.table extract from the "acss_processing_file_V##_Start-to-End_Year_Selective_Extraction.R" code
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
## Modifications Made: 
##        1) inclusion of the TotalValue and TotalVolume 
##          Code
##          (USBETXNsDataSeriesSentRecieved<- USBETXNsDataSeriesSentRecieved[, c("Total_Volume") := sum(RecievedVolume, SentVolume), 
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
load(extractedSummaryUSBEDataLoadPath);
load(paymentOpsDataFile);
gc();
print("data tables loaded");#used to record speed of computations
print(now());#used to record speed of computations
gc();
##Convert factors in the 2000-2010 payment ops input file to character and set as a data.datable
#USBETXNsDataSeriesSentRecieved2000To2010  <-  USBETXNsDataSeriesSentRecieved2000To2010 %>% mutate_if(is.factor, as.character);
#USBETXNsDataSeriesSentRecieved2000To2010 <- as.data.table(USBETXNsDataSeriesSentRecieved2000To2010);
#USBETXNsDataSeriesSentRecieved2000To2010$TotalVolume <- as.numeric(USBETXNsDataSeriesSentRecieved2000To2010$TotalVolume);

mergedUSBETXNsYearsSpecYM_df  <-  as.data.frame(mergedUSBETXNsYearsSpecYM);#convert mergedUSBETXNsYearsSpecYM to a temporary data.frame object
fiStaticDataFile_df  <-  as.data.frame(fiStaticDataFile);#convert fiStaticDataFile to a temporary data.frame object
USBEStreamDataFile_df  <- as.data.frame(RETAILStreamData);
rm(mergedUSBETXNsYearsSpecYM);#remove mergedUSBETXNsYearsSpecYM to free up memory since not in use
rm(fiStaticDataFile);#remove fiStaticDataFile to free up memory since not in use
rm(RETAILStreamData);#remove USBEStreamData to free up memory since not in use


##merge mergedUSBETXNsYearsSpecYM_df and fiStaticDataFile_df data.frame objects into the final mergedUSBETXNsYearsSpecYM data table object
##starting by merging using the recieving member ID numbers followed by the sending member ID numbers.
##after each merge, the memberBIC and memberName columns will be renamed to 
##"receiver"("sender") and "receiverFullName"("senderFullName") as appropriate
##starting with the "receiver"
mergedUSBETXNsYearsSpecYM  <-  merge(mergedUSBETXNsYearsSpecYM_df, fiStaticDataFile_df, by.x="receiverID", by.y="memberID");
colnames(mergedUSBETXNsYearsSpecYM)[colnames(mergedUSBETXNsYearsSpecYM)=="memberBIC"] <- "receiver";
colnames(mergedUSBETXNsYearsSpecYM)[colnames(mergedUSBETXNsYearsSpecYM)=="memberName"] <- "receiverFullName";

rm(mergedUSBETXNsYearsSpecYM_df);#remove mergedUSBETXNsYearsSpecYM_df to free up memory since not in use
gc(); #free up memory

##next the "sender"
##Note that for the sender details the code uses the main working data.frame object mergedUSBETXNsYearsSpecYM and not
##the temporary one mergedUSBETXNsYearsSpecYM_df
mergedUSBETXNsYearsSpecYM  <-  merge(mergedUSBETXNsYearsSpecYM, fiStaticDataFile_df, by.x="senderID", by.y="memberID");
colnames(mergedUSBETXNsYearsSpecYM)[colnames(mergedUSBETXNsYearsSpecYM)=="memberBIC"] <- "sender";
colnames(mergedUSBETXNsYearsSpecYM)[colnames(mergedUSBETXNsYearsSpecYM)=="memberName"] <- "senderFullName";

rm(fiStaticDataFile_df);#remove fiStaticDataFile_df to free up memory since not in use
gc(); #free up memory

##Now merge the stream description data with the transaction data
mergedUSBETXNsYearsSpecYM  <-  merge(mergedUSBETXNsYearsSpecYM, USBEStreamDataFile_df, by.x="stream", by.y="streamID", all.x = TRUE, all.y = TRUE);
gc(); #free up memory

mergedUSBETXNsYearsSpecYM  <-  as.data.table(mergedUSBETXNsYearsSpecYM);#convert mergedUSBETXNsYearsSpecYM to a data.table object
##drop any cases where the rows contain NAs
##commented out because the NAs seem to be only related to the stream discription and stream type columns
##Will need to update this in the stream data file
##This will break if the RetailStreamData file is not propoerly updated to reflect all possible streams in the retail system
mergedUSBETXNsYearsSpecYM  <-  mergedUSBETXNsYearsSpecYM[complete.cases(mergedUSBETXNsYearsSpecYM),];
##remove extra columns added
#mergedUSBETXNsYearsSpecYM  <-  mergedUSBETXNsYearsSpecYM[ ,c("Year_Month","Year_Quarter","Year") := NULL];
gc(); #free up memory


#ensure the numeric fields are indeed numeric and not strings
mergedUSBETXNsYearsSpecYM$valueRecieved <- as.double(mergedUSBETXNsYearsSpecYM$valueRecieved);
mergedUSBETXNsYearsSpecYM$volumeRecieved <- as.double(mergedUSBETXNsYearsSpecYM$volumeRecieved);
mergedUSBETXNsYearsSpecYM$valueSent <- as.double(mergedUSBETXNsYearsSpecYM$valueSent);
mergedUSBETXNsYearsSpecYM$volumeSent <- as.double(mergedUSBETXNsYearsSpecYM$volumeSent);
gc(); #free up memory


## Now create tables for each data frequency
## NOTE: 
## With the USBE data, "sent" and "recieved" refer to messages "sent" for payment and messaged "recieved" for payment.
## So the column names must be switched around to match the actual flow of funds/payments.
## Hence "SentVolume=sum(volumeRecieved)" "RecievedVolume=sum(volumeSent)" etc
if(byStream == FALSE){
  
  if(dataFrequency == "monthly"){
    
    USBETXNsDataSeriesBySender <-  mergedUSBETXNsYearsSpecYM[, list(SentVolume=sum(volumeRecieved), RecievedVolume=sum(volumeSent),
                                                                      TotalVolume=sum(volumeRecieved, volumeSent),
                                                                      SentValue=sum(valueRecieved), RecievedValue=sum(valueSent),
                                                                      TotalValue=sum(valueRecieved, valueSent)
    ), 
    by=c("sender","senderID","senderFullName","Year_Month")];
    USBETXNsDataSeriesBySender  <-  USBETXNsDataSeriesBySender[order(Year_Month, sender), ];
    
    
    USBETXNsDataSeriesByPeriod <-  mergedUSBETXNsYearsSpecYM[, list(SentVolume=sum(volumeRecieved), RecievedVolume=sum(volumeSent),
                                                                      TotalVolume=sum(volumeRecieved, volumeSent),
                                                                      SentValue=sum(valueRecieved), RecievedValue=sum(valueSent),
                                                                      TotalValue=sum(valueRecieved, valueSent)
    ), 
    by=c("Year_Month")];
    
    USBETXNsDataSeriesByPeriod  <-  USBETXNsDataSeriesByPeriod[order(Year_Month), ];
    
    USBETXNsDataSeriesBySenderReceiver <-  mergedUSBETXNsYearsSpecYM[, list(SentVolume=sum(volumeRecieved), RecievedVolume=sum(volumeSent),
                                                                              TotalVolume=sum(volumeRecieved, volumeSent),
                                                                              SentValue=sum(valueRecieved), RecievedValue=sum(valueSent),
                                                                              TotalValue=sum(valueRecieved, valueSent)
    ), 
    by=c("Year_Month","sender","senderID","senderFullName","receiver","receiverID","receiverFullName")];
    
    USBETXNsDataSeriesBySenderReceiver  <-  USBETXNsDataSeriesBySenderReceiver[order(Year_Month, sender, receiver), ];
    
    ##Note that the by FI sent and recieved data has to be collated by way of the sender because of the way the USBE extract files are generated
    ##i.e. each line record in the extract is from the persepcetive of the sender at a point in time. Hence in each line sender X sends and recieves 
    ##some volume and value of payments to and from reciever Y
    USBETXNsDataSeriesSent  <-  mergedUSBETXNsYearsSpecYM[, list(SentVolume=sum(volumeRecieved), SentValue=sum(valueRecieved)
    ), 
    by=c("sender","senderID","senderFullName","Year_Month")];
    
    USBETXNsDataSeriesRecieved  <-  mergedUSBETXNsYearsSpecYM[, list(RecievedVolume=sum(volumeSent), RecievedValue=sum(valueSent)
    ), 
    by=c("sender","senderID","senderFullName","Year_Month")];
    
    
    USBETXNsDataSeriesSent_df <-  as.data.frame(USBETXNsDataSeriesSent);
    USBETXNsDataSeriesRecieved_df <-  as.data.frame(USBETXNsDataSeriesRecieved);
    USBETXNsDataSeriesSentRecieved_df  <-  merge(USBETXNsDataSeriesSent_df, USBETXNsDataSeriesRecieved_df, by.x=c("sender","Year_Month"), 
                                                  by.y=c("sender","Year_Month"), all.x = TRUE, all.y = TRUE);
    
    print("computing Shares for 2000-2010");
    USBETXNsDataSeriesSentRecieved2000To2010_df <- as.data.frame(USBETXNsDataSeriesSentRecieved2000To2010);
    
    USBETXNsDataSeriesSentRecieved2000To2010_df <- do.call("rbind", as.list(by(USBETXNsDataSeriesSentRecieved2000To2010_df, 
                                                                               USBETXNsDataSeriesSentRecieved2000To2010_df["Year_Month"], 
                                                                     transform, ShareSentVolume=fnShare(SentVolume))));
    USBETXNsDataSeriesSentRecieved2000To2010_df <- do.call("rbind", as.list(by(USBETXNsDataSeriesSentRecieved2000To2010_df, 
                                                                               USBETXNsDataSeriesSentRecieved2000To2010_df["Year_Month"], 
                                                                     transform, ShareSentValue=fnShare(SentValue))));
    USBETXNsDataSeriesSentRecieved2000To2010_df <- do.call("rbind", as.list(by(USBETXNsDataSeriesSentRecieved2000To2010_df, 
                                                                     USBETXNsDataSeriesSentRecieved2000To2010_df["Year_Month"], 
                                                                     transform, ShareRecievedVolume=fnShare(RecievedVolume))));
    USBETXNsDataSeriesSentRecieved2000To2010_df <- do.call("rbind", as.list(by(USBETXNsDataSeriesSentRecieved2000To2010_df, 
                                                                               USBETXNsDataSeriesSentRecieved2000To2010_df["Year_Month"], 
                                                                     transform, ShareRecievedValue=fnShare(RecievedValue))));
    
    USBETXNsDataSeriesSentRecieved2000To2010 <-  as.data.table(USBETXNsDataSeriesSentRecieved2000To2010_df);
    
    
    USBETXNsDataSeriesSentRecieved_df <- do.call("rbind", as.list(by(USBETXNsDataSeriesSentRecieved_df, 
                                                                      USBETXNsDataSeriesSentRecieved_df["Year_Month"], 
                                                                      transform, ShareSentVolume=fnShare(SentVolume))));
    USBETXNsDataSeriesSentRecieved_df <- do.call("rbind", as.list(by(USBETXNsDataSeriesSentRecieved_df, 
                                                                      USBETXNsDataSeriesSentRecieved_df["Year_Month"], 
                                                                      transform, ShareSentValue=fnShare(SentValue))));
    USBETXNsDataSeriesSentRecieved_df <- do.call("rbind", as.list(by(USBETXNsDataSeriesSentRecieved_df, 
                                                                      USBETXNsDataSeriesSentRecieved_df["Year_Month"], 
                                                                      transform, ShareRecievedVolume=fnShare(RecievedVolume))));
    USBETXNsDataSeriesSentRecieved_df <- do.call("rbind", as.list(by(USBETXNsDataSeriesSentRecieved_df, 
                                                                      USBETXNsDataSeriesSentRecieved_df["Year_Month"], 
                                                                      transform, ShareRecievedValue=fnShare(RecievedValue))));
    
    print("Finished computing Shares for 2000-2010");
    
    USBETXNsDataSeriesSentRecieved <-  as.data.table(USBETXNsDataSeriesSentRecieved_df);
    USBETXNsDataSeriesSentRecieved <- USBETXNsDataSeriesSentRecieved[, c("TotalVolume") := sum(RecievedVolume, SentVolume), 
                                                                       with = FALSE, by=c("sender","Year_Month")];
    USBETXNsDataSeriesSentRecieved <- USBETXNsDataSeriesSentRecieved[, c("TotalValue") := sum(RecievedValue, SentValue), 
                                                                       with = FALSE, by=c("sender","Year_Month")];
    
    ##Remove and rename duplicated fields
    USBETXNsDataSeriesSentRecieved[ ,c("senderID.y", "senderFullName.y") := NULL];
    setnames(USBETXNsDataSeriesSentRecieved,"senderID.x","senderID");
    setnames(USBETXNsDataSeriesSentRecieved,"senderFullName.x","senderFullName");
    
    print("merging Shares for 2000-2010");
    
    ##merge the sent recieved data with the payment ops 2000 to 2010 data
    USBETXNsDataSeriesSentRecieved <-  rbindlist(list(USBETXNsDataSeriesSentRecieved2000To2010, USBETXNsDataSeriesSentRecieved), 
                                                 use.names=TRUE, fill=TRUE, idcol=FALSE);
    
    print("Finished merging Shares for 2000-2010");
    
    
    rm(USBETXNsDataSeriesSent_df);
    rm(USBETXNsDataSeriesRecieved_df);
    rm(USBETXNsDataSeriesSentRecieved_df);
    
    
  }else if(dataFrequency == "quarterly"){
    
    USBETXNsDataSeriesBySender <-  mergedUSBETXNsYearsSpecYM[, list(SentVolume=sum(volumeRecieved), RecievedVolume=sum(volumeSent),
                                                                      TotalVolume=sum(volumeRecieved, volumeSent),
                                                                      SentValue=sum(valueRecieved), RecievedValue=sum(valueSent),
                                                                      TotalValue=sum(valueRecieved, valueSent)
    ), 
    by=c("sender","senderFullName","Year_Quarter")];
    
    USBETXNsDataSeriesBySender <-  USBETXNsDataSeriesBySender[order(Year_Quarter, sender), ];
    
    USBETXNsDataSeriesByPeriod <-  mergedUSBETXNsYearsSpecYM[, list(SentVolume=sum(volumeRecieved), RecievedVolume=sum(volumeSent),
                                                                      TotalVolume=sum(volumeRecieved, volumeSent),
                                                                      SentValue=sum(valueRecieved), RecievedValue=sum(valueSent),
                                                                      TotalValue=sum(valueRecieved, valueSent)
    ), 
    by=c("Year_Quarter")];
    USBETXNsDataSeriesByPeriod <-  USBETXNsDataSeriesByPeriod[order(Year_Quarter), ];
    
    USBETXNsDataSeriesBySenderReceiver <-  mergedUSBETXNsYearsSpecYM[, list(SentVolume=sum(volumeRecieved), RecievedVolume=sum(volumeSent),
                                                                              TotalVolume=sum(volumeRecieved, volumeSent),
                                                                              SentValue=sum(valueRecieved), RecievedValue=sum(valueSent),
                                                                              TotalValue=sum(valueRecieved, valueSent)
    ), 
    by=c("Year_Quarter","sender","senderID","receiver","receiverID")];
    
    USBETXNsDataSeriesBySenderReceiver <-  USBETXNsDataSeriesBySenderReceiver[order(Year_Quarter, sender, receiver), ];
    
    ##Note that the by FI sent and recieved data has to be collated by way of the sender because of the way the USBE extract files are generated
    ##i.e. each line record in the extract is from the persepcetive of the sender at a point in time. Hence in each line sender X sends and recieves 
    ##some volume and value of payments to and from reciever Y
    USBETXNsDataSeriesSent  <-  mergedUSBETXNsYearsSpecYM[, list(SentVolume=sum(volumeRecieved), SentValue=sum(valueRecieved)
    ), 
    by=c("sender","senderID","senderFullName","Year_Quarter")];
    
    USBETXNsDataSeriesRecieved  <-  mergedUSBETXNsYearsSpecYM[, list(RecievedVolume=sum(volumeSent), RecievedValue=sum(valueSent)
    ), 
    by=c("sender","senderID","senderFullName","Year_Quarter")];
    
    
    
    
    USBETXNsDataSeriesSent_df <-  as.data.frame(USBETXNsDataSeriesSent);
    USBETXNsDataSeriesRecieved_df <-  as.data.frame(USBETXNsDataSeriesRecieved);
    USBETXNsDataSeriesSentRecieved_df <-  merge(USBETXNsDataSeriesSent_df, USBETXNsDataSeriesRecieved_df, by.x="sender", by.y="sender");
    USBETXNsDataSeriesSentRecieved_df  <-  merge(USBETXNsDataSeriesSent_df, USBETXNsDataSeriesRecieved_df, by.x=c("sender","Year_Quarter"), 
                                                  by.y=c("sender","Year_Quarter"), all.x = TRUE, all.y = TRUE);
    
    USBETXNsDataSeriesSentRecieved2000To2010_df <- as.data.frame(USBETXNsDataSeriesSentRecieved2000To2010);
    
    USBETXNsDataSeriesSentRecieved2000To2010_df <- do.call("rbind", as.list(by(USBETXNsDataSeriesSentRecieved2000To2010_df, 
                                                                               USBETXNsDataSeriesSentRecieved2000To2010_df["Year_Quarter"], 
                                                                               transform, ShareSentVolume=fnShare(SentVolume))));
    USBETXNsDataSeriesSentRecieved2000To2010_df <- do.call("rbind", as.list(by(USBETXNsDataSeriesSentRecieved2000To2010_df, 
                                                                               USBETXNsDataSeriesSentRecieved2000To2010_df["Year_Quarter"], 
                                                                               transform, ShareSentValue=fnShare(SentValue))));
    USBETXNsDataSeriesSentRecieved2000To2010_df <- do.call("rbind", as.list(by(USBETXNsDataSeriesSentRecieved2000To2010_df, 
                                                                               USBETXNsDataSeriesSentRecieved2000To2010_df["Year_Quarter"], 
                                                                               transform, ShareRecievedVolume=fnShare(RecievedVolume))));
    USBETXNsDataSeriesSentRecieved2000To2010_df <- do.call("rbind", as.list(by(USBETXNsDataSeriesSentRecieved2000To2010_df, 
                                                                               USBETXNsDataSeriesSentRecieved2000To2010_df["Year_Quarter"], 
                                                                               transform, ShareRecievedValue=fnShare(RecievedValue))));
    
    USBETXNsDataSeriesSentRecieved2000To2010 <-  as.data.table(USBETXNsDataSeriesSentRecieved2000To2010_df);
    
    
    USBETXNsDataSeriesSentRecieved_df <- do.call("rbind", as.list(by(USBETXNsDataSeriesSentRecieved_df, 
                                                                      USBETXNsDataSeriesSentRecieved_df["Year_Quarter"], 
                                                                      transform, ShareSentVolume=fnShare(SentVolume))));
    USBETXNsDataSeriesSentRecieved_df <- do.call("rbind", as.list(by(USBETXNsDataSeriesSentRecieved_df, 
                                                                      USBETXNsDataSeriesSentRecieved_df["Year_Quarter"], 
                                                                      transform, ShareSentValue=fnShare(SentValue))));
    USBETXNsDataSeriesSentRecieved_df <- do.call("rbind", as.list(by(USBETXNsDataSeriesSentRecieved_df, 
                                                                      USBETXNsDataSeriesSentRecieved_df["Year_Quarter"], 
                                                                      transform, ShareRecievedVolume=fnShare(RecievedVolume))));
    USBETXNsDataSeriesSentRecieved_df <- do.call("rbind", as.list(by(USBETXNsDataSeriesSentRecieved_df, 
                                                                      USBETXNsDataSeriesSentRecieved_df["Year_Quarter"], 
                                                                      transform, ShareRecievedValue=fnShare(RecievedValue))));
    
    
    USBETXNsDataSeriesSentRecieved <-  as.data.table(USBETXNsDataSeriesSentRecieved_df);
    USBETXNsDataSeriesSentRecieved <- USBETXNsDataSeriesSentRecieved[, c("TotalVolume") := sum(RecievedVolume, SentVolume), 
                                                                       with = FALSE, by=c("sender","Year_Quarter")];
    USBETXNsDataSeriesSentRecieved <- USBETXNsDataSeriesSentRecieved[, c("TotalValue") := sum(RecievedValue, SentValue), 
                                                                       with = FALSE, by=c("sender","Year_Quarter")];
    
    ##merge the sent recieved data with the payment ops 2000 to 2010 data
    USBETXNsDataSeriesSentRecieved <-  rbindlist(list(USBETXNsDataSeriesSentRecieved2000To2010, USBETXNsDataSeriesSentRecieved), 
                                                 use.names=TRUE, fill=TRUE, idcol=FALSE);
    
    
    ##Remove and rename duplicated fields
    USBETXNsDataSeriesSentRecieved[ ,c("senderID.y", "senderFullName.y") := NULL];
    setnames(USBETXNsDataSeriesSentRecieved,"senderID.x","senderID");
    setnames(USBETXNsDataSeriesSentRecieved,"senderFullName.x","senderFullName");
    
    rm(USBETXNsDataSeriesSent_df);
    rm(USBETXNsDataSeriesRecieved_df);
    rm(USBETXNsDataSeriesSentRecieved_df);
    
  }else if(dataFrequency == "annual"){
    
    USBETXNsDataSeriesBySender  <-  mergedUSBETXNsYearsSpecYM[, list(SentVolume=sum(volumeRecieved), RecievedVolume=sum(volumeSent),
                                                                       TotalVolume=sum(volumeRecieved, volumeSent),
                                                                       SentValue=sum(valueRecieved), RecievedValue=sum(valueSent),
                                                                       TotalValue=sum(valueRecieved, valueSent)
    ), 
    by=c("sender","senderID","senderFullName","Year")];
    
    USBETXNsDataSeriesBySender <-  USBETXNsDataSeriesBySender[order(Year, sender), ];
    
    USBETXNsDataSeriesByPeriod  <-  mergedUSBETXNsYearsSpecYM[, list(SentVolume=sum(volumeRecieved), RecievedVolume=sum(volumeSent),
                                                                       TotalVolume=sum(volumeRecieved, volumeSent),
                                                                       SentValue=sum(valueRecieved), RecievedValue=sum(valueSent),
                                                                       TotalValue=sum(valueRecieved, valueSent)
    ), 
    by=c("Year")];
    USBETXNsDataSeriesByPeriod <-  USBETXNsDataSeriesByPeriod[order(Year), ];
    
    USBETXNsDataSeriesBySenderReceiver  <-  mergedUSBETXNsYearsSpecYM[, list(SentVolume=sum(volumeRecieved), RecievedVolume=sum(volumeSent),
                                                                               TotalVolume=sum(volumeRecieved, volumeSent),
                                                                               SentValue=sum(valueRecieved), RecievedValue=sum(valueSent),
                                                                               TotalValue=sum(valueRecieved, valueSent)
    ), 
    by=c("Year","sender","senderID","senderFullName","receiver","receiverID","receiverFullName")];
    
    USBETXNsDataSeriesBySenderReceiver <-  USBETXNsDataSeriesBySenderReceiver[order(Year, sender, receiver), ];
    
    ##Note that the by FI sent and recieved data has to be collated by way of the sender because of the way the USBE extract files are generated
    ##i.e. each line record in the extract is from the persepcetive of the sender at a point in time. Hence in each line sender X sends and recieves 
    ##some volume and value of payments to and from reciever Y
    USBETXNsDataSeriesSent  <-  mergedUSBETXNsYearsSpecYM[, list(SentVolume=sum(volumeRecieved), SentValue=sum(valueRecieved)
    ), 
    by=c("sender","senderID","senderFullName","Year")];
    
    USBETXNsDataSeriesRecieved  <-  mergedUSBETXNsYearsSpecYM[, list(RecievedVolume=sum(volumeSent), RecievedValue=sum(valueSent)
    ), 
    by=c("sender","senderID","senderFullName","Year")];
    
    
    
    USBETXNsDataSeriesSent_df <-  as.data.frame(USBETXNsDataSeriesSent);
    USBETXNsDataSeriesRecieved_df <-  as.data.frame(USBETXNsDataSeriesRecieved);
    USBETXNsDataSeriesSentRecieved_df <-  merge(USBETXNsDataSeriesSent_df, USBETXNsDataSeriesRecieved_df, by.x="sender", by.y="sender");
    USBETXNsDataSeriesSentRecieved_df  <-  merge(USBETXNsDataSeriesSent_df, USBETXNsDataSeriesRecieved_df, by.x=c("sender","Year"), 
                                                  by.y=c("sender","Year"), all.x = TRUE, all.y = TRUE);
    

    USBETXNsDataSeriesSentRecieved2000To2010_df <- as.data.frame(USBETXNsDataSeriesSentRecieved2000To2010);
    
    USBETXNsDataSeriesSentRecieved2000To2010_df <- do.call("rbind", as.list(by(USBETXNsDataSeriesSentRecieved2000To2010_df, 
                                                                               USBETXNsDataSeriesSentRecieved2000To2010_df["Year"], 
                                                                               transform, ShareSentVolume=fnShare(SentVolume))));
    USBETXNsDataSeriesSentRecieved2000To2010_df <- do.call("rbind", as.list(by(USBETXNsDataSeriesSentRecieved2000To2010_df, 
                                                                               USBETXNsDataSeriesSentRecieved2000To2010_df["Year"], 
                                                                               transform, ShareSentValue=fnShare(SentValue))));
    USBETXNsDataSeriesSentRecieved2000To2010_df <- do.call("rbind", as.list(by(USBETXNsDataSeriesSentRecieved2000To2010_df, 
                                                                               USBETXNsDataSeriesSentRecieved2000To2010_df["Year"], 
                                                                               transform, ShareRecievedVolume=fnShare(RecievedVolume))));
    USBETXNsDataSeriesSentRecieved2000To2010_df <- do.call("rbind", as.list(by(USBETXNsDataSeriesSentRecieved2000To2010_df, 
                                                                               USBETXNsDataSeriesSentRecieved2000To2010_df["Year"], 
                                                                               transform, ShareRecievedValue=fnShare(RecievedValue))));
    
    USBETXNsDataSeriesSentRecieved2000To2010 <-  as.data.table(USBETXNsDataSeriesSentRecieved2000To2010_df);
    
    
    USBETXNsDataSeriesSentRecieved_df <- do.call("rbind", as.list(by(USBETXNsDataSeriesSentRecieved_df, 
                                                                      USBETXNsDataSeriesSentRecieved_df["Year"], 
                                                                      transform, ShareSentVolume=fnShare(SentVolume))));
    USBETXNsDataSeriesSentRecieved_df <- do.call("rbind", as.list(by(USBETXNsDataSeriesSentRecieved_df, 
                                                                      USBETXNsDataSeriesSentRecieved_df["Year"], 
                                                                      transform, ShareSentValue=fnShare(SentValue))));
    USBETXNsDataSeriesSentRecieved_df <- do.call("rbind", as.list(by(USBETXNsDataSeriesSentRecieved_df, 
                                                                      USBETXNsDataSeriesSentRecieved_df["Year"], 
                                                                      transform, ShareRecievedVolume=fnShare(RecievedVolume))));
    USBETXNsDataSeriesSentRecieved_df <- do.call("rbind", as.list(by(USBETXNsDataSeriesSentRecieved_df, 
                                                                      USBETXNsDataSeriesSentRecieved_df["Year"], 
                                                                      transform, ShareRecievedValue=fnShare(RecievedValue))));
    
    
    USBETXNsDataSeriesSentRecieved <-  as.data.table(USBETXNsDataSeriesSentRecieved_df);
    
    USBETXNsDataSeriesSentRecieved <- USBETXNsDataSeriesSentRecieved[, c("TotalVolume") := sum(RecievedVolume, SentVolume), 
                                                                       with = FALSE, by=c("sender","Year")];
    USBETXNsDataSeriesSentRecieved <- USBETXNsDataSeriesSentRecieved[, c("TotalValue") := sum(RecievedValue, SentValue), 
                                                                       with = FALSE, by=c("sender","Year")];
    
    ##merge the sent recieved data with the payment ops 2000 to 2010 data
    USBETXNsDataSeriesSentRecieved <-  rbindlist(list(USBETXNsDataSeriesSentRecieved2000To2010, USBETXNsDataSeriesSentRecieved), 
                                                 use.names=TRUE, fill=TRUE, idcol=FALSE);
    
    ##Remove and rename duplicated fields
    USBETXNsDataSeriesSentRecieved[ ,c("senderID.y", "senderFullName.y") := NULL];
    setnames(USBETXNsDataSeriesSentRecieved,"senderID.x","senderID");
    setnames(USBETXNsDataSeriesSentRecieved,"senderFullName.x","senderFullName");
    
    
    rm(USBETXNsDataSeriesSent_df);
    rm(USBETXNsDataSeriesRecieved_df);
    rm(USBETXNsDataSeriesSentRecieved_df);
    
  }else if(dataFrequency == "daily"){
    USBETXNsDataSeriesBySender  <-  mergedUSBETXNsYearsSpecYM[, list(SentVolume=sum(TotalVolume),RecievedVolume=sum(TotalVolume),
                                                                       TotalVolume=sum(volumeRecieved, volumeSent),
                                                                       SentValue=sum(TotalValue), RecievedValue=sum(TotalValue),
                                                                       TotalValue=sum(valueRecieved, valueSent)
    ), 
    by=c("sender","senderID","senderFullName","date")];
    
    USBETXNsDataSeriesBySender <-   USBETXNsDataSeriesBySender[order(date, sender), ];
    
    USBETXNsDataSeriesByPeriod  <-  mergedUSBETXNsYearsSpecYM[, list(SentVolume=sum(TotalVolume),RecievedVolume=sum(TotalVolume),
                                                                       TotalVolume=sum(volumeRecieved, volumeSent),
                                                                       SentValue=sum(TotalValue), RecievedValue=sum(TotalValue),
                                                                       TotalValue=sum(valueRecieved, valueSent)
    ),
    by=c("date")];
    USBETXNsDataSeriesByPeriod <-   USBETXNsDataSeriesByPeriod[order(date), ];
    
    USBETXNsDataSeriesBySenderReceiver  <-  mergedUSBETXNsYearsSpecYM[, list(SentVolume=sum(TotalVolume),RecievedVolume=sum(TotalVolume),
                                                                               TotalVolume=sum(volumeRecieved, volumeSent),
                                                                               SentValue=sum(TotalValue), RecievedValue=sum(TotalValue),
                                                                               TotalValue=sum(valueRecieved, valueSent)
    ), 
    by=c("date","sender","senderID","senderFullName","receiver","receiverID","receiverFullName")];
    
    USBETXNsDataSeriesBySenderReceiver <- USBETXNsDataSeriesBySenderReceiver[order(date, sender, receiver), ];
    
    
    USBETXNsDataSeriesSent  <-  mergedUSBETXNsYearsSpecYM[, list(SentVolume=sum(TotalVolume), SentValue=sum(TotalValue)
    ), 
    by=c("sender","date")];
    
    USBETXNsDataSeriesRecieved  <-  mergedUSBETXNsYearsSpecYM[, list(RecievedVolume=sum(TotalVolume), RecievedValue=sum(TotalValue)
    ), 
    by=c("sender","date")];
    
    
    USBETXNsDataSeriesSent_df <-  as.data.frame(USBETXNsDataSeriesSent);
    USBETXNsDataSeriesRecieved_df <-  as.data.frame(USBETXNsDataSeriesRecieved);
    USBETXNsDataSeriesSentRecieved_df  <-  merge(USBETXNsDataSeriesSent_df, USBETXNsDataSeriesRecieved_df, by.x=c("sender","date"), 
                                                  by.y=c("sender","date"),all.x = TRUE, all.y = TRUE);
    
    USBETXNsDataSeriesSentRecieved_df <- do.call("rbind", as.list(by(USBETXNsDataSeriesSentRecieved_df, 
                                                                      USBETXNsDataSeriesSentRecieved_df["date"], 
                                                                      transform, ShareSentVolume=fnShare(SentVolume))));
    USBETXNsDataSeriesSentRecieved_df <- do.call("rbind", as.list(by(USBETXNsDataSeriesSentRecieved_df, 
                                                                      USBETXNsDataSeriesSentRecieved_df["date"], 
                                                                      transform, ShareSentValue=fnShare(SentValue))));
    USBETXNsDataSeriesSentRecieved_df <- do.call("rbind", as.list(by(USBETXNsDataSeriesSentRecieved_df, 
                                                                      USBETXNsDataSeriesSentRecieved_df["date"], 
                                                                      transform, ShareRecievedVolume=fnShare(RecievedVolume))));
    USBETXNsDataSeriesSentRecieved_df <- do.call("rbind", as.list(by(USBETXNsDataSeriesSentRecieved_df, 
                                                                      USBETXNsDataSeriesSentRecieved_df["date"], 
                                                                      transform, ShareRecievedValue=fnShare(RecievedValue))));
    
    
    USBETXNsDataSeriesSentRecieved <-  as.data.table(USBETXNsDataSeriesSentRecieved_df);
    USBETXNsDataSeriesSentRecieved<- USBETXNsDataSeriesSentRecieved[, c("TotalVolume") := sum(RecievedVolume, SentVolume), 
                                                                      with = FALSE, by=c("sender","date")];
    USBETXNsDataSeriesSentRecieved<- USBETXNsDataSeriesSentRecieved[, c("TotalValue") := sum(RecievedValue, SentValue), 
                                                                      with = FALSE, by=c("sender","date")];
    ##Remove and rename duplicated fields
    USBETXNsDataSeriesSentRecieved[ ,c("senderID.y", "senderFullName.y") := NULL];
    setnames(USBETXNsDataSeriesSentRecieved,"senderID.x","senderID");
    setnames(USBETXNsDataSeriesSentRecieved,"senderFullName.x","senderFullName");
    
    
    #Remove temporary working datasets from memory
    rm(USBETXNsDataSeriesSent_df);
    rm(USBETXNsDataSeriesRecieved_df);
    rm(USBETXNsDataSeriesSentRecieved_df);
    
  }else if(dataFrequency == "dateTime"){
    ##Unlike LVTS, USBE and USBE data are netted and thus do not have the time dimension to them.
    ##This place holder has been left to mark where the code will go if at a later date this dimenssion is included
  }
  
}else{
  if(dataFrequency == "monthly"){
    
    USBETXNsDataSeriesBySender <-  mergedUSBETXNsYearsSpecYM[, list(SentVolume=sum(volumeRecieved), RecievedVolume=sum(volumeSent),
                                                                      TotalVolume=sum(volumeRecieved, volumeSent),
                                                                      SentValue=sum(valueRecieved), RecievedValue=sum(valueSent),
                                                                      TotalValue=sum(valueRecieved, valueSent)
    ), 
    by=c("sender","senderID","senderFullName","stream","Year_Month")];
    USBETXNsDataSeriesBySender  <-  USBETXNsDataSeriesBySender[order(Year_Month, sender), ];
    
    
    USBETXNsDataSeriesByPeriod <-  mergedUSBETXNsYearsSpecYM[, list(SentVolume=sum(volumeRecieved), RecievedVolume=sum(volumeSent),
                                                                      TotalVolume=sum(volumeRecieved, volumeSent),
                                                                      SentValue=sum(valueRecieved), RecievedValue=sum(valueSent),
                                                                      TotalValue=sum(valueRecieved, valueSent)
    ), 
    by=c("Year_Month","stream")];
    
    USBETXNsDataSeriesByPeriod  <-  USBETXNsDataSeriesByPeriod[order(Year_Month), ];
    
    USBETXNsDataSeriesBySenderReceiver <-  mergedUSBETXNsYearsSpecYM[, list(SentVolume=sum(volumeRecieved), RecievedVolume=sum(volumeSent),
                                                                              TotalVolume=sum(volumeRecieved, volumeSent),
                                                                              SentValue=sum(valueRecieved), RecievedValue=sum(valueSent),
                                                                              TotalValue=sum(valueRecieved, valueSent)
    ), 
    by=c("Year_Month","stream","sender","senderID","senderFullName","receiver","receiverID","receiverFullName")];
    
    USBETXNsDataSeriesBySenderReceiver  <-  USBETXNsDataSeriesBySenderReceiver[order(Year_Month, sender, receiver), ];
    
    ##Note that the by FI sent and recieved data has to be collated by way of the sender because of the way the USBE extract files are generated
    ##i.e. each line record in the extract is from the persepcetive of the sender at a point in time. Hence in each line sender X sends and recieves 
    ##some volume and value of payments to and from reciever Y
    USBETXNsDataSeriesSent  <-  mergedUSBETXNsYearsSpecYM[, list(SentVolume=sum(volumeRecieved), SentValue=sum(valueRecieved)
    ), 
    by=c("sender","senderID","senderFullName","stream","Year_Month")];
    
    USBETXNsDataSeriesRecieved  <-  mergedUSBETXNsYearsSpecYM[, list(RecievedVolume=sum(volumeSent), RecievedValue=sum(valueSent)
    ), 
    by=c("sender","senderID","senderFullName","stream","Year_Month")];
    
    
    USBETXNsDataSeriesSent_df <-  as.data.frame(USBETXNsDataSeriesSent);
    USBETXNsDataSeriesRecieved_df <-  as.data.frame(USBETXNsDataSeriesRecieved);
    USBETXNsDataSeriesSentRecieved_df  <-  merge(USBETXNsDataSeriesSent_df, USBETXNsDataSeriesRecieved_df, by.x=c("sender","stream","Year_Month"), 
                                                  by.y=c("sender","stream","Year_Month"), all.x = TRUE, all.y = TRUE);
    
    
    USBETXNsDataSeriesSentRecieved_df <- do.call("rbind", as.list(by(USBETXNsDataSeriesSentRecieved_df, 
                                                                      USBETXNsDataSeriesSentRecieved_df["Year_Month"], 
                                                                      transform, ShareSentVolume=fnShare(SentVolume))));
    USBETXNsDataSeriesSentRecieved_df <- do.call("rbind", as.list(by(USBETXNsDataSeriesSentRecieved_df, 
                                                                      USBETXNsDataSeriesSentRecieved_df["Year_Month"], 
                                                                      transform, ShareSentValue=fnShare(SentValue))));
    USBETXNsDataSeriesSentRecieved_df <- do.call("rbind", as.list(by(USBETXNsDataSeriesSentRecieved_df, 
                                                                      USBETXNsDataSeriesSentRecieved_df["Year_Month"], 
                                                                      transform, ShareRecievedVolume=fnShare(RecievedVolume))));
    USBETXNsDataSeriesSentRecieved_df <- do.call("rbind", as.list(by(USBETXNsDataSeriesSentRecieved_df, 
                                                                      USBETXNsDataSeriesSentRecieved_df["Year_Month"], 
                                                                      transform, ShareRecievedValue=fnShare(RecievedValue))));
    
    USBETXNsDataSeriesSentRecieved <-  as.data.table(USBETXNsDataSeriesSentRecieved_df);
    USBETXNsDataSeriesSentRecieved <- USBETXNsDataSeriesSentRecieved[, c("TotalVolume") := sum(RecievedVolume, SentVolume), 
                                                                       with = FALSE, by=c("sender","stream","Year_Month")];
    USBETXNsDataSeriesSentRecieved <- USBETXNsDataSeriesSentRecieved[, c("TotalValue") := sum(RecievedValue, SentValue), 
                                                                       with = FALSE, by=c("sender","stream","Year_Month")];
    
    ##Remove and rename duplicated fields
    USBETXNsDataSeriesSentRecieved[ ,c("senderID.y", "senderFullName.y") := NULL];
    setnames(USBETXNsDataSeriesSentRecieved,"senderID.x","senderID");
    setnames(USBETXNsDataSeriesSentRecieved,"senderFullName.x","senderFullName");
    
    
    
    rm(USBETXNsDataSeriesSent_df);
    rm(USBETXNsDataSeriesRecieved_df);
    rm(USBETXNsDataSeriesSentRecieved_df);
    
    
  }else if(dataFrequency == "quarterly"){
    
    USBETXNsDataSeriesBySender <-  mergedUSBETXNsYearsSpecYM[, list(SentVolume=sum(volumeRecieved), RecievedVolume=sum(volumeSent),
                                                                      TotalVolume=sum(volumeRecieved, volumeSent),
                                                                      SentValue=sum(valueRecieved), RecievedValue=sum(valueSent),
                                                                      TotalValue=sum(valueRecieved, valueSent)
    ), 
    by=c("sender","senderFullName","stream","Year_Quarter")];
    
    USBETXNsDataSeriesBySender <-  USBETXNsDataSeriesBySender[order(Year_Quarter, sender), ];
    
    USBETXNsDataSeriesByPeriod <-  mergedUSBETXNsYearsSpecYM[, list(SentVolume=sum(volumeRecieved), RecievedVolume=sum(volumeSent),
                                                                      TotalVolume=sum(volumeRecieved, volumeSent),
                                                                      SentValue=sum(valueRecieved), RecievedValue=sum(valueSent),
                                                                      TotalValue=sum(valueRecieved, valueSent)
    ), 
    by=c("Year_Quarter","stream")];
    USBETXNsDataSeriesByPeriod <-  USBETXNsDataSeriesByPeriod[order(Year_Quarter), ];
    
    USBETXNsDataSeriesBySenderReceiver <-  mergedUSBETXNsYearsSpecYM[, list(SentVolume=sum(volumeRecieved), RecievedVolume=sum(volumeSent),
                                                                              TotalVolume=sum(volumeRecieved, volumeSent),
                                                                              SentValue=sum(valueRecieved), RecievedValue=sum(valueSent),
                                                                              TotalValue=sum(valueRecieved, valueSent)
    ), 
    by=c("Year_Quarter","stream","sender","senderID","receiver","receiverID")];
    
    USBETXNsDataSeriesBySenderReceiver <-  USBETXNsDataSeriesBySenderReceiver[order(Year_Quarter, sender, receiver), ];
    
    ##Note that the by FI sent and recieved data has to be collated by way of the sender because of the way the USBE extract files are generated
    ##i.e. each line record in the extract is from the persepcetive of the sender at a point in time. Hence in each line sender X sends and recieves 
    ##some volume and value of payments to and from reciever Y
    USBETXNsDataSeriesSent  <-  mergedUSBETXNsYearsSpecYM[, list(SentVolume=sum(volumeRecieved), SentValue=sum(valueRecieved)
    ), 
    by=c("sender","senderID","senderFullName","stream","Year_Quarter")];
    
    USBETXNsDataSeriesRecieved  <-  mergedUSBETXNsYearsSpecYM[, list(RecievedVolume=sum(volumeSent), RecievedValue=sum(valueSent)
    ), 
    by=c("sender","senderID","senderFullName","stream","Year_Quarter")];
    
    
    
    
    USBETXNsDataSeriesSent_df <-  as.data.frame(USBETXNsDataSeriesSent);
    USBETXNsDataSeriesRecieved_df <-  as.data.frame(USBETXNsDataSeriesRecieved);
    USBETXNsDataSeriesSentRecieved_df <-  merge(USBETXNsDataSeriesSent_df, USBETXNsDataSeriesRecieved_df, by.x="sender", by.y="sender");
    USBETXNsDataSeriesSentRecieved_df  <-  merge(USBETXNsDataSeriesSent_df, USBETXNsDataSeriesRecieved_df, by.x=c("sender","stream","Year_Quarter"), 
                                                  by.y=c("sender","stream","Year_Quarter"), all.x = TRUE, all.y = TRUE);
    
    
    USBETXNsDataSeriesSentRecieved_df <- do.call("rbind", as.list(by(USBETXNsDataSeriesSentRecieved_df, 
                                                                      USBETXNsDataSeriesSentRecieved_df["Year_Quarter"], 
                                                                      transform, ShareSentVolume=fnShare(SentVolume))));
    USBETXNsDataSeriesSentRecieved_df <- do.call("rbind", as.list(by(USBETXNsDataSeriesSentRecieved_df, 
                                                                      USBETXNsDataSeriesSentRecieved_df["Year_Quarter"], 
                                                                      transform, ShareSentValue=fnShare(SentValue))));
    USBETXNsDataSeriesSentRecieved_df <- do.call("rbind", as.list(by(USBETXNsDataSeriesSentRecieved_df, 
                                                                      USBETXNsDataSeriesSentRecieved_df["Year_Quarter"], 
                                                                      transform, ShareRecievedVolume=fnShare(RecievedVolume))));
    USBETXNsDataSeriesSentRecieved_df <- do.call("rbind", as.list(by(USBETXNsDataSeriesSentRecieved_df, 
                                                                      USBETXNsDataSeriesSentRecieved_df["Year_Quarter"], 
                                                                      transform, ShareRecievedValue=fnShare(RecievedValue))));
    
    
    USBETXNsDataSeriesSentRecieved <-  as.data.table(USBETXNsDataSeriesSentRecieved_df);
    USBETXNsDataSeriesSentRecieved <- USBETXNsDataSeriesSentRecieved[, c("TotalVolume") := sum(RecievedVolume, SentVolume), 
                                                                       with = FALSE, by=c("sender","stream","Year_Quarter")];
    USBETXNsDataSeriesSentRecieved <- USBETXNsDataSeriesSentRecieved[, c("TotalValue") := sum(RecievedValue, SentValue), 
                                                                       with = FALSE, by=c("sender","stream","Year_Quarter")];
    
    ##Remove and rename duplicated fields
    USBETXNsDataSeriesSentRecieved[ ,c("senderID.y", "senderFullName.y") := NULL];
    setnames(USBETXNsDataSeriesSentRecieved,"senderID.x","senderID");
    setnames(USBETXNsDataSeriesSentRecieved,"senderFullName.x","senderFullName");
    
    
    
    rm(USBETXNsDataSeriesSent_df);
    rm(USBETXNsDataSeriesRecieved_df);
    rm(USBETXNsDataSeriesSentRecieved_df);
    
  }else if(dataFrequency == "annual"){
    
    USBETXNsDataSeriesBySender  <-  mergedUSBETXNsYearsSpecYM[, list(SentVolume=sum(volumeRecieved), RecievedVolume=sum(volumeSent),
                                                                       TotalVolume=sum(volumeRecieved, volumeSent),
                                                                       SentValue=sum(valueRecieved), RecievedValue=sum(valueSent),
                                                                       TotalValue=sum(valueRecieved, valueSent)
    ), 
    by=c("sender","senderID","senderFullName","stream","Year")];
    
    USBETXNsDataSeriesBySender <-  USBETXNsDataSeriesBySender[order(Year, sender), ];
    
    USBETXNsDataSeriesByPeriod  <-  mergedUSBETXNsYearsSpecYM[, list(SentVolume=sum(volumeRecieved), RecievedVolume=sum(volumeSent),
                                                                       TotalVolume=sum(volumeRecieved, volumeSent),
                                                                       SentValue=sum(valueRecieved), RecievedValue=sum(valueSent),
                                                                       TotalValue=sum(valueRecieved, valueSent)
    ), 
    by=c("Year","stream")];
    USBETXNsDataSeriesByPeriod <-  USBETXNsDataSeriesByPeriod[order(Year), ];
    
    USBETXNsDataSeriesBySenderReceiver  <-  mergedUSBETXNsYearsSpecYM[, list(SentVolume=sum(volumeRecieved), RecievedVolume=sum(volumeSent),
                                                                               TotalVolume=sum(volumeRecieved, volumeSent),
                                                                               SentValue=sum(valueRecieved), RecievedValue=sum(valueSent),
                                                                               TotalValue=sum(valueRecieved, valueSent)
    ), 
    by=c("Year","stream","sender","senderID","senderFullName","receiver","receiverID","receiverFullName")];
    
    USBETXNsDataSeriesBySenderReceiver <-  USBETXNsDataSeriesBySenderReceiver[order(Year, sender, receiver), ];
    
    ##Note that the by FI sent and recieved data has to be collated by way of the sender because of the way the USBE extract files are generated
    ##i.e. each line record in the extract is from the persepcetive of the sender at a point in time. Hence in each line sender X sends and recieves 
    ##some volume and value of payments to and from reciever Y
    USBETXNsDataSeriesSent  <-  mergedUSBETXNsYearsSpecYM[, list(SentVolume=sum(volumeRecieved), SentValue=sum(valueRecieved)
    ), 
    by=c("sender","senderID","senderFullName","stream","Year")];
    
    USBETXNsDataSeriesRecieved  <-  mergedUSBETXNsYearsSpecYM[, list(RecievedVolume=sum(volumeSent), RecievedValue=sum(valueSent)
    ), 
    by=c("sender","senderID","senderFullName","stream","Year")];
    
    
    
    USBETXNsDataSeriesSent_df <-  as.data.frame(USBETXNsDataSeriesSent);
    USBETXNsDataSeriesRecieved_df <-  as.data.frame(USBETXNsDataSeriesRecieved);
    USBETXNsDataSeriesSentRecieved_df <-  merge(USBETXNsDataSeriesSent_df, USBETXNsDataSeriesRecieved_df, by.x="sender", by.y="sender");
    USBETXNsDataSeriesSentRecieved_df  <-  merge(USBETXNsDataSeriesSent_df, USBETXNsDataSeriesRecieved_df, by.x=c("sender","stream","Year"), 
                                                  by.y=c("sender","stream","Year"), all.x = TRUE, all.y = TRUE);
    
    
    
    USBETXNsDataSeriesSentRecieved_df <- do.call("rbind", as.list(by(USBETXNsDataSeriesSentRecieved_df, 
                                                                      USBETXNsDataSeriesSentRecieved_df["Year"], 
                                                                      transform, ShareSentVolume=fnShare(SentVolume))));
    USBETXNsDataSeriesSentRecieved_df <- do.call("rbind", as.list(by(USBETXNsDataSeriesSentRecieved_df, 
                                                                      USBETXNsDataSeriesSentRecieved_df["Year"], 
                                                                      transform, ShareSentValue=fnShare(SentValue))));
    USBETXNsDataSeriesSentRecieved_df <- do.call("rbind", as.list(by(USBETXNsDataSeriesSentRecieved_df, 
                                                                      USBETXNsDataSeriesSentRecieved_df["Year"], 
                                                                      transform, ShareRecievedVolume=fnShare(RecievedVolume))));
    USBETXNsDataSeriesSentRecieved_df <- do.call("rbind", as.list(by(USBETXNsDataSeriesSentRecieved_df, 
                                                                      USBETXNsDataSeriesSentRecieved_df["Year"], 
                                                                      transform, ShareRecievedValue=fnShare(RecievedValue))));
    
    
    USBETXNsDataSeriesSentRecieved <-  as.data.table(USBETXNsDataSeriesSentRecieved_df);
    
    USBETXNsDataSeriesSentRecieved <- USBETXNsDataSeriesSentRecieved[, c("TotalVolume") := sum(RecievedVolume, SentVolume), 
                                                                       with = FALSE, by=c("sender","stream","Year")];
    USBETXNsDataSeriesSentRecieved <- USBETXNsDataSeriesSentRecieved[, c("TotalValue") := sum(RecievedValue, SentValue), 
                                                                       with = FALSE, by=c("sender","stream","Year")];
    
    ##Remove and rename duplicated fields
    USBETXNsDataSeriesSentRecieved[ ,c("senderID.y", "senderFullName.y") := NULL];
    setnames(USBETXNsDataSeriesSentRecieved,"senderID.x","senderID");
    setnames(USBETXNsDataSeriesSentRecieved,"senderFullName.x","senderFullName");
    
    rm(USBETXNsDataSeriesSent_df);
    rm(USBETXNsDataSeriesRecieved_df);
    rm(USBETXNsDataSeriesSentRecieved_df);
    
  }else if(dataFrequency == "daily"){
    USBETXNsDataSeriesBySender  <-  mergedUSBETXNsYearsSpecYM[, list(SentVolume=sum(TotalVolume),RecievedVolume=sum(TotalVolume),
                                                                       TotalVolume=sum(volumeRecieved, volumeSent),
                                                                       SentValue=sum(TotalValue), RecievedValue=sum(TotalValue),
                                                                       TotalValue=sum(valueRecieved, valueSent)
    ), 
    by=c("sender","senderID","senderFullName","stream","date")];
    
    USBETXNsDataSeriesBySender <-   USBETXNsDataSeriesBySender[order(date, sender), ];
    
    USBETXNsDataSeriesByPeriod  <-  mergedUSBETXNsYearsSpecYM[, list(SentVolume=sum(TotalVolume),RecievedVolume=sum(TotalVolume),
                                                                       TotalVolume=sum(volumeRecieved, volumeSent),
                                                                       SentValue=sum(TotalValue), RecievedValue=sum(TotalValue),
                                                                       TotalValue=sum(valueRecieved, valueSent)
    ),
    by=c("date","stream")];
    USBETXNsDataSeriesByPeriod <-   USBETXNsDataSeriesByPeriod[order(date), ];
    
    USBETXNsDataSeriesBySenderReceiver  <-  mergedUSBETXNsYearsSpecYM[, list(SentVolume=sum(TotalVolume),RecievedVolume=sum(TotalVolume),
                                                                               TotalVolume=sum(volumeRecieved, volumeSent),
                                                                               SentValue=sum(TotalValue), RecievedValue=sum(TotalValue),
                                                                               TotalValue=sum(valueRecieved, valueSent)
    ), 
    by=c("date","stream","sender","senderID","senderFullName","receiver","receiverID","receiverFullName")];
    
    USBETXNsDataSeriesBySenderReceiver <- USBETXNsDataSeriesBySenderReceiver[order(date, sender, receiver), ];
    
    
    USBETXNsDataSeriesSent  <-  mergedUSBETXNsYearsSpecYM[, list(SentVolume=sum(TotalVolume), SentValue=sum(TotalValue)
    ), 
    by=c("sender","stream","date")];
    
    USBETXNsDataSeriesRecieved  <-  mergedUSBETXNsYearsSpecYM[, list(RecievedVolume=sum(TotalVolume), RecievedValue=sum(TotalValue)
    ), 
    by=c("sender","stream","date")];
    
    
    USBETXNsDataSeriesSent_df <-  as.data.frame(USBETXNsDataSeriesSent);
    USBETXNsDataSeriesRecieved_df <-  as.data.frame(USBETXNsDataSeriesRecieved);
    USBETXNsDataSeriesSentRecieved_df  <-  merge(USBETXNsDataSeriesSent_df, USBETXNsDataSeriesRecieved_df, by.x=c("sender","stream","date"), 
                                                  by.y=c("sender","stream","date"),all.x = TRUE, all.y = TRUE);
    
    USBETXNsDataSeriesSentRecieved_df <- do.call("rbind", as.list(by(USBETXNsDataSeriesSentRecieved_df, 
                                                                      USBETXNsDataSeriesSentRecieved_df["date"], 
                                                                      transform, ShareSentVolume=fnShare(SentVolume))));
    USBETXNsDataSeriesSentRecieved_df <- do.call("rbind", as.list(by(USBETXNsDataSeriesSentRecieved_df, 
                                                                      USBETXNsDataSeriesSentRecieved_df["date"], 
                                                                      transform, ShareSentValue=fnShare(SentValue))));
    USBETXNsDataSeriesSentRecieved_df <- do.call("rbind", as.list(by(USBETXNsDataSeriesSentRecieved_df, 
                                                                      USBETXNsDataSeriesSentRecieved_df["date"], 
                                                                      transform, ShareRecievedVolume=fnShare(RecievedVolume))));
    USBETXNsDataSeriesSentRecieved_df <- do.call("rbind", as.list(by(USBETXNsDataSeriesSentRecieved_df, 
                                                                      USBETXNsDataSeriesSentRecieved_df["date"], 
                                                                      transform, ShareRecievedValue=fnShare(RecievedValue))));
    
    
    USBETXNsDataSeriesSentRecieved <-  as.data.table(USBETXNsDataSeriesSentRecieved_df);
    USBETXNsDataSeriesSentRecieved<- USBETXNsDataSeriesSentRecieved[, c("TotalVolume") := sum(RecievedVolume, SentVolume), 
                                                                      with = FALSE, by=c("sender","stream","date")];
    USBETXNsDataSeriesSentRecieved<- USBETXNsDataSeriesSentRecieved[, c("TotalValue") := sum(RecievedValue, SentValue), 
                                                                      with = FALSE, by=c("sender","stream","date")];
    ##Remove and rename duplicated fields
    USBETXNsDataSeriesSentRecieved[ ,c("senderID.y", "senderFullName.y") := NULL];
    setnames(USBETXNsDataSeriesSentRecieved,"senderID.x","senderID");
    setnames(USBETXNsDataSeriesSentRecieved,"senderFullName.x","senderFullName");
    
    
    #Remove temporary working datasets from memory
    rm(USBETXNsDataSeriesSent_df);
    rm(USBETXNsDataSeriesRecieved_df);
    rm(USBETXNsDataSeriesSentRecieved_df);
    
  }else if(dataFrequency == "dateTime"){
    ##Unlike LVTS, USBE and USBE data are netted and thus do not have the time dimension to them.
    ##This place holder has been left to mark where the code will go if at a later date this dimenssion is included
  }
  
}##end of the stream level collation
 
gc();

USBETXNsDataSeries <- USBETXNsDataSeriesBySenderReceiver;

save(USBETXNsDataSeries,file=USBETXNsDataSeriesSavePath);
save(USBETXNsDataSeriesSentRecieved, file=USBETXNsDataSeriesSentRecievedSavePath);

##write data into STATA readable files
write.dta(USBETXNsDataSeriesByPeriod,file=USBETXNsDataSeriesSTATASavePath);
write.dta(USBETXNsDataSeriesBySender,file=USBETXNsDataSeriesBySenderSTATASavePath);
#write.dta(USBETXNsDataSeriesByPeriod, file=USBETXNsDataSeriesByPeriodSTATASavePath);
write.dta(USBETXNsDataSeriesSentRecieved, file=USBETXNsDataSeriesSentRecievedSTATASavePath);
write.csv2(USBETXNsDataSeriesSentRecieved, file=USBETXNsDataSeriesSentRecievedXLSSavePath);
write.csv2(USBETXNsDataSeriesBySender,file=USBETXNsDataSeriesBySenderXLSSavePath);
gc();

##return to the original directory
#setwd(cur.dir);