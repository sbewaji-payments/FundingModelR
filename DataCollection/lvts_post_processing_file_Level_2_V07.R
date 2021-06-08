##This script acts to merge the ACSS data with the FI static data and assign the appropriate BIC Codes based on memberIDs.
##This is required because the ACSS data only records memberIDs and not their BIC code.
##Consequently, in order to maintain consistency with the LVTS dataset BIC codes must be assigned to the ACSS data
## 
##The Script will import the saved data.table extract from the "acss_processing_file_V##_Start-to-End_Year_Selective_Extraction.R" code
##as well as the saved StaticData file extracted using the "process_CPA-MemberNamesBICandIDs.R"code
##
##Final input data tables that the code will work with are 
##        a: mergedACSSTransYearsSpecYM - The ACSS data file
##        b: fiStaticDataFile - The static data file
##
##
##
## Author : Segun Bewaji
## Creation Date : 03 Sept 2015
## Modified : Segun Bewaji
## Modifications Made: 03 Sept 2015
##        1) inclusion of the TotalValue and TotalVolume 
##          Code
##          (ACSSTransDataSeriesSentRecieved<- ACSSTransDataSeriesSentRecieved[, c("Total_Volume") := sum(RecievedVolume, SentVolume), 
##            with = FALSE, by=c("sender","Year_Quarter")];)
##        2) commented out code related to calculation of payment value and volume shares
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
load(LVTSStreamDataLoadPath);
load(extractedSummaryLVTSDataLoadPath);
gc();
print("data tables loaded");#used to record speed of computations
print(now());#used to record speed of computations
gc();
mergedLVTSTXNsYearsSpecYM_df <-  as.data.frame(mergedLVTSTXNsYearsSpecYM);#convert mergedACSSTransYearsSpecYM to a temporary data.frame object
fiStaticDataFile_df  <-  as.data.frame(fiStaticDataFile);#convert fiStaticDataFile to a temporary data.frame object
LVTSStreamDataFile_df <- as.data.frame(LVTSStreamData);
rm(mergedLVTSTXNsYearsSpecYM);#remove mergedACSSTransYearsSpecYM to free up memory since not in use
rm(fiStaticDataFile);#remove fiStaticDataFile to free up memory since not in use
rm(LVTSStreamData);#remove ACSSStreamData to free up memory since not in use



##merge mergedLVTSTXNsYearsSpecYM_df and fiStaticDataFile_df data.frame objects into the final mergedLVTSTXNsYearsSpecYM data table object
##starting by merging using the recieving member ID numbers followed by the sending member ID numbers.
##after each merge, the memberBIC and memberName columns will be renamed to 
##"receiver"("sender") and "receiverFullName"("senderFullName") as appropriate
##starting with the "receiver"
mergedLVTSTXNsYearsSpecYM <-  merge(mergedLVTSTXNsYearsSpecYM_df, fiStaticDataFile_df, by.x="receiver", by.y="memberBIC");
colnames(mergedLVTSTXNsYearsSpecYM)[colnames(mergedLVTSTXNsYearsSpecYM)=="memberID"] <- "receiverID";
colnames(mergedLVTSTXNsYearsSpecYM)[colnames(mergedLVTSTXNsYearsSpecYM)=="memberName"] <- "receiverFullName";

rm(mergedLVTSTXNsYearsSpecYM_df);#remove mergedLVTSTXNsYearsSpecYM_df to free up memory since not in use
gc(); #free up memory

##next the "sender"
##Note that for the sender details the code uses the main working data.frame object mergedLVTSTXNsYearsSpecYM and not
##the temporary one mergedLVTSTXNsYearsSpecYM_df
mergedLVTSTXNsYearsSpecYM <-  merge(mergedLVTSTXNsYearsSpecYM, fiStaticDataFile_df, by.x="sender", by.y="memberBIC");
colnames(mergedLVTSTXNsYearsSpecYM)[colnames(mergedLVTSTXNsYearsSpecYM)=="memberID"] <- "senderID";
colnames(mergedLVTSTXNsYearsSpecYM)[colnames(mergedLVTSTXNsYearsSpecYM)=="memberName"] <- "senderFullName";

rm(fiStaticDataFile_df);#remove fiStaticDataFile_df to free up memory since not in use
gc(); #free up memory


mergedLVTSTXNsYearsSpecYM <- as.data.table(mergedLVTSTXNsYearsSpecYM);#convert mergedACSSTransYearsSpecYM to a data.table object

#the next lines of code handles those instances where FIs data have to be merged
#The implementation requires that the pseudo BIC code, CPA FI ID and Full names of FIs are all known
#Scotia aquistion of ING
#mergedLVTSTXNsYearsSpecYM <- mergeTwoFIs(mergedLVTSTXNsYearsSpecYM, "NOSCCA", "002", "The Bank of Nova Scotia", "INGBCA", dataFrequency);

gc(); #free up memory



##the RecievedVolume=TotalVolume, and RecievedValue=TotalVolume,TotalVolume=TotalVolume, TotalValue=TotalValue is used because the LVTS data 
##is only in the form of transaction value sent it is therefor assumed that the 
if(dataFrequency == "monthly"){

  
  #If aggregation should be at tranche level or overall payment
  if(byTranche){
    
    ##Create a new data.table object from the merged data table collecting the total volume and value sent and recieved by an FI 
    LVTSTXNsDataSeriesBySender <-  mergedLVTSTXNsYearsSpecYM[, list(SentVolume=sum(TotalVolume),RecievedVolume=sum(TotalVolume),
                                                                      TotalVolume=sum(TotalVolume),
                                                                      SentValue=sum(TotalValue), RecievedValue=sum(TotalValue),
                                                                      TotalValue=sum(TotalValue)
    ), 
    by=c("sender","senderFullName","Year_Month","tranche")];
    LVTSTXNsDataSeriesBySender  <-  LVTSTXNsDataSeriesBySender[order(Year_Month, sender), ];
    
    LVTSTXNsDataSeriesByPeriod <-  mergedLVTSTXNsYearsSpecYM[, list(SentVolume=sum(TotalVolume),RecievedVolume=sum(TotalVolume),
                                                                      TotalVolume=sum(TotalVolume),
                                                                      SentValue=sum(TotalValue), RecievedValue=sum(TotalValue),
                                                                      TotalValue=sum(TotalValue)
    ), 
    by=c("Year_Month","tranche")];
    
    LVTSTXNsDataSeriesByPeriod  <-  LVTSTXNsDataSeriesByPeriod[order(Year_Month), ];
    
    
    
    LVTSTXNsDataSeriesBySenderReceiver <-  mergedLVTSTXNsYearsSpecYM[, list(SentVolume=sum(TotalVolume),RecievedVolume=sum(TotalVolume),
                                                                              TotalVolume=sum(TotalVolume),
                                                                              SentValue=sum(TotalValue), RecievedValue=sum(TotalValue),
                                                                              TotalValue=sum(TotalValue)
    ), 
    by=c("Year_Month","sender","senderID","senderFullName","receiver","receiverID","receiverFullName","tranche")];
    
    #ensure the tranche field is a numeric and not a factor
    LVTSTXNsDataSeriesBySenderReceiver$tranche <- as.numeric(LVTSTXNsDataSeriesBySenderReceiver$tranche);
    
    LVTSTXNsDataSeriesSent  <-  mergedLVTSTXNsYearsSpecYM[, list(SentVolume=sum(TotalVolume), SentValue=sum(TotalValue)
    ), 
    by=c("sender","senderID","senderFullName","Year_Month","tranche")];
    
    LVTSTXNsDataSeriesRecieved  <-  mergedLVTSTXNsYearsSpecYM[, list(RecievedVolume=sum(TotalVolume), RecievedValue=sum(TotalValue)
    ), 
    by=c("receiver","receiverID","receiverFullName","Year_Month","tranche")];
    
    
    
    LVTSTXNsDataSeriesSent_df <-  as.data.frame(LVTSTXNsDataSeriesSent);
    LVTSTXNsDataSeriesRecieved_df <-  as.data.frame(LVTSTXNsDataSeriesRecieved);
#    LVTSTXNsDataSeriesSentRecieved_df  <-  merge(LVTSTXNsDataSeriesSent_df, LVTSTXNsDataSeriesRecieved_df, by.x=c("sender","Year_Month","tranche"), 
#                                                  by.y=c("receiver","Year_Month","tranche"), all.x = TRUE, all.y = TRUE);
    
    LVTSTXNsDataSeriesSentRecieved_df  <-  merge(LVTSTXNsDataSeriesRecieved_df, LVTSTXNsDataSeriesSent_df, by.x=c("receiver","Year_Month", "tranche"),
                                                 by.y=c("sender","Year_Month", "tranche"), all.x = TRUE, all.y = TRUE);
    
    
    LVTSTXNsDataSeriesSentRecieved <-  as.data.table(LVTSTXNsDataSeriesSentRecieved_df);
    LVTSTXNsDataSeriesSentRecieved[is.na(LVTSTXNsDataSeriesSentRecieved)] = 0;
    setnames(LVTSTXNsDataSeriesSentRecieved, "receiver", "sender");
    LVTSTXNsDataSeriesSentRecieved<- LVTSTXNsDataSeriesSentRecieved[, c("TotalVolume") := sum(SentVolume, RecievedVolume), 
                                                                      with = FALSE, by=c("sender","Year_Month","tranche")];
    LVTSTXNsDataSeriesSentRecieved<- LVTSTXNsDataSeriesSentRecieved[, c("TotalValue") := sum(SentValue, RecievedValue), 
                                                                      with = FALSE, by=c("sender","Year_Month","tranche")];
    
    LVTSTXNsDataSeriesSentRecieved[ ,c("senderID","senderFullName"):=NULL];
    setnames(LVTSTXNsDataSeriesSentRecieved, "receiverID", "senderID");
    setnames(LVTSTXNsDataSeriesSentRecieved, "receiverFullName", "senderFullName");
    
    
    
  }else{
    
    ##Create a new data.table object from the merged data table collecting the total volume and value sent and recieved by an FI 
    LVTSTXNsDataSeriesBySender <-  mergedLVTSTXNsYearsSpecYM[, list(SentVolume=sum(TotalVolume),RecievedVolume=sum(TotalVolume),
                                                                      TotalVolume=sum(TotalVolume),
                                                                      SentValue=sum(TotalValue), RecievedValue=sum(TotalValue),
                                                                      TotalValue=sum(TotalValue)
    ), 
    by=c("sender","senderFullName","Year_Month")];
    LVTSTXNsDataSeriesBySender  <-  LVTSTXNsDataSeriesBySender[order(Year_Month, sender), ];
    
    LVTSTXNsDataSeriesByPeriod <-  mergedLVTSTXNsYearsSpecYM[, list(SentVolume=sum(TotalVolume),RecievedVolume=sum(TotalVolume),
                                                                      TotalVolume=sum(TotalVolume),
                                                                      SentValue=sum(TotalValue), RecievedValue=sum(TotalValue),
                                                                      TotalValue=sum(TotalValue)
    ), 
    by=c("Year_Month")];
    
    LVTSTXNsDataSeriesByPeriod  <-  LVTSTXNsDataSeriesByPeriod[order(Year_Month), ];
    
    
    
    LVTSTXNsDataSeriesBySenderReceiver <-  mergedLVTSTXNsYearsSpecYM[, list(SentVolume=sum(TotalVolume),RecievedVolume=sum(TotalVolume),
                                                                              TotalVolume=sum(TotalVolume),
                                                                              SentValue=sum(TotalValue), RecievedValue=sum(TotalValue),
                                                                              TotalValue=sum(TotalValue)
    ), 
    by=c("Year_Month","sender","senderID","senderFullName","receiver","receiverID","receiverFullName")];
    
    
    LVTSTXNsDataSeriesSent  <-  mergedLVTSTXNsYearsSpecYM[, list(SentVolume=sum(TotalVolume), SentValue=sum(TotalValue)
    ), 
    by=c("sender","senderID","senderFullName","Year_Month")];
    
    LVTSTXNsDataSeriesRecieved  <-  mergedLVTSTXNsYearsSpecYM[, list(RecievedVolume=sum(TotalVolume), RecievedValue=sum(TotalValue)
    ), 
    by=c("receiver","receiverID","receiverFullName","Year_Month")];
    
    
    
    LVTSTXNsDataSeriesSent_df <-  as.data.frame(LVTSTXNsDataSeriesSent);
    LVTSTXNsDataSeriesRecieved_df <-  as.data.frame(LVTSTXNsDataSeriesRecieved);
#    LVTSTXNsDataSeriesSentRecieved_df  <-  merge(LVTSTXNsDataSeriesSent_df, LVTSTXNsDataSeriesRecieved_df, by.x=c("sender","Year_Month"), 
#                                                  by.y=c("receiver","Year_Month"), all.x = TRUE, all.y = TRUE);
    
    LVTSTXNsDataSeriesSentRecieved_df  <-  merge(LVTSTXNsDataSeriesRecieved_df, LVTSTXNsDataSeriesSent_df, by.x=c("receiver","Year_Month"),
                                                 by.y=c("sender","Year_Month"), all.x = TRUE, all.y = TRUE);
    
    
    LVTSTXNsDataSeriesSentRecieved <-  as.data.table(LVTSTXNsDataSeriesSentRecieved_df);
    LVTSTXNsDataSeriesSentRecieved[is.na(LVTSTXNsDataSeriesSentRecieved)] = 0;
    setnames(LVTSTXNsDataSeriesSentRecieved, "receiver", "sender");
    LVTSTXNsDataSeriesSentRecieved<- LVTSTXNsDataSeriesSentRecieved[, c("TotalVolume") := sum(SentVolume, RecievedVolume), 
                                                                      with = FALSE, by=c("sender","Year_Month")];
    LVTSTXNsDataSeriesSentRecieved<- LVTSTXNsDataSeriesSentRecieved[, c("TotalValue") := sum(SentValue, RecievedValue), 
                                                                      with = FALSE, by=c("sender","Year_Month")];
    
    LVTSTXNsDataSeriesSentRecieved[ ,c("senderID","senderFullName"):=NULL];
    setnames(LVTSTXNsDataSeriesSentRecieved, "receiverID", "senderID");
    setnames(LVTSTXNsDataSeriesSentRecieved, "receiverFullName", "senderFullName");
    
    
  }
  #order by date/time then sender then reciever
  LVTSTXNsDataSeriesBySenderReceiver  <-  LVTSTXNsDataSeriesBySenderReceiver[order(Year_Month, sender, receiver), ];
  
  rm(LVTSTXNsDataSeriesSent_df);
  rm(LVTSTXNsDataSeriesRecieved_df);
  rm(LVTSTXNsDataSeriesSentRecieved_df);
  
  

  
}else if(dataFrequency == "quarterly"){
  
  
    #If aggregation should be at tranche level or overall payment
  if(byTranche){
    LVTSTXNsDataSeriesBySender <-  mergedLVTSTXNsYearsSpecYM[, list(SentVolume=sum(TotalVolume),RecievedVolume=sum(TotalVolume),
                                                                      TotalVolume=sum(TotalVolume),
                                                                      SentValue=sum(TotalValue), RecievedValue=sum(TotalValue),
                                                                      TotalValue=sum(TotalValue)
    ), 
    by=c("sender","senderFullName","Year_Quarter","tranche")];
    
    LVTSTXNsDataSeriesBySender <-  LVTSTXNsDataSeriesBySender[order(Year_Quarter, sender), ];
    
    LVTSTXNsDataSeriesByPeriod <-  mergedLVTSTXNsYearsSpecYM[, list(SentVolume=sum(TotalVolume),RecievedVolume=sum(TotalVolume),
                                                                      TotalVolume=sum(TotalVolume),
                                                                      SentValue=sum(TotalValue), RecievedValue=sum(TotalValue),
                                                                      TotalValue=sum(TotalValue)
    ), 
    by=c("Year_Quarter","tranche")];
    LVTSTXNsDataSeriesByPeriod <- LVTSTXNsDataSeriesByPeriod[order(Year_Quarter), ];
    
    LVTSTXNsDataSeriesBySenderReceiver <-  mergedLVTSTXNsYearsSpecYM[, list(SentVolume=TotalVolume,RecievedVolume=TotalVolume,
                                                                              TotalVolume=TotalVolume,
                                                                              SentValue=sum(TotalValue), RecievedValue=sum(TotalValue),
                                                                              TotalValue=sum(TotalValue)
    ), 
    by=c("Year_Quarter","sender","senderID","senderFullName","receiver","receiverID","receiverFullName","tranche")];
    
    #ensure the tranche field is a numeric and not a factor
    LVTSTXNsDataSeriesBySenderReceiver$tranche <- as.numeric(LVTSTXNsDataSeriesBySenderReceiver$tranche);
    
    LVTSTXNsDataSeriesSent  <-  mergedLVTSTXNsYearsSpecYM[, list(SentVolume=sum(TotalVolume), SentValue=sum(TotalValue)
    ), 
    by=c("sender","senderID","senderFullName","Year_Quarter","tranche")];
    
    LVTSTXNsDataSeriesRecieved  <-  mergedLVTSTXNsYearsSpecYM[, list(RecievedVolume=sum(TotalVolume), RecievedValue=sum(TotalValue)
    ), 
    by=c("receiver","receiverID","receiverFullName","Year_Quarter","tranche")];
    
    
    LVTSTXNsDataSeriesSent_df <-  as.data.frame(LVTSTXNsDataSeriesSent);
    LVTSTXNsDataSeriesRecieved_df <-  as.data.frame(LVTSTXNsDataSeriesRecieved);
   # LVTSTXNsDataSeriesSentRecieved_df  <-  merge(LVTSTXNsDataSeriesSent_df, LVTSTXNsDataSeriesRecieved_df, by.x=c("sender","Year_Quarter","tranche"), 
    #                                              by.y=c("receiver","Year_Quarter","tranche"), all.x = TRUE, all.y = TRUE);
    
    LVTSTXNsDataSeriesSentRecieved_df  <-  merge(LVTSTXNsDataSeriesRecieved_df, LVTSTXNsDataSeriesSent_df, by.x=c("receiver","Year_Quarter", "tranche"),
                                                 by.y=c("sender","Year_Quarter", "tranche"), all.x = TRUE, all.y = TRUE);
    
    LVTSTXNsDataSeriesSentRecieved <-  as.data.table(LVTSTXNsDataSeriesSentRecieved_df);
    LVTSTXNsDataSeriesSentRecieved[is.na(LVTSTXNsDataSeriesSentRecieved)] = 0;
    setnames(LVTSTXNsDataSeriesSentRecieved, "receiver", "sender");
    LVTSTXNsDataSeriesSentRecieved<- LVTSTXNsDataSeriesSentRecieved[, c("TotalVolume") := sum(SentVolume, RecievedVolume), 
                                                                      with = FALSE, by=c("sender","Year_Quarter","tranche")];
    LVTSTXNsDataSeriesSentRecieved<- LVTSTXNsDataSeriesSentRecieved[, c("TotalValue") := sum(SentValue, RecievedValue), 
                                                                      with = FALSE, by=c("sender","Year_Quarter","tranche")];
    
    ##Remove duplicated fields
    LVTSTXNsDataSeriesSentRecieved[ ,c("senderID","senderFullName"):=NULL];
    setnames(LVTSTXNsDataSeriesSentRecieved, "receiverID", "senderID");
    setnames(LVTSTXNsDataSeriesSentRecieved, "receiverFullName", "senderFullName");
    

    
  }else{
    
    LVTSTXNsDataSeriesBySender <-  mergedLVTSTXNsYearsSpecYM[, list(SentVolume=sum(TotalVolume),RecievedVolume=sum(TotalVolume),
                                                                      TotalVolume=sum(TotalVolume),
                                                                      SentValue=sum(TotalValue), RecievedValue=sum(TotalValue),
                                                                      TotalValue=sum(TotalValue)
    ), 
    by=c("sender","senderFullName","Year_Quarter")];
    
    LVTSTXNsDataSeriesBySender <-  LVTSTXNsDataSeriesBySender[order(Year_Quarter, sender), ];
    
    LVTSTXNsDataSeriesByPeriod <-  mergedLVTSTXNsYearsSpecYM[, list(SentVolume=sum(TotalVolume),RecievedVolume=sum(TotalVolume),
                                                                      TotalVolume=sum(TotalVolume),
                                                                      SentValue=sum(TotalValue), RecievedValue=sum(TotalValue),
                                                                      TotalValue=sum(TotalValue)
    ), 
    by=c("Year_Quarter")];
    LVTSTXNsDataSeriesByPeriod <- LVTSTXNsDataSeriesByPeriod[order(Year_Quarter), ];
    
    LVTSTXNsDataSeriesBySenderReceiver <-  mergedLVTSTXNsYearsSpecYM[, list(SentVolume=TotalVolume,RecievedVolume=TotalVolume,
                                                                              TotalVolume=TotalVolume,
                                                                              SentValue=sum(TotalValue), RecievedValue=sum(TotalValue),
                                                                              TotalValue=sum(TotalValue)
    ), 
    by=c("Year_Quarter","sender","senderID","senderFullName","receiver","receiverID","receiverFullName")];
    
    
    
    LVTSTXNsDataSeriesSent  <-  mergedLVTSTXNsYearsSpecYM[, list(SentVolume=sum(TotalVolume), SentValue=sum(TotalValue)
    ), 
    by=c("sender","senderID","senderFullName","Year_Quarter")];
    
    LVTSTXNsDataSeriesRecieved  <-  mergedLVTSTXNsYearsSpecYM[, list(RecievedVolume=sum(TotalVolume), RecievedValue=sum(TotalValue)
    ), 
    by=c("receiver","receiverID","receiverFullName","Year_Quarter")];
    
    
    LVTSTXNsDataSeriesSent_df <-  as.data.frame(LVTSTXNsDataSeriesSent);
    LVTSTXNsDataSeriesRecieved_df <-  as.data.frame(LVTSTXNsDataSeriesRecieved);
#    LVTSTXNsDataSeriesSentRecieved_df  <-  merge(LVTSTXNsDataSeriesSent_df, LVTSTXNsDataSeriesRecieved_df, by.x=c("sender","Year_Quarter"), 
#                                                  by.y=c("receiver","Year_Quarter"), all.x = TRUE, all.y = TRUE);
    
    
    LVTSTXNsDataSeriesSentRecieved_df  <-  merge(LVTSTXNsDataSeriesRecieved_df, LVTSTXNsDataSeriesSent_df, by.x=c("receiver","Year_Quarter"),
                                                 by.y=c("sender","Year_Quarter"), all.x = TRUE, all.y = TRUE);
    
    LVTSTXNsDataSeriesSentRecieved <-  as.data.table(LVTSTXNsDataSeriesSentRecieved_df);
    LVTSTXNsDataSeriesSentRecieved[is.na(LVTSTXNsDataSeriesSentRecieved)] = 0;
    setnames(LVTSTXNsDataSeriesSentRecieved, "receiver", "sender");
    LVTSTXNsDataSeriesSentRecieved<- LVTSTXNsDataSeriesSentRecieved[, c("TotalVolume") := sum(SentVolume, RecievedVolume), 
                                                                      with = FALSE, by=c("sender","Year_Quarter")];
    LVTSTXNsDataSeriesSentRecieved<- LVTSTXNsDataSeriesSentRecieved[, c("TotalValue") := sum(SentValue, RecievedValue), 
                                                                      with = FALSE, by=c("sender","Year_Quarter")];
    
    ##Remove duplicated fields
    LVTSTXNsDataSeriesSentRecieved[ ,c("senderID","senderFullName"):=NULL];
    setnames(LVTSTXNsDataSeriesSentRecieved, "receiverID", "senderID");
    setnames(LVTSTXNsDataSeriesSentRecieved, "receiverFullName", "senderFullName");
    
  }
  #order by date/time then sender then reciever
  LVTSTXNsDataSeriesBySenderReceiver = LVTSTXNsDataSeriesBySenderReceiver[order(Year_Quarter, sender, receiver), ];

  
  rm(LVTSTXNsDataSeriesSent_df);
  rm(LVTSTXNsDataSeriesRecieved_df);
  rm(LVTSTXNsDataSeriesSentRecieved_df);
  
  
  
}else if(dataFrequency == "annual"){
  
    #If aggregation should be at tranche level or overall payment
  if(byTranche){
    
    LVTSTXNsDataSeriesBySender  <-  mergedLVTSTXNsYearsSpecYM[, list(SentVolume=sum(TotalVolume),RecievedVolume=sum(TotalVolume),
                                                                       TotalVolume=sum(TotalVolume),
                                                                       SentValue=sum(TotalValue), RecievedValue=sum(TotalValue),
                                                                       TotalValue=sum(TotalValue)
    ), 
    by=c("sender","senderFullName","Year","tranche")];
    
    LVTSTXNsDataSeriesBySender <-   LVTSTXNsDataSeriesBySender[order(Year, sender), ];
    
    LVTSTXNsDataSeriesByPeriod  <-  mergedLVTSTXNsYearsSpecYM[, list(SentVolume=sum(TotalVolume),RecievedVolume=sum(TotalVolume),
                                                                       TotalVolume=sum(TotalVolume),
                                                                       SentValue=sum(TotalValue), RecievedValue=sum(TotalValue),
                                                                       TotalValue=sum(TotalValue)
    ), 
    by=c("Year","tranche")];
    LVTSTXNsDataSeriesByPeriod <-   LVTSTXNsDataSeriesByPeriod[order(Year), ];
    
    
    LVTSTXNsDataSeriesBySenderReceiver  <-  mergedLVTSTXNsYearsSpecYM[, list(SentVolume=sum(TotalVolume),RecievedVolume=sum(TotalVolume),
                                                                               TotalVolume=sum(TotalVolume),
                                                                               SentValue=sum(TotalValue), RecievedValue=sum(TotalValue),
                                                                               TotalValue=sum(TotalValue)
    ), 
    by=c("Year","sender","senderID","senderFullName","receiver","receiverID","receiverFullName","tranche")];
    
    
    #ensure the tranche field is a numeric and not a factor
    LVTSTXNsDataSeriesBySenderReceiver$tranche <- as.numeric(LVTSTXNsDataSeriesBySenderReceiver$tranche);

    
    LVTSTXNsDataSeriesSent  <-  mergedLVTSTXNsYearsSpecYM[, list(SentVolume=sum(TotalVolume), SentValue=sum(TotalValue)
    ), 
    by=c("sender","senderID","senderFullName","Year","tranche")];
    
    LVTSTXNsDataSeriesRecieved  <-  mergedLVTSTXNsYearsSpecYM[, list(RecievedVolume=sum(TotalVolume), RecievedValue=sum(TotalValue)
    ), 
    by=c("receiver","receiverID","receiverFullName","Year","tranche")];
    
    
    LVTSTXNsDataSeriesSent_df <-  as.data.frame(LVTSTXNsDataSeriesSent);
    LVTSTXNsDataSeriesRecieved_df <-  as.data.frame(LVTSTXNsDataSeriesRecieved);
#    LVTSTXNsDataSeriesSentRecieved_df  <-  merge(LVTSTXNsDataSeriesSent_df, LVTSTXNsDataSeriesRecieved_df, by.x=c("sender","Year","tranche"), 
#                                                  by.y=c("receiver","Year","tranche"),all.x = TRUE, all.y = TRUE);
    

    LVTSTXNsDataSeriesSentRecieved_df  <-  merge(LVTSTXNsDataSeriesRecieved_df, LVTSTXNsDataSeriesSent_df, by.x=c("receiver","Year", "tranche"),
                                                 by.y=c("sender","Year", "tranche"), all.x = TRUE, all.y = TRUE);
    
        
    LVTSTXNsDataSeriesSentRecieved <-  as.data.table(LVTSTXNsDataSeriesSentRecieved_df);
    LVTSTXNsDataSeriesSentRecieved[is.na(LVTSTXNsDataSeriesSentRecieved)] = 0;
    setnames(LVTSTXNsDataSeriesSentRecieved, "receiver", "sender");
    LVTSTXNsDataSeriesSentRecieved<- LVTSTXNsDataSeriesSentRecieved[, c("TotalVolume") := sum(SentVolume, RecievedVolume), 
                                                                      with = FALSE, by=c("sender","Year","tranche")];
    LVTSTXNsDataSeriesSentRecieved<- LVTSTXNsDataSeriesSentRecieved[, c("TotalValue") := sum(SentValue, RecievedValue), 
                                                                      with = FALSE, by=c("sender","Year","tranche")];
    ##Remove duplicated fields
    LVTSTXNsDataSeriesSentRecieved[ ,c("senderID","senderFullName"):=NULL];
    setnames(LVTSTXNsDataSeriesSentRecieved, "receiverID", "senderID");
    setnames(LVTSTXNsDataSeriesSentRecieved, "receiverFullName", "senderFullName");
    

    
  }else{
    
    LVTSTXNsDataSeriesBySender  <-  mergedLVTSTXNsYearsSpecYM[, list(SentVolume=sum(TotalVolume),RecievedVolume=sum(TotalVolume),
                                                                       TotalVolume=sum(TotalVolume),
                                                                       SentValue=sum(TotalValue), RecievedValue=sum(TotalValue),
                                                                       TotalValue=sum(TotalValue)
    ), 
    by=c("sender","senderFullName","Year")];
    
    LVTSTXNsDataSeriesBySender <-   LVTSTXNsDataSeriesBySender[order(Year, sender), ];
    
    LVTSTXNsDataSeriesByPeriod  <-  mergedLVTSTXNsYearsSpecYM[, list(SentVolume=sum(TotalVolume),RecievedVolume=sum(TotalVolume),
                                                                       TotalVolume=sum(TotalVolume),
                                                                       SentValue=sum(TotalValue), RecievedValue=sum(TotalValue),
                                                                       TotalValue=sum(TotalValue)
    ), 
    by=c("Year")];
    LVTSTXNsDataSeriesByPeriod <-   LVTSTXNsDataSeriesByPeriod[order(Year), ];
    
    
    
    LVTSTXNsDataSeriesBySenderReceiver  <-  mergedLVTSTXNsYearsSpecYM[, list(SentVolume=sum(TotalVolume),RecievedVolume=sum(TotalVolume),
                                                                               TotalVolume=sum(TotalVolume),
                                                                               SentValue=sum(TotalValue), RecievedValue=sum(TotalValue),
                                                                               TotalValue=sum(TotalValue)
    ), 
    by=c("Year","sender","senderID","senderFullName","receiver","receiverID","receiverFullName")];
    
    
    LVTSTXNsDataSeriesSent  <-  mergedLVTSTXNsYearsSpecYM[, list(SentVolume=sum(TotalVolume), SentValue=sum(TotalValue)
    ), 
    by=c("sender","senderID","senderFullName","Year")];
    
    LVTSTXNsDataSeriesRecieved  <-  mergedLVTSTXNsYearsSpecYM[, list(RecievedVolume=sum(TotalVolume), RecievedValue=sum(TotalValue)
    ), 
    by=c("receiver","receiverID","receiverFullName","Year")];
    
    
    LVTSTXNsDataSeriesSent_df <-  as.data.frame(LVTSTXNsDataSeriesSent);
    LVTSTXNsDataSeriesRecieved_df <-  as.data.frame(LVTSTXNsDataSeriesRecieved);
#    LVTSTXNsDataSeriesSentRecieved_df  <-  merge(LVTSTXNsDataSeriesSent_df, LVTSTXNsDataSeriesRecieved_df, by.x=c("sender","Year"), 
#                                                  by.y=c("receiver","Year"),all.x = TRUE, all.y = TRUE);
    
    LVTSTXNsDataSeriesSentRecieved_df  <-  merge(LVTSTXNsDataSeriesRecieved_df, LVTSTXNsDataSeriesSent_df, by.x=c("receiver","Year"),
                                                 by.y=c("sender","Year"), all.x = TRUE, all.y = TRUE);
    
    LVTSTXNsDataSeriesSentRecieved <-  as.data.table(LVTSTXNsDataSeriesSentRecieved_df);
    LVTSTXNsDataSeriesSentRecieved[is.na(LVTSTXNsDataSeriesSentRecieved)] = 0;
    setnames(LVTSTXNsDataSeriesSentRecieved, "receiver", "sender");
    LVTSTXNsDataSeriesSentRecieved<- LVTSTXNsDataSeriesSentRecieved[, c("TotalVolume") := sum(SentVolume, RecievedVolume), 
                                                                      with = FALSE, by=c("sender","Year")];
    LVTSTXNsDataSeriesSentRecieved<- LVTSTXNsDataSeriesSentRecieved[, c("TotalValue") := sum(SentValue, RecievedValue), 
                                                                      with = FALSE, by=c("sender","Year")];
    ##Remove duplicated fields
    LVTSTXNsDataSeriesSentRecieved[ ,c("senderID","senderFullName"):=NULL];
    setnames(LVTSTXNsDataSeriesSentRecieved, "receiverID", "senderID");
    setnames(LVTSTXNsDataSeriesSentRecieved, "receiverFullName", "senderFullName");
    
    
    
  }
  #order by date/time then sender then reciever
  LVTSTXNsDataSeriesBySenderReceiver <- LVTSTXNsDataSeriesBySenderReceiver[order(Year, sender, receiver), ];
  
  
  
  rm(LVTSTXNsDataSeriesSent_df);
  rm(LVTSTXNsDataSeriesRecieved_df);
  rm(LVTSTXNsDataSeriesSentRecieved_df);

}else if(dataFrequency == "daily"){
   
  #If aggregation should be at tranche level or overall payment
  if(byTranche){
    
    LVTSTXNsDataSeriesBySender  <-  mergedLVTSTXNsYearsSpecYM[, list(SentVolume=sum(TotalVolume),RecievedVolume=sum(TotalVolume),
                                                                       TotalVolume=sum(TotalVolume),
                                                                       SentValue=sum(TotalValue), RecievedValue=sum(TotalValue),
                                                                       TotalValue=sum(TotalValue)
    ), 
    by=c("sender","senderFullName","date","tranche")];
    
    LVTSTXNsDataSeriesBySender <-   LVTSTXNsDataSeriesBySender[order(date, sender), ];
    
    LVTSTXNsDataSeriesByPeriod  <-  mergedLVTSTXNsYearsSpecYM[, list(SentVolume=sum(TotalVolume),RecievedVolume=sum(TotalVolume),
                                                                       TotalVolume=sum(TotalVolume),
                                                                       SentValue=sum(TotalValue), RecievedValue=sum(TotalValue),
                                                                       TotalValue=sum(TotalValue)
    ),
    by=c("date","tranche")];
    LVTSTXNsDataSeriesByPeriod <-   LVTSTXNsDataSeriesByPeriod[order(date), ];
    
    
    LVTSTXNsDataSeriesBySenderReceiver  <-  mergedLVTSTXNsYearsSpecYM[, list(SentVolume=sum(TotalVolume),RecievedVolume=sum(TotalVolume),
                                                                               TotalVolume=sum(TotalVolume),
                                                                               SentValue=sum(TotalValue), RecievedValue=sum(TotalValue),
                                                                               TotalValue=sum(TotalValue)
    ), 
    by=c("date","sender","senderID","senderFullName","receiver","receiverID","receiverFullName","tranche")];
    
    
    #ensure the tranche field is a numeric and not a factor
    LVTSTXNsDataSeriesBySenderReceiver$tranche <- as.numeric(LVTSTXNsDataSeriesBySenderReceiver$tranche);
    

    LVTSTXNsDataSeriesSent  <-  mergedLVTSTXNsYearsSpecYM[, list(SentVolume=sum(TotalVolume), SentValue=sum(TotalValue)
    ), 
    by=c("sender","senderID","senderFullName","date","tranche")];
    
    LVTSTXNsDataSeriesRecieved  <-  mergedLVTSTXNsYearsSpecYM[, list(RecievedVolume=sum(TotalVolume), RecievedValue=sum(TotalValue)
    ), 
    by=c("receiver","receiverID","receiverFullName","date","tranche")];
    
    
    LVTSTXNsDataSeriesSent_df <-  as.data.frame(LVTSTXNsDataSeriesSent);
    LVTSTXNsDataSeriesRecieved_df <-  as.data.frame(LVTSTXNsDataSeriesRecieved);
#    LVTSTXNsDataSeriesSentRecieved_df  <-  merge(LVTSTXNsDataSeriesSent_df, LVTSTXNsDataSeriesRecieved_df, by.x=c("sender","date","tranche"), 
#                                                  by.y=c("receiver","date","tranche"),all.x = TRUE, all.y = TRUE);
    
    LVTSTXNsDataSeriesSentRecieved_df  <-  merge(LVTSTXNsDataSeriesRecieved_df, LVTSTXNsDataSeriesSent_df, by.x=c("receiver","date", "tranche"),
                                                 by.y=c("sender","date", "tranche"), all.x = TRUE, all.y = TRUE);
    
    
    LVTSTXNsDataSeriesSentRecieved <-  as.data.table(LVTSTXNsDataSeriesSentRecieved_df);
    LVTSTXNsDataSeriesSentRecieved[is.na(LVTSTXNsDataSeriesSentRecieved)] = 0;
    setnames(LVTSTXNsDataSeriesSentRecieved, "receiver", "sender");
    LVTSTXNsDataSeriesSentRecieved<- LVTSTXNsDataSeriesSentRecieved[, c("TotalVolume") := sum(SentVolume, RecievedVolume), 
                                                                      with = FALSE, by=c("sender","date","tranche")];
    LVTSTXNsDataSeriesSentRecieved<- LVTSTXNsDataSeriesSentRecieved[, c("TotalValue") := sum(SentValue, RecievedValue), 
                                                                      with = FALSE, by=c("sender","date","tranche")];
    
    ##Remove duplicated fields
    LVTSTXNsDataSeriesSentRecieved[ ,c("senderID","senderFullName"):=NULL];
    setnames(LVTSTXNsDataSeriesSentRecieved, "receiverID", "senderID");
    setnames(LVTSTXNsDataSeriesSentRecieved, "receiverFullName", "senderFullName");
    

  }else{
    
    LVTSTXNsDataSeriesBySender  <-  mergedLVTSTXNsYearsSpecYM[, list(SentVolume=sum(TotalVolume),RecievedVolume=sum(TotalVolume),
                                                                       TotalVolume=sum(TotalVolume),
                                                                       SentValue=sum(TotalValue), RecievedValue=sum(TotalValue),
                                                                       TotalValue=sum(TotalValue)
    ), 
    by=c("sender","senderFullName","date")];
    
    LVTSTXNsDataSeriesBySender <-   LVTSTXNsDataSeriesBySender[order(date, sender), ];
    
    LVTSTXNsDataSeriesByPeriod  <-  mergedLVTSTXNsYearsSpecYM[, list(SentVolume=sum(TotalVolume),RecievedVolume=sum(TotalVolume),
                                                                       TotalVolume=sum(TotalVolume),
                                                                       SentValue=sum(TotalValue), RecievedValue=sum(TotalValue),
                                                                       TotalValue=sum(TotalValue)
    ),
    by=c("date")];
    LVTSTXNsDataSeriesByPeriod <-   LVTSTXNsDataSeriesByPeriod[order(date), ];
    
    
    LVTSTXNsDataSeriesBySenderReceiver  <-  mergedLVTSTXNsYearsSpecYM[, list(SentVolume=sum(TotalVolume),RecievedVolume=sum(TotalVolume),
                                                                               TotalVolume=sum(TotalVolume),
                                                                               SentValue=sum(TotalValue), RecievedValue=sum(TotalValue),
                                                                               TotalValue=sum(TotalValue)
    ), 
    by=c("date","sender","senderID","senderFullName","receiver","receiverID","receiverFullName")];
    
    
    
    LVTSTXNsDataSeriesSent  <-  mergedLVTSTXNsYearsSpecYM[, list(SentVolume=sum(TotalVolume), SentValue=sum(TotalValue)
    ), 
    by=c("sender","senderID","senderFullName","date")];
    
    LVTSTXNsDataSeriesRecieved  <-  mergedLVTSTXNsYearsSpecYM[, list(RecievedVolume=sum(TotalVolume), RecievedValue=sum(TotalValue)
    ), 
    by=c("receiver","receiverID","receiverFullName","date")];
    
    
    LVTSTXNsDataSeriesSent_df <-  as.data.frame(LVTSTXNsDataSeriesSent);
    LVTSTXNsDataSeriesRecieved_df <-  as.data.frame(LVTSTXNsDataSeriesRecieved);
#    LVTSTXNsDataSeriesSentRecieved_df  <-  merge(LVTSTXNsDataSeriesSent_df, LVTSTXNsDataSeriesRecieved_df, by.x=c("sender","date"), 
#                                                  by.y=c("receiver","date"),all.x = TRUE, all.y = TRUE);
    
    
    LVTSTXNsDataSeriesSentRecieved_df  <-  merge(LVTSTXNsDataSeriesRecieved_df, LVTSTXNsDataSeriesSent_df, by.x=c("receiver","date"),
                                                 by.y=c("sender","date"), all.x = TRUE, all.y = TRUE);
    
    LVTSTXNsDataSeriesSentRecieved <-  as.data.table(LVTSTXNsDataSeriesSentRecieved_df);
    LVTSTXNsDataSeriesSentRecieved[is.na(LVTSTXNsDataSeriesSentRecieved)] = 0;
    setnames(LVTSTXNsDataSeriesSentRecieved, "receiver", "sender");
    LVTSTXNsDataSeriesSentRecieved<- LVTSTXNsDataSeriesSentRecieved[, c("TotalVolume") := sum(SentVolume, RecievedVolume), 
                                                                      with = FALSE, by=c("sender","date")];
    LVTSTXNsDataSeriesSentRecieved<- LVTSTXNsDataSeriesSentRecieved[, c("TotalValue") := sum(SentValue, RecievedValue), 
                                                                      with = FALSE, by=c("sender","date")];
    
    ##Remove duplicated fields
    LVTSTXNsDataSeriesSentRecieved[ ,c("senderID","senderFullName"):=NULL];
    setnames(LVTSTXNsDataSeriesSentRecieved, "receiverID", "senderID");
    setnames(LVTSTXNsDataSeriesSentRecieved, "receiverFullName", "senderFullName");
    
  }
  #order by date/time then sender then reciever
  LVTSTXNsDataSeriesBySenderReceiver <- LVTSTXNsDataSeriesBySenderReceiver[order(date, sender, receiver), ];
  
  
  #Remove temporary working datasets from memory
  rm(LVTSTXNsDataSeriesSent_df);
  rm(LVTSTXNsDataSeriesRecieved_df);
  rm(LVTSTXNsDataSeriesSentRecieved_df);
  
  }else if(dataFrequency == "dateTime"){
    #clear memory space
    print(paste("start time: ",now()));
    gc();
    #Collate the TotalValue and TotalVolume sent and recieved. Unlike other instances, this has been completed first to ensure all data 
    #fields are maintained after applying the data.frame aggregation to collate the data by date and time intervals ,"tranche"
    
    
    #If aggregation should be at tranche level or overall payment
    if(byTranche){
      mergedLVTSTXNsYearsSpecYM<-  mergedLVTSTXNsYearsSpecYM[, list(SentVolume=sum(TotalVolume),RecievedVolume=sum(TotalVolume),
                                                                      TotalVolume=sum(TotalVolume),
                                                                      SentValue=sum(TotalValue), RecievedValue=sum(TotalValue),
                                                                      TotalValue=sum(TotalValue)
      ), 
      by=c("date.time","sender","senderID","senderFullName","receiver","receiverID","receiverFullName","tranche")];
      
      
      #clear memory space since the next processes are resource intensive
      gc();
      
      #convert working data.table object mergedLVTSTXNsYearsSpecYM to a temporary data.frame object for manipulation and calculations 
      mergedLVTSTXNsYearsSpecYM_df <- as.data.frame(mergedLVTSTXNsYearsSpecYM);
      #aggregate the SentVolume, RecievedVolume, TotalVolume, SentValue, RecievedValue, TotalValue fields by date and time intervals
      #Using cumsum to compute the cumulative sum over the dateTimeNettingFrequency. The use of the sum raised some questions on the
      #final value of the summations with respect to determining multilateral net debit positions.
      mergedLVTSTXNsYearsSpecYM_df <- aggregate(cbind(SentVolume, RecievedVolume, TotalVolume, SentValue, RecievedValue, TotalValue) ~ 
                                                   sender + senderID + senderFullName + receiver + receiverID + receiverFullName + tranche
                                                 + cut(mergedLVTSTXNsYearsSpecYM_df$date.time, dateTimeNettingFrequency), 
                                                 mergedLVTSTXNsYearsSpecYM_df[setdiff(names(mergedLVTSTXNsYearsSpecYM_df),c("date.time"))], sum);
      
      #clear memory space
      gc();
      
      
    }else{
      mergedLVTSTXNsYearsSpecYM<-  mergedLVTSTXNsYearsSpecYM[, list(SentVolume=sum(TotalVolume),RecievedVolume=sum(TotalVolume),
                                                                      TotalVolume=sum(TotalVolume),
                                                                      SentValue=sum(TotalValue), RecievedValue=sum(TotalValue),
                                                                      TotalValue=sum(TotalValue)
      ), 
      by=c("date.time","sender","senderID","senderFullName","receiver","receiverID","receiverFullName")];
      
      #clear memory space since the next processes are resource intensive
      gc();
      
      #convert working data.table object mergedLVTSTXNsYearsSpecYM to a temporary data.frame object for manipulation and calculations 
      mergedLVTSTXNsYearsSpecYM_df <- as.data.frame(mergedLVTSTXNsYearsSpecYM);
      #aggregate the SentVolume, RecievedVolume, TotalVolume, SentValue, RecievedValue, TotalValue fields by date and time intervals
      gc();
      gc();
      rm(mergedLVTSTXNsYearsSpecYM);
      gc();
      gc();
      mergedLVTSTXNsYearsSpecYM_df <- aggregate(cbind(SentVolume, RecievedVolume, TotalVolume, SentValue, RecievedValue, TotalValue) ~ 
                                                   sender + senderID + senderFullName + receiver + receiverID + receiverFullName 
                                                 + cut(mergedLVTSTXNsYearsSpecYM_df$date.time, dateTimeNettingFrequency), 
                                                 mergedLVTSTXNsYearsSpecYM_df[setdiff(names(mergedLVTSTXNsYearsSpecYM_df),"date.time")], sum);
      
      #clear memory space
      gc();
      gc();
      
    }
  
    print(paste("end time: ",now()));
  
    #Convert the temporary data.frame with the data collated by date and time intervals to a data.table object by replacing the main working data set
    #mergedLVTSTXNsYearsSpecYM
    mergedLVTSTXNsYearsSpecYM <- as.data.table(mergedLVTSTXNsYearsSpecYM_df);
    #remove the temporary dfata.frame object to free up memmory
    rm(mergedLVTSTXNsYearsSpecYM_df);
    #clear memory space
    gc();
  
    #Rename the new date and time field because the aggregation function applied to the temporary data.frame object
    #creates a new flied/column called "cut(mergedLVTSTXNsYearsSpecYM_df$date.time, dateTimeNettingFrequency)"
    #
    #The trouble with changing column names of a data.frame is that, almost unbelievably, the entire data.frame is copied. 
    #Even when it's in .GlobalEnv and no other variable points to it.
    #The data.table package has a setnames() function which changes column names by reference without copying the whole dataset. 
    #data.table is different in that it doesn't copy-on-write, which can be very important for large datasets.
    setnames(mergedLVTSTXNsYearsSpecYM, "cut(mergedLVTSTXNsYearsSpecYM_df$date.time, dateTimeNettingFrequency)", "date.time");
  
    #If aggregation should be at tranche level or overall payment
    if(byTranche){
      
      #collate other data agregations as with other data frequencies
      LVTSTXNsDataSeriesBySender  <-  mergedLVTSTXNsYearsSpecYM[, list(SentVolume=sum(TotalVolume),RecievedVolume=sum(TotalVolume),
                                                                         TotalVolume=sum(TotalVolume),
                                                                         SentValue=sum(TotalValue), RecievedValue=sum(TotalValue),
                                                                         TotalValue=sum(TotalValue)
      ), 
      by=c("sender","senderFullName","date.time","tranche")];
      
      LVTSTXNsDataSeriesBySender <-   LVTSTXNsDataSeriesBySender[order(date.time, sender), ];
      
      LVTSTXNsDataSeriesByPeriod  <-  mergedLVTSTXNsYearsSpecYM[, list(SentVolume=sum(TotalVolume),RecievedVolume=sum(TotalVolume),
                                                                         TotalVolume=sum(TotalVolume),
                                                                         SentValue=sum(TotalValue), RecievedValue=sum(TotalValue),
                                                                         TotalValue=sum(TotalValue)
      ), 
      by=c("date.time","tranche")];
      
      
      LVTSTXNsDataSeriesByPeriod <-   LVTSTXNsDataSeriesByPeriod[order(date.time), ];
      
      
      
      LVTSTXNsDataSeriesBySenderReceiver  <-  mergedLVTSTXNsYearsSpecYM[, list(SentVolume=sum(TotalVolume),RecievedVolume=sum(TotalVolume),
                                                                                 TotalVolume=sum(TotalVolume),
                                                                                 SentValue=sum(TotalValue), RecievedValue=sum(TotalValue),
                                                                                 TotalValue=sum(TotalValue)
      ), 
      by=c("date.time","sender","senderID","senderFullName","receiver",
           "receiverID","receiverFullName","tranche")];

      #ensure the tranche field is a numeric and not a factor
      LVTSTXNsDataSeriesBySenderReceiver$tranche <- as.numeric(LVTSTXNsDataSeriesBySenderReceiver$tranche);

      LVTSTXNsDataSeriesSent  <-  mergedLVTSTXNsYearsSpecYM[, list(SentVolume=sum(TotalVolume), SentValue=sum(TotalValue)
      ), 
      by=c("sender","senderID","senderFullName","date.time","tranche")];
      
      LVTSTXNsDataSeriesRecieved  <-  mergedLVTSTXNsYearsSpecYM[, list(RecievedVolume=sum(TotalVolume), RecievedValue=sum(TotalValue)
      ), 
      by=c("receiver","receiverID","receiverFullName","date.time","tranche")];
      
      
      LVTSTXNsDataSeriesSent_df <-  as.data.frame(LVTSTXNsDataSeriesSent);
      LVTSTXNsDataSeriesRecieved_df <-  as.data.frame(LVTSTXNsDataSeriesRecieved);
#      LVTSTXNsDataSeriesSentRecieved_df  <-  merge(LVTSTXNsDataSeriesSent_df, LVTSTXNsDataSeriesRecieved_df, by.x=c("sender","date.time","tranche"), 
#                                                    by.y=c("receiver","date.time","tranche"),all.x = TRUE, all.y = TRUE);
      
      
      LVTSTXNsDataSeriesSentRecieved_df  <-  merge(LVTSTXNsDataSeriesRecieved_df, LVTSTXNsDataSeriesSent_df, by.x=c("receiver","date.time", "tranche"),
                                                   by.y=c("sender","date.time", "tranche"), all.x = TRUE, all.y = TRUE);
      
      
      LVTSTXNsDataSeriesSentRecieved <-  as.data.table(LVTSTXNsDataSeriesSentRecieved_df);
      ##Add the total value and total volume columns/fields
      LVTSTXNsDataSeriesSentRecieved[is.na(LVTSTXNsDataSeriesSentRecieved)] = 0;
      setnames(LVTSTXNsDataSeriesSentRecieved, "receiver", "sender");
      LVTSTXNsDataSeriesSentRecieved<- LVTSTXNsDataSeriesSentRecieved[, c("TotalVolume") := sum(SentVolume, RecievedVolume), 
                                                                        with = FALSE, by=c("sender","date.time","tranche")];
      LVTSTXNsDataSeriesSentRecieved<- LVTSTXNsDataSeriesSentRecieved[, c("TotalValue") := sum(SentValue, RecievedValue), 
                                                                        with = FALSE, by=c("sender","date.time","tranche")];
      
      ##Remove duplicated fields
      LVTSTXNsDataSeriesSentRecieved[ ,c("senderID","senderFullName"):=NULL];
      setnames(LVTSTXNsDataSeriesSentRecieved, "receiverID", "senderID");
      setnames(LVTSTXNsDataSeriesSentRecieved, "receiverFullName", "senderFullName");

    }else{
      
      #collate other data agregations as with other data frequencies
      LVTSTXNsDataSeriesBySender  <-  mergedLVTSTXNsYearsSpecYM[, list(SentVolume=sum(TotalVolume),RecievedVolume=sum(TotalVolume),
                                                                         TotalVolume=sum(TotalVolume),
                                                                         SentValue=sum(TotalValue), RecievedValue=sum(TotalValue),
                                                                         TotalValue=sum(TotalValue)
      ), 
      by=c("sender","senderFullName","date.time")];
      
      LVTSTXNsDataSeriesBySender <-   LVTSTXNsDataSeriesBySender[order(date.time, sender), ];
      
      LVTSTXNsDataSeriesByPeriod  <-  mergedLVTSTXNsYearsSpecYM[, list(SentVolume=sum(TotalVolume),RecievedVolume=sum(TotalVolume),
                                                                         TotalVolume=sum(TotalVolume),
                                                                         SentValue=sum(TotalValue), RecievedValue=sum(TotalValue),
                                                                         TotalValue=sum(TotalValue)
      ), 
      by=c("date.time")];
      
      
      LVTSTXNsDataSeriesByPeriod <-   LVTSTXNsDataSeriesByPeriod[order(date.time), ];
      
      
      
      LVTSTXNsDataSeriesBySenderReceiver  <-  mergedLVTSTXNsYearsSpecYM[, list(SentVolume=sum(TotalVolume),RecievedVolume=sum(TotalVolume),
                                                                                 TotalVolume=sum(TotalVolume),
                                                                                 SentValue=sum(TotalValue), RecievedValue=sum(TotalValue),
                                                                                 TotalValue=sum(TotalValue)
      ), 
      by=c("date.time","sender","senderID","senderFullName","receiver",
           "receiverID","receiverFullName")];
      
      LVTSTXNsDataSeriesSent  <-  mergedLVTSTXNsYearsSpecYM[, list(SentVolume=sum(TotalVolume), SentValue=sum(TotalValue)
      ), 
      by=c("sender","senderID","senderFullName","date.time")];
      
      LVTSTXNsDataSeriesRecieved  <-  mergedLVTSTXNsYearsSpecYM[, list(RecievedVolume=sum(TotalVolume), RecievedValue=sum(TotalValue)
      ), 
      by=c("receiver","receiverID","receiverFullName","date.time")];
      
      
      LVTSTXNsDataSeriesSent_df <-  as.data.frame(LVTSTXNsDataSeriesSent);
      LVTSTXNsDataSeriesRecieved_df <-  as.data.frame(LVTSTXNsDataSeriesRecieved);
#      LVTSTXNsDataSeriesSentRecieved_df  <-  merge(LVTSTXNsDataSeriesSent_df, LVTSTXNsDataSeriesRecieved_df, by.x=c("sender","date.time"), 
#                                                    by.y=c("receiver","date.time"),all.x = TRUE, all.y = TRUE);
      
      LVTSTXNsDataSeriesSentRecieved_df  <-  merge(LVTSTXNsDataSeriesRecieved_df, LVTSTXNsDataSeriesSent_df, by.x=c("receiver","date.time"),
                                                   by.y=c("sender","date.time"), all.x = TRUE, all.y = TRUE);
      
      LVTSTXNsDataSeriesSentRecieved <-  as.data.table(LVTSTXNsDataSeriesSentRecieved_df);
      LVTSTXNsDataSeriesSentRecieved[is.na(LVTSTXNsDataSeriesSentRecieved)] = 0;
      setnames(LVTSTXNsDataSeriesSentRecieved, "receiver", "sender");
      LVTSTXNsDataSeriesSentRecieved<- LVTSTXNsDataSeriesSentRecieved[, c("TotalVolume") := sum(SentVolume, RecievedVolume), 
                                                                        with = FALSE, by=c("sender","date.time")];
      LVTSTXNsDataSeriesSentRecieved<- LVTSTXNsDataSeriesSentRecieved[, c("TotalValue") := sum(SentValue, RecievedValue), 
                                                                        with = FALSE, by=c("sender","date.time")];
      
      ##Remove duplicated fields
      LVTSTXNsDataSeriesSentRecieved[ ,c("senderID","senderFullName"):=NULL];
      setnames(LVTSTXNsDataSeriesSentRecieved, "receiverID", "senderID");
      setnames(LVTSTXNsDataSeriesSentRecieved, "receiverFullName", "senderFullName");
    }
    #order by date/time then sender then reciever
    LVTSTXNsDataSeriesBySenderReceiver <-   LVTSTXNsDataSeriesBySenderReceiver[order(date.time, sender, receiver), ];

    
    
    #Remove temporary working datasets from memory
    rm(LVTSTXNsDataSeriesSent_df);
    rm(LVTSTXNsDataSeriesRecieved_df);
    rm(LVTSTXNsDataSeriesSentRecieved_df);
    
    }#
gc();

LVTSTXNsDataSeries <- LVTSTXNsDataSeriesBySenderReceiver;

save(LVTSTXNsDataSeries,file=LVTSTXNsDataSeriesSavePath);
save(LVTSTXNsDataSeriesSentRecieved, file=LVTSTXNsDataSeriesSentRecievedSavePath);

#save data sets per tranche If aggregation has been set to tranche level
if(byTranche){
  
  #extract the tranche 1 only items
  LVTSTXNsDataSeriesTranche1 <- LVTSTXNsDataSeries[tranche==1,];
  
  #extract the tranche 2 only items
  LVTSTXNsDataSeriesTranche2 <- LVTSTXNsDataSeries[tranche==2,];

   #extract the tranche 1 only items
  save(LVTSTXNsDataSeriesTranche1,file=LVTSTXNsDataSeriesTranche1SavePath);
  #extract the tranche 2 only items
  save(LVTSTXNsDataSeriesTranche2,file=LVTSTXNsDataSeriesTranche2SavePath);
}

##write data into STATA readable files
write.dta(LVTSTXNsDataSeries,file=LVTSTXNsDataSeriesSTATASavePath);
write.dta(LVTSTXNsDataSeriesBySender,file=LVTSTXNsDataSeriesBySenderSTATASavePath);
#write.dta(LVTSTXNsDataSeriesByPeriod, file=LVTSTXNsDataSeriesByPeriodSTATASavePath);
write.dta(LVTSTXNsDataSeriesSentRecieved, file=LVTSTXNsDataSeriesSentRecievedSTATASavePath);
if(dataFrequency!="dateTime"){
  write.csv2(LVTSTXNsDataSeriesSentRecieved, file=LVTSTXNsDataSeriesSentRecievedXLSSavePath);
}else{
  write.csv2(LVTSTXNsDataSeriesSentRecieved, file=LVTSTXNsDataSeriesSentRecievedXLSSavePath);
}

gc();
