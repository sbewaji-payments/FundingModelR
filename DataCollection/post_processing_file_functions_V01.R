####
##This script houses the functions used in the post processing files






#post_processing_file_functions_V01.R





##<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<DECLARE METHODS/FUNCTIONS>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

##This function is used to calculate the shares
fnShare <- function(x) x/sum(x);


##This function has been created to merge ING and Scotia data. However, it is optimised to marge any two FIs data.
##NOTE: This implementation assumes that it is the working raw data file that is passed once level 1 post processing is complete.
##Any other file will not be processed and a new implementation will be required for that input data.table object
##The implementation requires that the pseudo BIC code, CPA FI ID and Full names of FIs are all known
##The function returns a data.table object
mergeTwoFIs <- function(inputDataTable, aquiringFIName, aquiringFICode, aquiringFIFullName, aquiredFIName, dataFreq){
  dtDataMergedFIs <- inputDataTable;
  dtDataMergedFIs <- as.data.table(dtDataMergedFIs);#force dtDataMergedFIs to be a data.table object
  inputColNames <- colnames(dtDataMergedFIs);
  
  #This if statement will need to be optimised to make it more elegant
  if("Year_Month" %in% inputColNames | "Year_Quarter" %in% inputColNames | "Year" %in% inputColNames 
     | "date" %in% inputColNames | "date.time" %in% inputColNames
     & "sender" %in% inputColNames & "senderID" %in% inputColNames & "receiver" %in% inputColNames & "receiverID" %in% inputColNames
     & "senderFullName" %in% inputColNames & "receiverFullName" %in% inputColNames){
    
    #the following lines of code change all instances/occurances (BIC/Full Name/ ID) of the aquired FI in the input data.table to that of the 
    #aquiring FI
    #apply where aquired FI is the reciever of payments
    dtDataMergedFIs <- dtDataMergedFIs[, receiverID := as.character(receiverID)][receiver == aquiredFIName, receiverID := aquiringFICode];
    dtDataMergedFIs <- dtDataMergedFIs[, receiverFullName := as.character(receiverFullName)][receiver == aquiredFIName, receiverFullName := aquiringFIFullName];
    dtDataMergedFIs <- dtDataMergedFIs[, receiver := as.character(receiver)][receiver == aquiredFIName, receiver := aquiringFIName];
    
    #apply where aquired FI is the sender of payments
    dtDataMergedFIs <- dtDataMergedFIs[, senderID := as.character(senderID)][sender == aquiredFIName, senderID := aquiringFICode];
    dtDataMergedFIs <- dtDataMergedFIs[, senderFullName := as.character(senderFullName)][sender == aquiredFIName, senderFullName := aquiringFIFullName];
    dtDataMergedFIs <- dtDataMergedFIs[, sender := as.character(sender)][sender == aquiredFIName, sender := aquiringFIName];
    
    #if monthly  
    if(dataFreq == "monthly"){
      #sum the data for the FIs being merged
      #If aggregation should be at tranche level or overall payment
      if(byTranche){
        dtDataMergedFIs <- dtDataMergedFIs[, TotalVolume := as.numeric(TotalVolume)][sender == aquiringFIName,`:=`(TotalVolume = sum(TotalVolume), 
                                                                                                                   TotalValue=sum(TotalValue)), 
                                                                                     by=c("Year_Month","sender","senderID","receiver","receiverID",
                                                                                          "senderFullName", "receiverFullName","tranche")];
      }else{
        dtDataMergedFIs <- dtDataMergedFIs[, TotalVolume := as.numeric(TotalVolume)][sender == aquiringFIName,`:=`(TotalVolume = sum(TotalVolume), 
                                                                                                                   TotalValue=sum(TotalValue)), 
                                                                                     by=c("Year_Month","sender","senderID","receiver","receiverID",
                                                                                          "senderFullName", "receiverFullName")];
      }
      
      dtDataMergedFIs <- dtDataMergedFIs[order(sender, receiver,Year_Month)];
      #if quarterly  
    }else if(dataFreq == "quarterly"){
      #sum the data for the FIs being merged
      #If aggregation should be at tranche level or overall payment
      if(byTranche){
        dtDataMergedFIs <- dtDataMergedFIs[, TotalVolume := as.numeric(TotalVolume)][sender == aquiringFIName,`:=`(TotalVolume = sum(TotalVolume), 
                                                                                                                   TotalValue=sum(TotalValue)), 
                                                                                     by=c("Year_Quarter","sender","senderID","receiver","receiverID",
                                                                                          "senderFullName", "receiverFullName","tranche")];
      }else{
        dtDataMergedFIs <- dtDataMergedFIs[, TotalVolume := as.numeric(TotalVolume)][sender == aquiringFIName,`:=`(TotalVolume = sum(TotalVolume), 
                                                                                                                   TotalValue=sum(TotalValue)), 
                                                                                     by=c("Year_Quarter","sender","senderID","receiver","receiverID",
                                                                                          "senderFullName", "receiverFullName")];
      }
      
      dtDataMergedFIs <- dtDataMergedFIs[order(sender, receiver,Year_Quarter)];
      #if annual 
    }else if(dataFreq == "annual"){
      #sum the data for the FIs being merged
      #If aggregation should be at tranche level or overall payment
      if(byTranche){
        dtDataMergedFIs <- dtDataMergedFIs[, TotalVolume := as.numeric(TotalVolume)][sender == aquiringFIName,`:=`(TotalVolume = sum(TotalVolume), 
                                                                                                                   TotalValue=sum(TotalValue)), 
                                                                                     by=c("Year","sender","senderID","receiver","receiverID",
                                                                                          "senderFullName", "receiverFullName","tranche")];
      }else{
        dtDataMergedFIs <- dtDataMergedFIs[, TotalVolume := as.numeric(TotalVolume)][sender == aquiringFIName,`:=`(TotalVolume = sum(TotalVolume), 
                                                                                                                   TotalValue=sum(TotalValue)), 
                                                                                     by=c("Year","sender","senderID","receiver","receiverID",
                                                                                          "senderFullName", "receiverFullName")];
      }
      dtDataMergedFIs <- dtDataMergedFIs[order(sender, receiver,Year)];
      #if daily   
    }else if(dataFreq == "daily"){
      #sum the data for the FIs being merged
      #If aggregation should be at tranche level or overall payment
      if(byTranche){
        dtDataMergedFIs <- dtDataMergedFIs[, TotalVolume := as.numeric(TotalVolume)][sender == aquiringFIName,`:=`(TotalVolume = sum(TotalVolume), 
                                                                                                                   TotalValue=sum(TotalValue)), 
                                                                                     by=c("date","sender","senderID","receiver","receiverID",
                                                                                          "senderFullName", "receiverFullName","tranche")];
      }else{
        dtDataMergedFIs <- dtDataMergedFIs[, TotalVolume := as.numeric(TotalVolume)][sender == aquiringFIName,`:=`(TotalVolume = sum(TotalVolume), 
                                                                                                                   TotalValue=sum(TotalValue)), 
                                                                                     by=c("date","sender","senderID","receiver","receiverID",
                                                                                          "senderFullName", "receiverFullName")];
      }
      dtDataMergedFIs <- dtDataMergedFIs[order(sender, receiver,date)];
    }else if(dataFreq == "dateTime"){
      #sum the data for the FIs being merged
      #If aggregation should be at tranche level or overall payment
      if(byTranche){
        dtDataMergedFIs <- dtDataMergedFIs[, TotalVolume := as.numeric(TotalVolume)][sender == aquiringFIName,`:=`(TotalVolume = sum(TotalVolume), 
                                                                                                                   TotalValue=sum(TotalValue)), 
                                                                                     by=c("date.time","sender","senderID","receiver","receiverID",
                                                                                          "senderFullName", "receiverFullName","tranche")];
      }else{
        dtDataMergedFIs <- dtDataMergedFIs[, TotalVolume := as.numeric(TotalVolume)][sender == aquiringFIName,`:=`(TotalVolume = sum(TotalVolume), 
                                                                                                                   TotalValue=sum(TotalValue)), 
                                                                                     by=c("date.time","sender","senderID","receiver","receiverID",
                                                                                          "senderFullName", "receiverFullName")];
      }
      dtDataMergedFIs <- dtDataMergedFIs[order(sender, receiver,date.time)];
    }
  }else{#error handling approximation to try-catch in java
    print("The input data table does not cotain all the required columns. Check that the correct data is being passed");
  }
  return(dtDataMergedFIs);
}



mergeBCLsForTwoFIs <- function(inputDataTable, aquiringFIName, aquiringFICode, aquiringFIFullName, aquiredFIName, dataFreq){
  dtDataMergedFIs <- inputDataTable;
  dtDataMergedFIs <- as.data.table(dtDataMergedFIs);#force dtDataMergedFIs to be a data.table object
  inputColNames <- colnames(dtDataMergedFIs);
  
  #This if statement will need to be optimised to make it more elegant
  if("Year_Month" %in% inputColNames | "Year_Quarter" %in% inputColNames | "Year" %in% inputColNames 
     | "date" %in% inputColNames | "date.time" %in% inputColNames
     & "grantor" %in% inputColNames & "grantorID" %in% inputColNames & "grantee" %in% inputColNames & "granteeID" %in% inputColNames
     & "grantorFullName" %in% inputColNames & "granteeFullName" %in% inputColNames){
    
    #the following lines of code change all instances/occurances (BIC/Full Name/ ID) of the aquired FI in the input data.table to that of the 
    #aquiring FI
    #apply where aquired FI is the reciever of payments
    dtDataMergedFIs <- dtDataMergedFIs[, granteeID := as.character(granteeID)][grantee == aquiredFIName, granteeID := aquiringFICode];
    dtDataMergedFIs <- dtDataMergedFIs[, granteeFullName := as.character(granteeFullName)][grantee == aquiredFIName, granteeFullName := aquiringFIFullName];
    dtDataMergedFIs <- dtDataMergedFIs[, grantee := as.character(grantee)][grantee == aquiredFIName, grantee := aquiringFIName];
    
    #apply where aquired FI is the grantor of payments
    dtDataMergedFIs <- dtDataMergedFIs[, grantorID := as.character(grantorID)][grantor == aquiredFIName, grantorID := aquiringFICode];
    dtDataMergedFIs <- dtDataMergedFIs[, grantorFullName := as.character(grantorFullName)][grantor == aquiredFIName, grantorFullName := aquiringFIFullName];
    dtDataMergedFIs <- dtDataMergedFIs[, grantor := as.character(grantor)][grantor == aquiredFIName, grantor := aquiringFIName];
    
    #if monthly  
    if(dataFreq == "monthly"){
      #sum the data for the FIs being merged
      dtDataMergedFIs <- dtDataMergedFIs[, TotalVolume := as.numeric(TotalVolume)][grantor == aquiringFIName,`:=`(TotalVolume = sum(TotalVolume), 
                                                                                                                  TotalValue=sum(TotalValue)), 
                                                                                   by=c("Year_Month","grantor","grantorID","grantee","granteeID",
                                                                                        "grantorFullName", "granteeFullName")];
      dtDataMergedFIs <- dtDataMergedFIs[order(grantor, grantee,Year_Month)];
      #if quarterly  
    }else if(dataFreq == "quarterly"){
      #sum the data for the FIs being merged
      dtDataMergedFIs <- dtDataMergedFIs[, TotalVolume := as.numeric(TotalVolume)][grantor == aquiringFIName,`:=`(TotalVolume = sum(TotalVolume), 
                                                                                                                  TotalValue=sum(TotalValue)), 
                                                                                   by=c("Year_Quarter","grantor","grantorID","grantee","granteeID",
                                                                                        "grantorFullName", "granteeFullName")];
      dtDataMergedFIs <- dtDataMergedFIs[order(grantor, grantee,Year_Quarter)];
      #if annual 
    }else if(dataFreq == "annual"){
      #sum the data for the FIs being merged
      dtDataMergedFIs <- dtDataMergedFIs[, TotalVolume := as.numeric(TotalVolume)][grantor == aquiringFIName,`:=`(TotalVolume = sum(TotalVolume), 
                                                                                                                  TotalValue=sum(TotalValue)), 
                                                                                   by=c("Year","grantor","grantorID","grantee","granteeID",
                                                                                        "grantorFullName", "granteeFullName")];
      dtDataMergedFIs <- dtDataMergedFIs[order(grantor, grantee,Year)];
      #if daily   
    }else if(dataFreq == "daily"){
      #sum the data for the FIs being merged
      dtDataMergedFIs <- dtDataMergedFIs[, TotalVolume := as.numeric(TotalVolume)][grantor == aquiringFIName,`:=`(TotalVolume = sum(TotalVolume), 
                                                                                                                  TotalValue=sum(TotalValue)), 
                                                                                   by=c("date","grantor","grantorID","grantee","granteeID",
                                                                                        "grantorFullName", "granteeFullName")];
      dtDataMergedFIs <- dtDataMergedFIs[order(grantor, grantee,date)];
    }else if(dataFreq == "dateTime"){
      #sum the data for the FIs being merged
      dtDataMergedFIs <- dtDataMergedFIs[, TotalVolume := as.numeric(TotalVolume)][grantor == aquiringFIName,`:=`(TotalVolume = sum(TotalVolume), 
                                                                                                                  TotalValue=sum(TotalValue)), 
                                                                                   by=c("date.time","grantor","grantorID","grantee","granteeID",
                                                                                        "grantorFullName", "granteeFullName")];
      dtDataMergedFIs <- dtDataMergedFIs[order(grantor, grantee,date.time)];
    }
  }else{#error handling approximation to try-catch in java
    print("The input data table does not cotain all the required columns. Check that the correct data is being passed");
  }
  return(dtDataMergedFIs);
}



