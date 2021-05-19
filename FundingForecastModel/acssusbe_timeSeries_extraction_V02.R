
##This script is used to collect the FI data generated in the usbe_post_processing_file_Level_2_V##.R file.
##The generated file will be used to provide finance with the sumarry details for each FI's sending an recieving habits
##
##Final input data tables that the code will work with are 
##        a: ACSSUSBETXNsDataSeriesSentRecieved - The ACSS and USBE data file
##        b: 
##
##
##Final outut is a list containing the transaction volume/value time series data 
##        a: ACSSUSBEFILevelTransDataSeriesList - The list containing ACSS and USBE time series data for each FI. Each element of the list is a time series object for the FI
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


##<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<START DATA/STATISTICAL ANALYSIS>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
##<<<<<<<<<<<<<<<<<<<<<<<<<LOAD DATA>>>>>>>>>>>>>>>>>>>>>>>>
##Now load the individual FI's complete data tables
print("Loading data table(s)");#used to record speed of computations
print(now());#used to record speed of computations
load(extractedACSSUSBEDataLoadPath);
gc();
print("data table(s) loaded");#used to record speed of computations
print(now());#used to record speed of computations
##Ensure the imported dataset is in the form of a data.table object as all proceeding code relies on the dataset being a data.table object
ACSSUSBETXNsDataSeriesSentRecieved <- as.data.table(ACSSUSBETXNsDataSeriesSentRecieved);
gc();



if(dataFrequency == "monthly"){
  ACSSUSBETXNsDataSeriesSentRecieved$Year_Month <- as.yearmon(ACSSUSBETXNsDataSeriesSentRecieved$Year_Month, '%YM%m');
  ACSSUSBETXNsDataSeriesSentRecieved <- ACSSUSBETXNsDataSeriesSentRecieved[order(Year_Month),];
  #if quarterly  
}else if(dataFrequency == "quarterly"){
  ACSSUSBETXNsDataSeriesSentRecieved$Year_Quarter <- as.yearqtr(ACSSUSBETXNsDataSeriesSentRecieved$Year_Quarter);
  ACSSUSBETXNsDataSeriesSentRecieved <- ACSSUSBETXNsDataSeriesSentRecieved[order(Year_Quarter),];
  #if annual 
}else if(dataFrequency == "annual"){
  ACSSUSBETXNsDataSeriesSentRecieved <- ACSSUSBETXNsDataSeriesSentRecieved[order(Year),];
  #if daily   
}else if(dataFrequency == "daily"){
  ACSSUSBETXNsDataSeriesSentRecieved <- ACSSUSBETXNsDataSeriesSentRecieved[order(date),];
}

vectorColumnNames <- getColumnNames(ACSSUSBETXNsDataSeriesSentRecieved);
vectorFINames <- getListOfFINames(ACSSUSBETXNsDataSeriesSentRecieved, vectorColumnNames, "sender");
vectorOfTimeSeriesDates <- getListOfTImeSeriesDates(ACSSUSBETXNsDataSeriesSentRecieved, vectorColumnNames);
vectorOfTimeSeriesDates <- as.yearqtr(vectorOfTimeSeriesDates);


print("creating timeSeries Tables/Collection");
print(now());#used to record speed of computations

ACSSUSBEFITimeSeriesDataTable <- 
  getFITimeSeriesDataTable(as.data.table(ACSSUSBETXNsDataSeriesSentRecieved),vectorColumnNames, dataFrequencyColumnName,
                           "Volume", "sender",minObservations);
ACSSUSBEFITimeSeriesDataTable <- list.names(ACSSUSBEFITimeSeriesDataTable,"ACSSUSBEFITimeSeriesList");
list.save(ACSSUSBEFITimeSeriesDataTable, ACSSUSBEFITimeSeriesDataTableSavePath);


if(dataFrequency == "monthly"){

  m112001MissingDataDummyTS <- createDummyVarriable(ACSSUSBETXNsDataSeriesSentRecieved, 
                                                   vectorColumnNames, dataFrequencyColumnName, "missingData", "2001M11");
  m12006MissingDataDummyTS <- createDummyVarriable(ACSSUSBETXNsDataSeriesSentRecieved, 
                                                   vectorColumnNames, dataFrequencyColumnName, "missingData", "2006M01");
  m102006MissingDataDummyTS <- createDummyVarriable(ACSSUSBETXNsDataSeriesSentRecieved, 
                                                   vectorColumnNames, dataFrequencyColumnName, "missingData", "2006M10");
  m112007MissingDataDummyTS <- createDummyVarriable(ACSSUSBETXNsDataSeriesSentRecieved, 
                                                   vectorColumnNames, dataFrequencyColumnName, "missingData", "2007M11");
  m122007MissingDataDummyTS <- createDummyVarriable(ACSSUSBETXNsDataSeriesSentRecieved, 
                                                   vectorColumnNames, dataFrequencyColumnName, "missingData", "2007M12");
  
  usbeMonthlyMissingDataDummyTS <- (m112001MissingDataDummyTS + m12006MissingDataDummyTS + 
                                      m102006MissingDataDummyTS + m112007MissingDataDummyTS + m122007MissingDataDummyTS);
  
  print(m112001MissingDataDummyTS);
  print(m12006MissingDataDummyTS);
  print(m102006MissingDataDummyTS);
  print(m112007MissingDataDummyTS);
  print(m122007MissingDataDummyTS);
  print(usbeMonthlyMissingDataDummyTS);
  
  
  save(m112001MissingDataDummyTS, file=m112001ACSSUSBEFITimeSeriesMissingDataDummySavePath);
  save(m12006MissingDataDummyTS, file=m12006ACSSUSBEFITimeSeriesMissingDataDummySavePath);
  save(m102006MissingDataDummyTS, file=m102006ACSSUSBEFITimeSeriesMissingDataDummySavePath);
  save(m112007MissingDataDummyTS, file=m112007ACSSUSBEFITimeSeriesMissingDataDummySavePath);
  save(m122007MissingDataDummyTS, file=m122007ACSSUSBEFITimeSeriesMissingDataDummySavePath);

  save(usbeMonthlyMissingDataDummyTS, file=ACSSUSBEFITimeSeriesMissingDataDummySavePath);
  
    #if quarterly  
}else if(dataFrequency == "quarterly"){
  
  q42007MissingDataDummyTS <- createDummyVarriable(ACSSUSBETXNsDataSeriesSentRecieved, vectorColumnNames, dataFrequencyColumnName, "missingData", "2007Q4");

  
  print(q42007MissingDataDummyTS);
  
  save(q42007MissingDataDummyTS, file=ACSSUSBEFITimeSeriesMissingDataDummySavePath);
  
  
    #if annual 
}else if(dataFrequency == "annual"){

    #if daily   
}else if(dataFrequency == "daily"){

  }



gc();#free up some memory


