
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


