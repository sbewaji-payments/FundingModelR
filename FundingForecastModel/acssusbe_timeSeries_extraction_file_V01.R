
##This script is used to collect the FI data generated in the usbe_post_processing_file_Level_2_V##.R file.
##The generated file will be used to provide finance with the sumarry details for each FI's sending an recieving habits
##
##Final input data tables that the code will work with are 
##        a: ACSSUSBETransDataSeriesSentRecieved - The ACSS and USBE data file
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

##the usual
gc();
rm(list=ls());
gc();

##<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<IMPORT REQUIRED LIBRARIES>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
##The use of require forces R to install the library if not already installed
require(plyr);# contains method and function calls to be used to filter and select subsets of the data
require(lubridate);# handles that date elements of the data
require(dplyr);# contains method and function calls to be used to filter and select subsets of the data
require(data.table);#used to store and manipiulate data in the form of a data table similar to a mySQL database table
require(xts);#used to manipulate date-time data into quarters
require(ggplot2);#used to plot data
require(zoo);#for date conventions used in financial analysis and quantitiative analysis
require(magrittr);#contains method/function calls to use for chaining e.g. %>%
require(scales);#component of ggplot2 that requires to be manually loaded to handle the scaling of various axies
require(foreach);#handles computations for each element of a vector/data frame/array/etc required the use of %do% or %dopar% operators in base R
require(iterators);#handles iteration through for each element of a vector/data frame/array/etc
require(stringr);#handles strings. Will be used to remove all quotation marks from the data
require(moments);#3rd-party library used to compute statistics such as skewness and kurtosis which are not provided in the base R package set
require(rlist);#3-party package to allow more flexible and java-collections like handling of lists in R. Allows you to save a list as a Rdata file
require(foreign);#to export data into formats readable by STATA and other statistical packages
require(xlsx);#3-party package to export data into excel xlsx format




##<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<DECLARE METHODS/FUNCTIONS>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

##The following method extracts the unique column names from the input data table
getColumnNames <- function(inputDataTable){
  colNames <- names(inputDataTable);#create a vector or array of column heading names from the input table
  return(colNames)
}


##This method returns the list of unique FI names in the input data set based on the key field supplied
getListOfFINames <- function(inputDataTable, colNames, keyFld){
  gc();
  tempFINamesVector <- NULL;
  if(keyFld == "sender" & keyField %in% colNames){#check if the field sender is in the list of column headers and if so get the unique list of sender names
    tempFINamesVector  <- unique(as.character(inputDataTable$sender));
  } else if(keyFld == "receiver" & keyField %in% colNames){#otherwise check if the field reciever is in the list of column headers and if so get the unique list of sender names
    tempFINamesVector <- unique(as.character(inputDataTable$receiver));
  }
  return(tempFINamesVector);#return the list of unique FI names
}



##The following method extracts the unique dates from the input data table
getListOfTImeSeriesDates <- function(inputDataTable, colNames){
  gc();
  tempFullDataSetDatesVector <- NULL;
  if("Year_Month" %in% colNames){#check if the field sender is in the list of column headers and if so get the unique list of sender names
    print("is month");
    tempFullDataSetDatesVector  <- unique(as.yearmon(inputDataTable$Year_Month));
  } else if("Year_Quarter" %in% colNames){#check if the field sender is in the list of column headers and if so get the unique list of sender names
    print("is quarter");
    tempFullDataSetDatesVector  <- unique(as.yearqtr(inputDataTable$Year_Quarter));
    print(tempFullDataSetDatesVector);
  }else if("Year" %in% colNames){#check if the field sender is in the list of column headers and if so get the unique list of sender names
    tempFullDataSetDatesVector  <- unique(inputDataTable$Year);
  }else if("date" %in% colNames){#check if the field sender is in the list of column headers and if so get the unique list of sender names
    tempFullDataSetDatesVector  <- unique(inputDataTable$date);
  }else {#otherwise check if the field reciever is in the list of column headers and if so get the unique list of sender names
    print("No Date field found in input table");
  }
  return(tempFullDataSetDatesVector);#return the list of unique FI names
}



##The following method loops through the array of FI names and for each element of the FI names vector extracts the time series of volume and value data for each
##FI from the input data table
getFITimeSeriesDataTable <- function(inputDataTable, colNames, freqCol, seriesValue, keyFld, minObserv){
  gc();
  inputDataTable <- as.data.table(inputDataTable);
  tsData <- NULL;
  tempTS <- NULL;
  globalListOfDates <- NULL; #used to filter out FIs that are no longer part of the system
  listOfDates <- NULL;
  listOfNames <- getListOfFINames(inputDataTable, colNames, keyFld);
  print(listOfNames);
  tempDataTable <- NULL;
  tempTSDataTable <- NULL;
  tempTSList <- NULL;
  counter <- 0;
  
  globalListOfDates <- getListOfTImeSeriesDates(inputDataTable, colNames);#set the time series dates for the selected FI
  ##order the table by the Year_Month_Day column
  sort(globalListOfDates);#this is redundant since the tempDataTable is by default ordered by date 
  #but it is included as a failsafe to ensure that the dates are sorted in assending order
  
  
  ##The following if statement is for elegant error handling....
  ##This method/function and R script more generally is to create time series objects from an input data table of 
  ##transactions data. Hence the specificity of hard coding values such as "sender".
  ##The code can be easily transformed into a more generic implementation simply by passing a string object into the function for
  ##the reference field/column (in this case sender) and replacing the "sender" with the passed string object/value
  if(keyFld %in% colNames & "TotalValue" %in% colNames  && "TotalVolume" %in% colNames){
    print("entered 1st if");
    print(now());
    tempTSList <- list();
    counter <- counter+1;
  }
  else{
    print("invalid data table sent: check and resend");
  }
  
  #This function/method is used to extract the time series data by FI from the input datatable, where each time series generated is labeled/named 
  #using the FI name
  #This implementation uses the base structure of the foreach loop implementation i.e. foreach(i=listOfNames)%do%. However it has been diliberately 
  #written as would be written in Java or C++ to better follow and understand what is happening in the code
  #
  for (i in listOfNames){ 
    print("entered 1st for");
    print(i);
    print(now());
    tempDataTable <- filter(inputDataTable, (inputDataTable$sender==i)); #filter the inputDataTable by the sender to select only those entires pertinant to sender i
    ##note that the filtering is done in the if(i==j){} statement to ensure the work is only done when needed
    #print(tempDataTable);
    tempDataTable <- as.data.table(tempDataTable); #convert to data.table to allow easy sql style reference calls
    print(tempDataTable);
    if(freqCol %in% colNames){#check for the correct date format to use to order
      print("entered 2nd if");
      print("True");
      print(now());
      if(freqCol == "Year_Month"){ #order the date/time column based on what it is in the input table
        tempDataTable <- tempDataTable[order(Year_Month),];
        listOfDates <- getListOfTImeSeriesDates(tempDataTable, colNames);#set the time series dates for the selected FI
        ##order the table by the Year_Month_Day column
        sort(listOfDates);#this is redundant since the tempDataTable is by default ordered by date 
        #but it is included as a failsafe to ensure that the dates are sorted in assending order
        listOfDates <- as.yearmon(listOfDates);#since the date/time field is a extracted as a vector of characters, it needs to be converted back into a time
        #object. This uses the zoo package
        globalListOfDates <- as.yearmon(globalListOfDates);#since the date/time field is a extracted as a vector of characters, it needs to be converted back into a time
        #object. This uses the zoo package
        #if quarterly  
      }else if(freqCol == "Year_Quarter"){##Year_Quarter
        print(tempDataTable);
        #tempDataTable <- tempDataTable[order(Year_Quarter),];
        listOfDates <- getListOfTImeSeriesDates(tempDataTable, colNames);#set the time series dates for the selected FI
        ##order the table by the Year_Month_Day column
        sort(listOfDates);#this is redundant since the tempDataTable is by default ordered by date 
        #but it is included as a failsafe to ensure that the dates are sorted in assending order
        print(length(listOfDates));
        listOfDates <- as.yearqtr(listOfDates);#since the date/time field is a extracted as a vector of characters, it needs to be converted back into a time
        #object. This uses the zoo package
        globalListOfDates <- as.yearqtr(globalListOfDates);#since the date/time field is a extracted as a vector of characters, it needs to be converted back into a time
        #object. This uses the zoo package
        print(length(listOfDates));
        print(length(globalListOfDates));
        #if annual 
      }else if(freqCol == "Year"){
        tempDataTable <- tempDataTable[order(Year),];
        listOfDates <- getListOfTImeSeriesDates(tempDataTable, colNames);#set the time series dates for the selected FI
        ##order the table by the Year_Month_Day column
        sort(listOfDates);#this is redundant since the tempDataTable is by default ordered by date 
        #but it is included as a failsafe to ensure that the dates are sorted in assending order
        listOfDates <- numeric(listOfDates);#since the date/time field is a extracted as a vector of characters, it needs to be converted back into a time
        #object. In this instance, years are set to be numeric values.
        globalListOfDates <- numeric(globalListOfDates);#since the date/time field is a extracted as a vector of characters, it needs to be converted back into a time
        #object. In this instance, years are set to be numeric values.
        #if daily   
      }else if(freqCol == "date"){
        tempDataTable <- tempDataTable[order(date),];
      }
    } else {#error handling incase the date/time field is not already predefined
      print("Invalid data frequency/time field naming passed check input data.table object 
            or update the getFIVolumeTimeSeriesDataTable function if naming is correct");
    }
    
    ## if the series to extract is for transaction volumes
    if(seriesValue %in% c("volume","Volume","Vol","VOLUME")){#set to capture the possible entries that could be used for volume
      tsData <- tempDataTable$TotalVolume;
      print(tsData);
      ##build the timeSeries object for the individual FIs depending on the value of the frequency column name
      if(freqCol == "Year_Month"){
        print("creating tempTimeSeries of FI: ");
        print(i)
        print(now());
        print(length(listOfDates));
        print(length(globalListOfDates));
        if(length(tsData) >= minObserv & year(listOfDates[length(listOfDates)]) == year(globalListOfDates[length(globalListOfDates)])){#only add the FI to the 
          #time series list if there are at least the minimum number of required observations and individual FI's final year is consistent with the final year
          #of the entire data set
          tempTS <- ts(tsData, start=c(year(listOfDates[1]), month(listOfDates[1])),
                       end=c(year(listOfDates[length(listOfDates)]), month(listOfDates[length(listOfDates)])), frequency = 12,
                       names=c("TotalVolume"));#create a new time series
          print("adding tempTimeSeries to data.table of FI: ");
          print(i)
          print(now());
          tempLST <- list(FinancialInstitution=i, TimeSeries=tempTS);
          #names(tempLST) <- i;
          #tempTSList[[counter]] <- tempLST;
          tempTSList[[counter]] <- list(FinancialInstitution=i, TimeSeries=tempTS);
          counter <- counter+1;
        }
        
        ##if quarterly  
      }else if(freqCol == "Year_Quarter"){
        print("creating tempTimeSeries of FI: ");
        print(i)
        print(now());
        if(length(tsData) >= minObserv & year(listOfDates[length(listOfDates)]) == year(globalListOfDates[length(globalListOfDates)])){#only add the FI to the 
          #time series list if there are at least the minimum number of required observations and individual FI's final year is consistent with the final year
          #of the entire data set
          tempTS <- ts(tsData, start=c(year(listOfDates[1]), quarter(listOfDates[1])), 
                       end=c(year(listOfDates[length(listOfDates)]), quarter(listOfDates[length(listOfDates)])), frequency = 4,
                       names=c("TotalVolume"));#create a new time series
          print("adding tempTimeSeries to data.table of FI: ");
          print(i)
          print(now());
          tempLST <- list(FinancialInstitution=i, TimeSeries=tempTS);
          #names(tempLST) <- i;
          #tempTSList[[counter]] <- tempLST;
          tempTSList[[counter]] <- list(FinancialInstitution=i, TimeSeries=tempTS);
          counter <- counter+1;
        }
        
        ##if annual 
      }else if(freqCol == "Year"){
        print("creating tempTimeSeries of FI: ");
        print(i)
        print(now());
        if(length(tsData) >= minObserv & year(listOfDates[length(listOfDates)]) == year(globalListOfDates[length(globalListOfDates)])){#only add the FI to the 
          #time series list if there are at least the minimum number of required observations and individual FI's final year is consistent with the final year
          #of the entire data set
          tempTS <- ts(tsData, start=c(year(listOfDates[1])), 
                       end=c(year(listOfDates[length(listOfDates)])), frequency = 1,
                       names=c("TotalVolume"));#create a new time series
          print("adding tempTimeSeries to data.table of FI: ");
          print(i)
          print(now());
          tempLST <- list(FinancialInstitution=i, TimeSeries=tempTS);
          #names(tempLST) <- i;
          #tempTSList[[counter]] <- tempLST;
          tempTSList[[counter]] <- list(FinancialInstitution=i, TimeSeries=tempTS);
          counter <- counter+1;
        }
        
        ##if daily
      }else if(freqCol == "date"){
        
      }
    }
    ## if the series to extract is for transaction values
    else if(seriesValue %in% c("value","Value","Val","VALUE")){#set to capture the possible entries that could be used for value
      tsData <- tempDataTable$TotalValue;
      ##build the timeSeries object for the individual FIs depending on the value of the frequency column name
      if(freqCol == "Year_Month"){
        print("creating tempTimeSeries of FI: ");
        print(i)
        print(now());
        if(length(tsData) >= minObserv & year(listOfDates[length(listOfDates)]) == year(globalListOfDates[length(globalListOfDates)])){#only add the FI to the 
          #time series list if there are at least the minimum number of required observations and individual FI's final year is consistent with the final year
          #of the entire data set
          tempTS <- ts(tsData, start=c(year(as.yearmon(listOfDates[1])), month(as.yearmon(listOfDates[1]))), 
                       end=c(year(listOfDates[length(listOfDates)]), month(listOfDates[length(listOfDates)])), frequency = 12,
                       names=c("TotalValue"));#create a new time series
          print("adding tempTimeSeries to data.table of FI: ");
          print(i)
          print(now());
          tempLST <- list(FinancialInstitution=i, TimeSeries=tempTS);
          #names(tempLST) <- i;
          #tempTSList[[counter]] <- tempLST;
          tempTSList[[counter]] <- list(FinancialInstitution=i, TimeSeries=tempTS);
          counter <- counter+1;
        }
        
        
        ##if quarterly  
      }else if(freqCol == "Year_Quarter"){
        print("creating tempTimeSeries of FI: ");
        print(i)
        print(now());
        if(length(tsData) >= minObserv & year(listOfDates[length(listOfDates)]) == year(globalListOfDates[length(globalListOfDates)])){#only add the FI to the 
          #time series list if there are at least the minimum number of required observations and individual FI's final year is consistent with the final year
          #of the entire data set
          tempTS <- ts(tsData, start=c(year(listOfDates[1]), quarter(listOfDates[1])), 
                       end=c(year(listOfDates[length(listOfDates)]), quarter(listOfDates[length(listOfDates)])), frequency = 4,
                       names=c("TotalValue"));#create a new time series
          print("adding tempTimeSeries to data.table of FI: ");
          print(i)
          print(now());
          tempLST <- list(FinancialInstitution=i, TimeSeries=tempTS);
          #names(tempLST) <- i;
          #tempTSList[[counter]] <- tempLST;
          tempTSList[[counter]] <- list(FinancialInstitution=i, TimeSeries=tempTS);
          counter <- counter+1;
        }
        
        
        ##if annual 
      }else if(freqCol == "Year"){
        print("creating tempTimeSeries of FI: ");
        print(i)
        print(now());
        if(length(tsData) >= minObserv & year(listOfDates[length(listOfDates)]) == year(globalListOfDates[length(globalListOfDates)])){#only add the FI to the 
          #time series list if there are at least the minimum number of required observations and individual FI's final year is consistent with the final year
          #of the entire data set
          tempTS <- ts(tsData, start=c(year(listOfDates[1])), 
                       end=c(year(listOfDates[length(listOfDates)])), frequency = 1,
                       names=c("TotalValue"));#create a new time series
          print("adding tempTimeSeries to data.table of FI: ");
          print(i)
          print(now());
          tempLST <- list(FinancialInstitution=i, TimeSeries=tempTS);
          #names(tempLST) <- i;
          #tempTSList[[counter]] <- tempLST;
          tempTSList[[counter]] <- list(FinancialInstitution=i, TimeSeries=tempTS);
          counter <- counter+1;
        }
        
        
        ##if daily   
      }else if(freqCol == "date"){
        
      }
    }
    }
  return(tempTSList);
}


createDummyVarriable <- function(inputDataTable, colNames, freqCol, keyFldName, keyFld){
  gc();
  #inputDataTable <- as.data.table(inputDataTable);
  tsData <- NULL;
  tempTS <- NULL;
  globalListOfDates <- NULL; #used to filter out FIs that are no longer part of the system
  tempTSDataTable <- NULL;
  tempTSList <- NULL;
  counter <- 0;
  
  globalListOfDates <- getListOfTImeSeriesDates(inputDataTable, colNames);#set the time series dates for the selected FI
  ##order the table by the Year_Month_Day column
  sort(globalListOfDates);#this is redundant since the inputDataTable is by default ordered by date 
  #but it is included as a failsafe to ensure that the dates are sorted in assending order
  
  if(freqCol == "Year_Month"){ #order the date/time column based on what it is in the input table
    print("monthly");
    tsData <- as.numeric((year(globalListOfDates) == year(as.yearmon(keyFld)) &
                            (month(globalListOfDates) == month(as.yearmon(keyFld)))));
    
    tempTS <- ts(tsData, start=c(year(globalListOfDates[1]), month(globalListOfDates[1])),
                 end=c(year(globalListOfDates[length(globalListOfDates)]), month(globalListOfDates[length(globalListOfDates)])), frequency = 12,
                 names=c(keyFldName));#create a new time series
    #if quarterly  
  }else if(freqCol == "Year_Quarter"){##Year_Quarter
    print("quarterly");
    print(year(as.yearqtr(keyFld)));
    print(quarter(as.yearqtr(keyFld)));
    
    tsData <- as.numeric((year(globalListOfDates) == year(as.yearqtr(keyFld)) &
                            (quarter(globalListOfDates) == quarter(as.yearqtr(keyFld)))));
    
    #tsData <- ifelse((year(globalListOfDates) == year(as.yearqtr(keyFld)) &
     #                   (quarter(globalListOfDates) == quarter(as.yearqtr(keyFld)))), 1,0);
    
    globalListOfDates <- as.yearqtr(globalListOfDates);#since the date/time field is a extracted as a vector of characters,
    #it needs to be converted back into a time object. This uses the zoo package
    tempTS <- ts(tsData, start=c(year(globalListOfDates[1]), quarter(globalListOfDates[1])), 
                 end=c(year(globalListOfDates[length(globalListOfDates)]), quarter(globalListOfDates[length(globalListOfDates)])), frequency = 4,
                 names=c(keyFldName));#create a new time series
    print(length(globalListOfDates));
    #if annual 
  }else if(freqCol == "Year"){
    inputDataTable <- inputDataTable[order(Year),];
    tsData <- as.numeric((year(globalListOfDates) == as.numeric(keyFld)));
    globalListOfDates <- numeric(globalListOfDates);#since the date/time field is a extracted as a vector of characters,
    #it needs to be converted back into a time object. In this instance, years are set to be numeric values.
    tempTS <- ts(tsData, start=c(year(globalListOfDates[1])), 
                 end=c(year(globalListOfDates[length(globalListOfDates)])), frequency = 1,
                 names=c(keyFldName));#create a new time series
    
    print("adding tempTimeSeries to data.table of FI: ");
    #if daily   
  }else if(freqCol == "date"){
    inputDataTable <- inputDataTable[order(date),];
  } else {#error handling incase the date/time field is not already predefined
      print("Invalid data frequency/time field naming passed check input data.table object 
        or update the getFIVolumeTimeSeriesDataTable function if naming is correct");
  }
  #tempTSList[1] <- tempTS;

  return(tempTS);
}




##<<<<<<<<<<<<<<<<<<<<<<<<<DECLARE GLOBAL VARIABLES>>>>>>>>>>>>>>>>>>>>>>>>
##Declare all global variables to be used in this class
minObservations <- NULL; #minimum number of observations required for the collected time series
keyField <- "sender"; #the colum to be used as the unique key forthe data table (can be "sender" or "reciever")'
dataFrequency <- "quarterly"; #the options are monthly, quarterly, annual, daily (does not make sense since it does not distingush the year)
dataFrequencyColumnName <- NULL; #used to capture the name of the time colum for use in the time series generation method/function.
                                 #dataFrequencyColumnName can take values "Year_Month", "Year_Quarter", "Year" "date" and will be set automatically 
                                 #based on the dataFrequency
extractedACSSUSBEDataLoadPath <- NULL; #used to store the location of the RData file from which the time sereis are to be generate
ACSSUSBETransDataSeriesSavePath <- NULL;
ACSSUSBETransDataSeriesSentRecievedSavePath <- NULL;




#will load the data table: USBETransDataSeriesSentRecieved
#and set the name of the time column (dataFrequencyColumnName)
#if monthly
if(dataFrequency == "monthly"){
  minObservations <- 24; # 12*2 number of observations per period multiplied by 2 full periods of data minimum number of observations required for the collected time series
  ##The ARIMA analysis requires 2 full periods
  extractedACSSUSBEDataLoadPath <- c("C:/Projects/FundingDataTables/acssusbeTransSentRecievedByFIMonthlySeries2000-2016.Rdata");
  ACSSUSBEFITimeSeriesDataTableSavePath <- 
c("C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/ExtractedTimeSeriesData/ACSSUSBEFIMonthlyTimeSeriesData2000-2016.RData");
  ACSSUSBEFITimeSeriesMissingDataDummySavePath <- 
    c("C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/ExtractedTimeSeriesData/ACSSUSBEFIMonthlyTSMissingDataDummy.RData");
  dataFrequencyColumnName <- "Year_Month";
  #if quarterly  
}else if(dataFrequency == "quarterly"){
  minObservations <- 8; # 4*2 number of observations per period multiplied by 2 full periods of data minimum number of observations required for the collected time series
  ##The ARIMA analysis requires 2 full periods
  extractedACSSUSBEDataLoadPath <- c("C:/Projects/FundingDataTables/acssusbeTransSentRecievedByFIQuarterlySeries2000-2016.Rdata");
  ACSSUSBEFITimeSeriesDataTableSavePath <- 
c("C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/ExtractedTimeSeriesData/ACSSUSBEFIQuarterlyTimeSeriesData2000-2016.RData");
  ACSSUSBEFITimeSeriesMissingDataDummySavePath <- 
    c("C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/ExtractedTimeSeriesData/ACSSUSBEFIQuarterlyTSMissingDataDummy.RData");
  dataFrequencyColumnName <- "Year_Quarter";
  #if annual 
}else if(dataFrequency == "annual"){
  minObservations <- 2; # 1*2 number of observations per period multiplied by 2 full periods of data minimum number of observations required for the collected time series
  ##The ARIMA analysis requires 2 full periods
  extractedACSSUSBEDataLoadPath <- c("C:/Projects/FundingDataTables/acssusbeTransSentRecievedByFIAnnualSeries2000-2016.Rdata");
  ACSSUSBEFITimeSeriesDataTableSavePath <- 
c("C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/ExtractedTimeSeriesData/ACSSUSBEFIAnnualTimeSeriesData2000-2016.RData");
  ACSSUSBEFITimeSeriesMissingDataDummySavePath <- 
    c("C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/ExtractedTimeSeriesData/ACSSUSBEFIAnnualTSMissingDataDummy.RData");
  dataFrequencyColumnName <- "Year";
  #if daily   
}else if(dataFrequency == "daily"){
  minObservations <- 540; # 250*2 number of observations per period multiplied by 2 full periods of data minimum number of observations required for the collected time series
  ##The ARIMA analysis requires 2 full periods
  extractedACSSUSBEDataLoadPath <- c("");
  ACSSUSBEFITimeSeriesDataTableSavePath <- 
    c("C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/ExtractedTimeSeriesData/ACSSUSBEFIDailyTimeSeriesData2000-2016.RData");
  ACSSUSBEFITimeSeriesMissingDataDummySavePath <- 
    c("C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/ExtractedTimeSeriesData/ACSSUSBEFIDailyTSMissingDataDummyTS.RData");
  dataFrequencyColumnName <- "date";
}

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
ACSSUSBETransDataSeriesSentRecieved <- as.data.table(ACSSUSBETransDataSeriesSentRecieved);
gc();



if(dataFrequency == "monthly"){
  ACSSUSBETransDataSeriesSentRecieved$Year_Month <- as.yearmon(ACSSUSBETransDataSeriesSentRecieved$Year_Month);
  ACSSUSBETransDataSeriesSentRecieved <- ACSSUSBETransDataSeriesSentRecieved[order(Year_Month),];
  #if quarterly  
}else if(dataFrequency == "quarterly"){
  ACSSUSBETransDataSeriesSentRecieved$Year_Quarter <- as.yearqtr(ACSSUSBETransDataSeriesSentRecieved$Year_Quarter);
  ACSSUSBETransDataSeriesSentRecieved <- ACSSUSBETransDataSeriesSentRecieved[order(Year_Quarter),];
  #if annual 
}else if(dataFrequency == "annual"){
  ACSSUSBETransDataSeriesSentRecieved <- ACSSUSBETransDataSeriesSentRecieved[order(Year),];
  #if daily   
}else if(dataFrequency == "daily"){
  ACSSUSBETransDataSeriesSentRecieved <- ACSSUSBETransDataSeriesSentRecieved[order(date),];
}

vectorColumnNames <- getColumnNames(ACSSUSBETransDataSeriesSentRecieved);
vectorFINames <- getListOfFINames(ACSSUSBETransDataSeriesSentRecieved, vectorColumnNames, "sender");
vectorOfTimeSeriesDates <- getListOfTImeSeriesDates(ACSSUSBETransDataSeriesSentRecieved, vectorColumnNames);
vectorOfTimeSeriesDates <- as.yearqtr(vectorOfTimeSeriesDates);


print("creating timeSeries Tables/Collection");
print(now());#used to record speed of computations

ACSSUSBEFITimeSeriesDataTable <- 
  getFITimeSeriesDataTable(as.data.table(ACSSUSBETransDataSeriesSentRecieved),vectorColumnNames, dataFrequencyColumnName, "Volume", "sender",minObservations);
ACSSUSBEFITimeSeriesDataTable <- list.names(ACSSUSBEFITimeSeriesDataTable,"ACSSUSBEFITimeSeriesList");
list.save(ACSSUSBEFITimeSeriesDataTable, ACSSUSBEFITimeSeriesDataTableSavePath);

q42007MissingDataDummyTS <- createDummyVarriable(ACSSUSBETransDataSeriesSentRecieved, vectorColumnNames, dataFrequencyColumnName, "missingData", "2007Q4");
print(q42007MissingDataDummyTS);

save(q42007MissingDataDummyTS, file=ACSSUSBEFITimeSeriesMissingDataDummySavePath);


gc();#free up some memory


