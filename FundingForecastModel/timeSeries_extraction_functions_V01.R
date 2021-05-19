
##This script is used to collect the FI data generated in the acss_post_processing_file_Level_2_V##.R file.
##The generated file will be used to provide finance with the sumarry details for each FI's sending an recieving habits
##
##Final input data tables that the code will work with are 
##        a: ACSSTransDataSeriesSentRecieved - The ACSS data file
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
##clear memory
gc();

##<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<DECLARE METHODS/FUNCTIONS>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

##The following method extracts the unique column names from the input data table
getColumnNames <- function(inputDataTable){
  colNames <- names(inputDataTable);#create a vector or array of column heading names from the input table
  return(colNames)
}


##This method returns the list of unique FI names in the input data set based on the key field supplied
getListOfFINames <- function(inputDataTable, colNames, keyField){
  gc();
  tempFINamesVector <- NULL;
  if(keyField == "sender" & keyField %in% colNames){#check if the field sender is in the list of column headers and if so get the unique list of sender names
    tempFINamesVector  <- unique(as.character(inputDataTable$sender));
  } else if(keyField == "receiver" & keyField %in% colNames){#otherwise check if the field reciever is in the list of column headers and if so get the unique list of sender names
    tempFINamesVector <- unique(as.character(inputDataTable$receiver));
  }
  return(tempFINamesVector);#return the list of unique FI names
}



##This method returns the list of unique FI names in the input data set based on the key field supplied
getListOfStreamNames <- function(inputDataTable, colNames, keyField){
  gc();
  tempStreamNamesVector <- NULL;
  if(keyField == "stream" & keyField %in% colNames){#check if the field stream is in the list of column headers and if so get the unique list of sender names
    tempStreamNamesVector  <- unique(as.character(inputDataTable$stream));
  } else if(keyField == "tranche" & keyField %in% colNames){#otherwise check if the field reciever is in the list of column headers and if so get the unique list of sender names
    tempStreamNamesVector <- unique(as.character(inputDataTable$tranche));
  }
  return(tempStreamNamesVector);#return the list of unique FI names
}


##The following method extracts the unique dates from the input data table
getListOfTImeSeriesDates <- function(inputDataTable, colNames){
  gc();
  tempFullDataSetDatesVector <- NULL;
  if("Year_Month" %in% colNames){#check if the field sender is in the list of column headers and if so get the unique list of sender names
    print("is month");
    tempFullDataSetDatesVector  <- unique(as.yearmon(inputDataTable$Year_Month, "%YM%m"));
  } else if("Year_Quarter" %in% colNames){#check if the field sender is in the list of column headers and if so get the unique list of sender names
    print("is quarter");
    tempFullDataSetDatesVector  <- unique(as.yearqtr(inputDataTable$Year_Quarter, "%YQ%q"));
    print(tempFullDataSetDatesVector);
  }else if("Year" %in% colNames){#check if the field sender is in the list of column headers and if so get the unique list of sender names
    tempFullDataSetDatesVector  <- unique(inputDataTable$Year);
  }else if("date" %in% colNames){#check if the field sender is in the list of column headers and if so get the unique list of sender names
    tempFullDataSetDatesVector  <- unique(inputDataTable$date);
  }else {#otherwise check if the field reciever is in the list of column headers and if so get the unique list of sender names
    print("No Date field found in input table");
  }
  print("tempFullDataSetDatesVector Dates");
  print(tempFullDataSetDatesVector);
  return(tempFullDataSetDatesVector);#return the list of unique FI names
}



##The following method loops through the array of FI names and for each element of the FI names vector extracts the time series of volume and value data for each
##FI from the input data table
getFieldsAsTimeSeriesDataTable <- function(inputDataTable, colNames, freqCol, minObserv){
  gc();
  inputDataTable <- as.data.table(inputDataTable);
  tsData <- NULL;
  tempTS <- NULL;##temp time series object to be overwritten for each FI
  globalListOfDates <- NULL; #used to filter out FIs that are no longer part of the system
  listOfDates <- NULL;
  listOfNames <- colNames;
  print("listOfNames");
  print(listOfNames);
  print("colNames");
  print(colNames);
  tempDataTable <- NULL;
  tempTSDataTable <- NULL;
  tempTSList <- NULL;
  counter <- 0;
  
  globalListOfDates <- getListOfTImeSeriesDates(inputDataTable, colNames);#set the time series dates for the selected FI
  ##order the table by the Year_Month_Day column
  sort(globalListOfDates);#this is redundant since the tempDataTable is by default ordered by date 
  #but it is included as a failsafe to ensure that the dates are sorted in assending order
  
  tempTSList <- list();
  counter <- counter+1;
  
  
  #This function/method is used to extract the time series data by FI from the input datatable, where each time series generated is labeled/named 
  #using the FI name
  #This implementation uses the base structure of the foreach loop implementation i.e. foreach(i=listOfNames)%do%. However it has been diliberately 
  #written as would be written in Java or C++ to better follow and understand what is happening in the code
  #
  for (i in listOfNames){
    if(i != c("Year_Month", "Year_Quarter", "Year", "Date", "date", "dateTime")){
      print("entered 1st for");
      print(i);
      print(now());
      print("column names");
      print(names(inputDataTable));
      if(freqCol %in% listOfNames){
        tempDataTable <- subset(inputDataTable, select=c(freqCol,i)); #filter the inputDataTable by the sender to select only those entires pertinant to sender i
        ##note that the filtering is done in the if(i==j){} statement to ensure the work is only done when needed
        #print(tempDataTable);
      }
      else{
        print("Input Error: Check the correct data frequency has be specified in the method call");
      }
      tempDataTable <- as.data.table(tempDataTable); #convert to data.table to allow easy sql style reference calls
      print(tempDataTable);
    }
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
        listOfDates <- as.yearmon(listOfDates, "%YM%m");#since the date/time field is a extracted as a vector of characters, it needs to be converted back into a time
        #object. This uses the zoo package
        globalListOfDates <- as.yearmon(globalListOfDates, "%YM%m");#since the date/time field is a extracted as a vector of characters, it needs to be converted back into a time
        print("global list of dates");
        print(globalListOfDates);
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
    if(freqCol == "Year_Month"){
      print("creating tempTimeSeries of FI: ");
      print(i)
      print(now());
      print(length(listOfDates));
      print(length(globalListOfDates));
      
      tsData <- tempDataTable[[2]];
      if(length(tsData) >= minObserv & year(listOfDates[length(listOfDates)]) == year(globalListOfDates[length(globalListOfDates)])){#only add the FI to the 
        #time series list if there are at least the minimum number of required observations and individual FI's final year is consistent with the final year
        #of the entire data set
        tempTS <- ts(tsData, start=c(year(listOfDates[1]), month(listOfDates[1])),
                     end=c(year(listOfDates[length(listOfDates)]), month(listOfDates[length(listOfDates)])), frequency = 12,
                     names=c("TotalVolume"));#create a new time series
        print("adding tempTimeSeries to data.table of FI: ");
        print(i)
        print(now());
        tempLST <- list(SeriesID=i, TimeSeries=tempTS);##Add time sereis and the name of the FI the time sereis 
        ##relates to to a list of all time sereis objects which will be returned by this function
        #names(tempLST) <- i;
        #tempTSList[[counter]] <- tempLST;
        tempTSList[[counter]] <- list(SeriesID=i, TimeSeries=tempTS);
        counter <- counter+1;
      } else  if(freqCol == "Year_Quarter"){
        print("creating tempTimeSeries of FI: ");
        print(i)
        print(now());
        print(length(listOfDates));
        print(length(globalListOfDates));
        
        tsData <- tempDataTable[[2]];
        if(length(tsData) >= minObserv & year(listOfDates[length(listOfDates)]) == year(globalListOfDates[length(globalListOfDates)])){#only add the FI to the 
          #time series list if there are at least the minimum number of required observations and individual FI's final year is consistent with the final year
          #of the entire data set
          tempTS <- ts(tsData, start=c(year(listOfDates[1]), month(listOfDates[1])),
                       end=c(year(listOfDates[length(listOfDates)]), month(listOfDates[length(listOfDates)])), frequency = 4,
                       names=c("TotalVolume"));#create a new time series
          print("adding tempTimeSeries to data.table of FI: ");
          print(i)
          print(now());
          tempLST <- list(SeriesID=i, TimeSeries=tempTS);
          #names(tempLST) <- i;
          #tempTSList[[counter]] <- tempLST;
          tempTSList[[counter]] <- list(SeriesID=i, TimeSeries=tempTS);
          counter <- counter+1;
        }  else  if(freqCol == "Year"){
          print("creating tempTimeSeries of FI: ");
          print(i)
          print(now());
          print(length(listOfDates));
          print(length(globalListOfDates));
          
          tsData <- tempDataTable[[2]];
          if(length(tsData) >= minObserv & year(listOfDates[length(listOfDates)]) == year(globalListOfDates[length(globalListOfDates)])){#only add the FI to the 
            #time series list if there are at least the minimum number of required observations and individual FI's final year is consistent with the final year
            #of the entire data set
            tempTS <- ts(tsData, start=c(year(listOfDates[1]), month(listOfDates[1])),
                         end=c(year(listOfDates[length(listOfDates)]), month(listOfDates[length(listOfDates)])), frequency = 1,
                         names=c("TotalVolume"));#create a new time series
            print("adding tempTimeSeries to data.table of FI: ");
            print(i)
            print(now());
            tempLST <- list(SeriesID=i, TimeSeries=tempTS);
            #names(tempLST) <- i;
            #tempTSList[[counter]] <- tempLST;
            tempTSList[[counter]] <- list(SeriesID=i, TimeSeries=tempTS);
            counter <- counter+1;
          }
      } else if(freqCol == "date"){
        
      }
      }
    }
  }
  return(tempTSList);
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
  print("listOfNames");
  print(listOfNames);
  print("colNames");
  print(colNames);
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
    print("column names");
    print(names(inputDataTable));
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
        listOfDates <- as.yearmon(listOfDates, "%YM%m");#since the date/time field is a extracted as a vector of characters, it needs to be converted back into a time
        #object. This uses the zoo package
        globalListOfDates <- as.yearmon(globalListOfDates, "%YM%m");#since the date/time field is a extracted as a vector of characters, it needs to be converted back into a time
        print("global list of dates");
        print(globalListOfDates);
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


getStreamTimeSeriesDataTable <- function(inputDataTable, colNames, freqCol, seriesValue, keyFld, minObserv){
  gc();
  inputDataTable <- as.data.table(inputDataTable);
  tsData <- NULL;
  tempTS <- NULL;
  globalListOfDates <- NULL; #used to filter out FIs that are no longer part of the system
  listOfDates <- NULL;
  listOfNames <- getListOfStreamNames(inputDataTable, colNames, keyFld);
  print("listOfNames");
  print(listOfNames);
  print("colNames");
  print(colNames);
  tempDataTable <- NULL;
  tempTSDataTable <- NULL;
  tempTSList <- NULL;
  counter <- 0;
  
  globalListOfDates <- getListOfTImeSeriesDates(inputDataTable, colNames);#set the time series dates for the selected FI
  ##ensure there are no NA values in list
  globalListOfDates <- globalListOfDates[!is.na(globalListOfDates)];
  ##order the table by the Year_Month_Day column
  #sort(globalListOfDates);#this is redundant since the tempDataTable is by default ordered by date 
  #but it is included as a failsafe to ensure that the dates are sorted in assending order
  
  
  ##The following if statement is for elegant error handling....
  ##This method/function and R script more generally is to create time series objects from an input data table of 
  ##transactions data. Hence the specificity of hard coding values such as "sender".
  ##The code can be easily transformed into a more generic implementation simply by passing a string object into the function for
  ##the reference field/column (in this case sender) and replacing the "sender" with the passed string object/value
  if(keyFld %in% colNames & "TotalValue" %in% colNames  &&
     "TotalVolume" %in% colNames){
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
    print("column names");
    print(names(inputDataTable));
    tempDataTable <- filter(inputDataTable, (inputDataTable$stream==i)); #filter the inputDataTable by the sender to select only those entires pertinant to sender i
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
        listOfDates <- as.yearmon(listOfDates, "%YM%m");#since the date/time field is a extracted as a vector of characters, it needs to be converted back into a time
        #object. This uses the zoo package
        globalListOfDates <- as.yearmon(globalListOfDates, "%YM%m");#since the date/time field is a extracted as a vector of characters, it needs to be converted back into a time
        print("global list of dates");
        print(globalListOfDates);
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
          tempLST <- list(Stream=i, TimeSeries=tempTS);
          #names(tempLST) <- i;
          #tempTSList[[counter]] <- tempLST;
          tempTSList[[counter]] <- list(Stream=i, TimeSeries=tempTS);
          counter <- counter+1;
        }
        
        ##if quarterly  
      }else if(freqCol == "Year_Quarter"){
        print("creating tempTimeSeries of FI: ");
        print(i)
        print(now());
        #if(length(tsData) >= minObserv & year(listOfDates[length(listOfDates)]) == year(globalListOfDates[length(globalListOfDates)])){#only add the FI to the 
        if(length(tsData) >= minObserv){#only add the FI to the 
          #time series list if there are at least the minimum number of required observations and individual FI's final year is consistent with the final year
          #of the entire data set
          tempTS <- ts(tsData, start=c(year(listOfDates[1]), quarter(listOfDates[1])), 
                       end=c(year(listOfDates[length(listOfDates)]), quarter(listOfDates[length(listOfDates)])), frequency = 4,
                       names=c("TotalVolume"));#create a new time series
          print("adding tempTimeSeries to data.table of FI: ");
          print(i)
          print(now());
          tempLST <- list(Stream=i, TimeSeries=tempTS);
          #names(tempLST) <- i;
          #tempTSList[[counter]] <- tempLST;
          tempTSList[[counter]] <- list(Stream=i, TimeSeries=tempTS);
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
          tempLST <- list(Stream=i, TimeSeries=tempTS);
          #names(tempLST) <- i;
          #tempTSList[[counter]] <- tempLST;
          tempTSList[[counter]] <- list(Stream=i, TimeSeries=tempTS);
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
          tempLST <- list(Stream=i, TimeSeries=tempTS);
          #names(tempLST) <- i;
          #tempTSList[[counter]] <- tempLST;
          tempTSList[[counter]] <- list(Stream=i, TimeSeries=tempTS);
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
          tempLST <- list(Stream=i, TimeSeries=tempTS);
          #names(tempLST) <- i;
          #tempTSList[[counter]] <- tempLST;
          tempTSList[[counter]] <- list(Stream=i, TimeSeries=tempTS);
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
          tempLST <- list(Stream=i, TimeSeries=tempTS);
          #names(tempLST) <- i;
          #tempTSList[[counter]] <- tempLST;
          tempTSList[[counter]] <- list(Stream=i, TimeSeries=tempTS);
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
    tsData <- as.numeric((year(globalListOfDates) == year(as.yearmon(keyFld, '%YM%m')) &
                            (month(globalListOfDates) == month(as.yearmon(keyFld, '%YM%m')))));
    
    tempTS <- ts(tsData, start=c(year(globalListOfDates[1]), month(globalListOfDates[1])),
                 end=c(year(globalListOfDates[length(globalListOfDates)]), month(globalListOfDates[length(globalListOfDates)])), 
                 frequency = 12,
                 names=c(keyFldName));#create a new time series
    #if quarterly  
  }else if(freqCol == "Year_Quarter"){##Year_Quarter
    print("quarterly");
    print(year(as.yearqtr(keyFld, '%YQ%m')));
    print(quarter(as.yearqtr(keyFld, '%YQ%m')));
    
    tsData <- as.numeric((year(globalListOfDates) == year(as.yearqtr(keyFld, '%YQ%m')) &
                            (quarter(globalListOfDates) == quarter(as.yearqtr(keyFld, '%YQ%m')))));
    
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



gc();#free up some memory


