##This is class to test chaining of the filter and select methods/function in R for funding data
##to work with for work on individual FIs in the funding model
##
## Author : Segun Bewaji
## Modified : Segun Bewaji
## Modifications Made:
##        1: Refinement of getSummaryStatisticsAmountByReceiver method to include FI pairs and payment frequerncy. 
##           NOTE: Payment frequency is not a based on native counting functions in R. I found these to be too convoluted
##                 (put down to statistitians dreaming up a programming langues I guess :)). 
##                 Rather than using the the table(x), xtabs(x) or tabulate(x) functions in R for counting values, the calcualtion
##                 of the payment frequency is derived from the mean of payment and sum of payments knowing that 
##                 mean(x) = sum(x)/freq(x); hence freq(x) = sum(x)/mean(x). This mathematical manipulation is simply easier and 
##                 more efficient to compute from a programming/code development standpoint
##
## Creation Date : 25 May 2015
## Modification Date : 28-29 May 2015
## Modification Date : 1-3 June 2015
## $Id$


##clear system memory and workspace
gc();
rm(list=ls());
gc();

##import require libraries 
require(lubridate);# handles that date elements of the data
require(dplyr);# contains method and function calls to be used to filter and select subsets of the data
require(data.table);#used to store and manipiulate data in the form of a data table similar to a mySQL database table
require(ggplot2);#used to plot data
require(magrittr);#contains method/function calls to use for chaining e.g. %>%
require(scales);#component of ggplot2 that requires to be manually loaded to handle the scaling of various axies
require(foreign);#to export data into formats readable by STATA and other statistical packages
require(xlsx);#3-party package to export data into excel xlsx format

##<<<<<<<<<<<<<<<<<<<<<<<<<DECLARE GLOBAL VARIABLES>>>>>>>>>>>>>>>>>>>>>>>>

fiStaticDataLoadPath <- c("C:/Projects/FundingDataTables/FIStaticData.Rdata");#will load the data table: fiStaticDataFile
startYr <- 2005;
endYr <- 2015;

##Declare all global variables to be used in this class
isDaily <- FALSE;
workingDataTableMonthlyByYear  <- NULL; ##working datatable

vectorFINames <-  NULL;
vectorColumnNames <- NULL;


##<<<<<<<<<<<<<<<<<<<<<<<<<LOAD DATA>>>>>>>>>>>>>>>>>>>>>>>>
##Now load the individual FI's complete data tables
print("Loading data table");#used to record speed of computations
print(now());#used to record speed of computations
load("C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/DataFiles/YearsSpecificLVTSTransYM2000-15DataSeries.Rdata");
load(fiStaticDataLoadPath);
##c("C:/Projects/FundingDataTables/YearsSpecificLVTSTransYM2000-15DataSeries.Rdata");
#mergedLVTSTransYearsSpecYM
gc();
print("data table loaded");#used to record speed of computations
print(now());#used to record speed of computations
gc();

##<<<<<<<<<<<<<<<<<<<<<<<<<DECLARE CLASS METHODS/FUNCTIONS>>>>>>>>>>>>>>>>>>>>>>>>
getColumnNames <- function(inputDataTable){
  colNames <- names(inputDataTable);#create a vector or array of column heading names from the input table
  print(colNames)
  print("vectorColumnNames built");#used to record speed of computations
  return(colNames)
}



##version containing the data for the specific month and year of interest
getAnnualDataRangeExtraction <- function(inputDataTable, startYr, endYr, lookupColumnName, colNames){
  temporaryTable <- NULL;
  inputDataTable <- as.data.table(inputDataTable);
  if(endYr < startYr){
    return(print("start year is greater than end year: please check and correct entry"))
  }else if(lookupColumnName %in% colNames & lookupColumnName == "Year_Month"){ #check if the key look-up column name is
    #in (i.e. %in%) the list of column names
    ##the following filter and this IF-statement in general is hardcoded because R does not appear to have a way of 
    ##passing strings to functions to get column contents. THe ideal implementation would simply be
    ##"inputDataTable$lookupColumnName" and would not require checking that "lookupColumnName == "Year_Month""
    print("Building TempTable");#used to record speed of computations
    print(now());#used to record speed of computations
    gc();
    temporaryTable <-  filter(inputDataTable, (year(inputDataTable$Year_Month) >= startYr & year(inputDataTable$Year_Month) <= endYr)
    );##end of filter statement
  };#end of if statment
  return(temporaryTable)
}

##This method returns the list of FIs in the input data set
getListOfFINames <- function(inputDataTable, colNames){
  gc();
  tempFINamesVector <- NULL;
  if("sender" %in% colNames){#check if the field sender is in the list of column headers and if so get the unique list of sender names
    tempFINamesVector  <- unique(inputDataTable$sender);
  } else if("receiver" %in% colNames){#otherwise check if the field reciever is in the list of column headers and if so get the unique 
    ##list of sender names
    tempFINamesVector <- unique(inputDataTable$receiver);
  }
  return(tempFINamesVector);#return the list of unique FI names
}




##<<<<<<<<<<<<<<<<<<<<<<<<<PROCESS REQUIRED CALCULATIONS AND PUBLISH OUTPUTS>>>>>>>>>>>>>>>>>>>>>>>>>
print("Build and Processing started");#used to record speed of computations
print(now());#used to record speed of computations
gc();
vectorColumnNames <- getColumnNames(mergedLVTSTransYearsSpecYM);
print(now());#used to record speed of computations
gc();
gc();
workingDataTableMonthlyByYear <- getAnnualDataRangeExtraction(mergedLVTSTransYearsSpecYM,startYr,endYr,"Year_Month",vectorColumnNames);
rm(mergedLVTSTransYearsSpecYM);
tempDT <- workingDataTableMonthlyByYear[,sum(amount), by=c("sender")];
tempDT2 <- workingDataTableMonthlyByYear[,sum(amount), by=c("receiver")];
##return to the original directory
gc();
setwd(cur.dir);