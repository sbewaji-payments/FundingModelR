##This script is used to run the post processing for LVTS data
##The .R files used are  
##post_processing_file_functions_V01.R for the functions used
##lvts_post_processing_file_Level_1_V03.R for LVTS data files
##lvts_post_processing_file_Level_2_V07.R
##
##
##
## Author : Segun Bewaji
## Creation Date : 05 Dec 2016
## Modified : Segun Bewaji
## Modifications MadC: 05 Dec 2016
##        1) wrote lines to run USBE and LVTS data files processing scripts
##        2) 
##        3)  
##           
##        4)  
##           
##        5) 
##        6)  
## Modified : Segun Bewaji
## Modifications MadC:
## $Id$

##the usual
gc();
rm(list=ls());
gc();

##import required libraries
require(plyr);# contains method and function calls to be used to filter and select subsets of the data
require(lubridate);# handles that date elements of the data
require(dplyr);# contains method and function calls to be used to filter and select subsets of the data
require(data.table);#used to store and manipiulate data in the form of a data table similar to a mySQL database table
require(xts);#used to manipulate date-time data into quarters
require(ggplot2);#used to plot data
require(magrittr);#contains method/function calls to use for chaining e.g. %>%
require(scales);#component of ggplot2 that requires to be manually loaded to handle the scaling of various axies
require(foreach);#handles computations for each element of a vector/data frame/array/etc required the use of %do% or %dopar% operators in base R
require(iterators);#handles iteration through for each element of a vector/data frame/array/etc
require(stringr);#handles strings. Will be used to remove all quotation marks from the data
require(foreign);#to export data into formats readable by STATA and other statistical packages
require(xlsx);#3-party package to export data into excel xlsx format


options(scipen=999);

##<<<<<<<<<<<<<<<<<<<<<<<<<DECLARE GLOBAL VARIABLES>>>>>>>>>>>>>>>>>>>>>>>>
##Declare all global variables to be used in this class

##<<<<<<<<<<<<<<<<<<<<<<<<<DECLARE GLOBAL VARIABLES>>>>>>>>>>>>>>>>>>>>>>>>
##Declare all global variables to be used in this class
startYr <- 2000;
endYr <- 2021;

##Set Data Frequency
dataFrequency <- "monthly"; #the options are monthly, quarterly, annual, daily, dateTime (does not make sense since it does not distingush the year)
dateTimeNettingFrequency <- "5 min"; #This is used only if the dataFrequency is dateTime.
#Available options are "x min", "x hour", "x day" etc where x is the integer value of the interval

byStream <- FALSE;##Note that no stream level analysis can be done on USBE data prior to 2010 due to missing TXN data files
onlyACSS <- FALSE;##Note that no stream level analysis can be done on USBE data prior to 2010 due to missing TXN data files


###Load used functions
gc();
print("Loading Used Functions");
source('G:/Development Workspaces/R-Workspace/DataCollection/post_processing_file_functions_V01.R', encoding = 'UTF-8', echo=TRUE);
gc();
##Run Level 1 post porocessing
##NOTC: Save paths are located in the source/script files
gc();
print("Commencing ACSS post processing");
source('G:/Development Workspaces/R-Workspace/DataCollection/acss_post_processing_EntryPoint_V02.R', encoding = 'UTF-8', echo=TRUE);
print("Level 1 post processing complete");
gc();

##Run Level USBE post porocessing 
gc();
if(!onlyACSS){
  print("Commencing USBE post processing");
  source('G:/Development Workspaces/R-Workspace/DataCollection/usbe_post_processing_EntryPoint_V02.R', encoding = 'UTF-8', echo=TRUE);
  print("Level 2 post processing complete");
  gc();
  print("Commencing Merging of ACSS and USBE");
  source('G:/Development Workspaces/R-Workspace/DataCollection/merge_Cleaned_ACSS_USBE_TXN_Data_V02.R', encoding = 'UTF-8', echo=TRUE);
  print("Level 2 post processing complete"); 
}


##Temp Raw data Extraction
mergedACSSTXNsYearsSpecYMCSVFile <-  c("E:/Projects/FundingDataTables/Raw Transactions Data/YearsSpecificACSSTXNsYM2006-2020DataSeries.csv")
write.csv(mergedACSSTXNsYearsSpecYM, file=mergedACSSTXNsYearsSpecYMCSVFile);


gc();
