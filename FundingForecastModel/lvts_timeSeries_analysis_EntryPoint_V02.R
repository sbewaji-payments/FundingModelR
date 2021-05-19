##This is the class in which the statistical analysis and forecasting is carried out
##The forecasts are carried out by looping through each element of the Timeseries list and assessing the timeseries 
## (TS) object contained therein
##
##
## Author : Segun Bewaji
## Modified : Segun Bewaji
## Modifications Made:
##        1: . 
##           NOTE: 
##                   
##                 
##                  
##                  
##                 
##        2: 
##           NOTE: 
##                 
##                 
##        3: 
##            
##           

## Creation Date : 23 June 2015
## Modification Date : 
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
require(moments);#3rd-party library used to compute statistics such as skewness and kurtosis which are not provided in the base R package set
require(rlist);#3-party package to allow more flexible and java-collections like handling of lists in R. Allows you to save a list as a Rdata file
require(foreign);#to export data into formats readable by STATA and other statistical packages
require(xts);#used to manipulate date-time data into quarters
require(xlsx);#to export data into formats readable by excel
require(lattice);#to export data into formats readable by excel
require(latticeExtra);#to export data into formats readable by excel
require(forecast);#to export data into formats readable by excel
require(tseries);
require(timeSeries);
require(astsa);##Load the astsa package for SARIMA forcasting 
#require(sarima);##Load the sarima package
require(TSA);

options(scipen=999);##To set the use of scientific notation in your entire R session, you can use the scipen option.


##<<<<<<<<<<<<<<<<<<<<<<<<<DECLARE GLOBAL VARIABLES>>>>>>>>>>>>>>>>>>>>>>>>
##Declare all global variables to be used in this class
startYear <- 2000;
endYear <- 2019;
inSampleForecastEndYear <- 2014;
forecastEndYear <- 2043;
measurementUnitType <- "Volume";
measurementUnit <- 1000000; #possible values 1000000, 1000, or 1
dataFrequency <- "quarterly"; #the options are monthly, quarterly, annual, daily (does not make sense since it does not distingush the year)
forecastDate <- Sys.Date();##Set date the forecast was run on. This will be used to save the forecast outups to prevent
##overwriting previous results
confidenceLevel <- 95 #c(80,95)

keyField <- "sender"; #the colum to be used as the unique key forthe data table (can be "sender" or "reciever")'
arimaOrder <- c(1,1,1); ##Model order component for ARIMA model 
forecastHorizen  <- 12; ##coincides with 3yrs of quarterly forecasts (Finance Corpoerate Funding Model) and 1yr of monthly forecasts (COO Dashboard)
arimaDrift <- TRUE;
arimaConstant <- TRUE;
seasonalOrder <- c(0,1,2); ##Seasonal order component for SARIMA model
nSPeriods <- 1; ##number of periods in seasonal model component
modelSelection <- "ARIMA";##can take the form SARIMA, ARIMA, etc
sarimaDrift <- FALSE;
sarimaConstant <- FALSE;



dataFrequencyColumnName <- NULL; #used to capture the name of the time colum for use in the time series generation method/function.
#dataFrequencyColumnName can take values "Year_Month", "Year_Quarter", "Year" "date" and will be set automatically 
#based on the dataFrequency
extractedLVTSFITimeSeriesLoadPath <- NULL; #used to store the location of the RData file from which the time sereis are to be generate
timeSeriesPlotsLVTS <- NULL;
timeSeriesDecompositionPlotsLVTS <- NULL;
timeSeriesPACFPlotsLVTS <- NULL;
timeSeriesACFPlotsLVTS <- NULL;
timeSeriesDecompositionLVTS <- NULL;
timeSeriesARIMAForecastsLVTS <- NULL;
dtListFILevelARIMAForecastsLVTS <- NULL;
dtListFILevelARIMACoefficientsLVTS <- NULL;
chartListFILevelARIMAForecastLVTS <- NULL;
seasonallyAdjustedTimeSeriesPlotsLVTS <- NULL;
forecastsSavePath <- NULL;




#will load the data table: LVTSTransDataSeriesSentRecieved
#and set the name of the time column (dataFrequencyColumnName)
#if monthly
if(dataFrequency == "monthly"){
  forecastHorizen  <- (forecastEndYear - endYear)*12;
  extractedLVTSFITimeSeriesLoadPath <- 
    c(paste("C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/ExtractedTimeSeriesData/LVTSFIMonthlyTimeSeriesData",
            startYear,"-",endYear,".RData", sep = "", collapse = NULL));
  forecastsSavePath <- 
    c(paste("C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/MonthlyForecasts/FI_Level/LVTS/",
            forecastDate, "/",sep = ""));
  dataFrequencyColumnName <- "Year_Month";
  nSPeriods <- 12;
  #if quarterly  
}else if(dataFrequency == "quarterly"){
  forecastHorizen  <- (forecastEndYear - endYear)*4;
  extractedLVTSFITimeSeriesLoadPath <- 
    c(paste("C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/ExtractedTimeSeriesData/LVTSFIQuarterlyTimeSeriesData",
            startYear,"-",endYear,".RData", sep = "", collapse = NULL));
  forecastsSavePath <- 
  c(paste("C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/QuarterlyForecasts/FI_Level/LVTS/",
          forecastDate, "/",sep = ""));
  dataFrequencyColumnName <- "Year_Quarter";
  nSPeriods <- 4;
  #if annual 
}else if(dataFrequency == "annual"){
  forecastHorizen  <- (forecastEndYear - endYear);
  extractedLVTSFITimeSeriesLoadPath <- 
    c(paste("C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/ExtractedTimeSeriesData/LVTSFIAnnualTimeSeriesData",
            startYear,"-",endYear,".RData", sep = "", collapse = NULL));
  forecastsSavePath <- 
  c(paste("C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/AnnualForecasts/FI_Level/LVTS/",
          forecastDate,"/", sep = ""));
    dataFrequencyColumnName <- "Year";
    nSPeriods <- 1;
  #if daily   
}else if(dataFrequency == "daily"){
  forecastHorizen  <- (forecastEndYear - endYear)*250; ##this has to be fixed to allow leap years
  extractedLVTSFITimeSeriesLoadPath <- 
    c(paste("C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/ExtractedTimeSeriesData/LVTSFIDailyTimeSeriesData",
            startYear,"-",endYear,".RData", sep = "", collapse = NULL));
  forecastsSavePath <- 
  c(paste("C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/DailyForecasts/FI_Level/LVTS/",
          forecastDate, "/",sep = ""));
  
    dataFrequencyColumnName <- "date";
    nSPeriods <- 250;
}

####<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<RESULTS SAVE PATH DIRECTORY>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
dir.create(forecastsSavePath);###Create the directory in which all analysis results and charts will be saved

##<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<START DATA/STATISTICAL ANALYSIS>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
##<<<<<<<<<<<<<<<<<<<<<<<<<LOAD DATA>>>>>>>>>>>>>>>>>>>>>>>>
##Now load the individual FI's complete data tables
print("Loading data table(s)");#used to record speed of computations
print(now());#used to record speed of computations
load(extractedLVTSFITimeSeriesLoadPath);
##for some reason loading the LVTSFITimeSeriesList from the RData file loads the list as a variable "x".
##this if-statement is used to rename the loaded list from x to the more meaningfull LVTSFITimeSeriesList
if(exists("x")){ #check if variable x exists
  LVTSFITimeSeriesList <- x; #if x exists, create a new variable LVTSFITimeSeriesList and set its value to the value of x
#  rm(x);#remove the default system named x list
}



gc();

##<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<START DATA/STATISTICAL ANALYSIS>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
###Load used functions
gc();
print("Loading Used Functions");
source('C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/lvts_timeSeries_analysis_functions_V02.R', encoding = 'UTF-8', echo=TRUE);
gc();

gc();
if(modelSelection=="ARIMA"){
  print("LVTS ARIMA Timeseries Forecasts Generation");
  source('C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/lvts_timeSeries_analysis_ARIMA_V01.R', encoding = 'UTF-8', echo=TRUE);
  gc();
}else if(modelSelection=="SARIMA"){
  print("LVTS SARIMA Timeseries Forecasts Generation");
  source('C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/lvts_timeSeries_analysis_SARIMA_V01.R', encoding = 'UTF-8', echo=TRUE);
  gc();
}



gc();

