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
##Make the list of packages to be used in this script
usedPackagesList <- c("lubridate","dplyr","data.table","ggplot2","magrittr","scales","moments","gghighlight",
                      "rlist","foreign","zoo","xts","xlsx","gridExtra","grid","lattice",
                      "ggrepel", "latticeExtra", "forecast", "tseries", "timeSeries", "astsa","TSA"
);

##check to see if required packages are installed.
#if not install and load them them
##else looad them
checkPagagesInstalled <- lapply(
  usedPackagesList,
  FUN = function(x){
    if(!require(x,character.only = TRUE)){
      install.packages(x, dependencies = TRUE);
      require(x,character.only = TRUE)
    }
  }
);



##<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<DECLARE METHODS/FUNCTIONS>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
##This function removes time series entries from a list of time series objects where some end
##period condition for the data is not met
##i.e. if end(ts)< specified end period


##<<<<<<<<<<<<<<<<<<<<<<<<<DECLARE GLOBAL VARIABLES>>>>>>>>>>>>>>>>>>>>>>>>
##Declare all global variables to be used in this class

startYear <- 2001;
endYear <- 2021;
forecastEndYear <- 2029;
measurementUnitType <- "Volume";##Volume or Value
if(measurementUnitType == "Volume"){
  measurementUnit <- 1000000; #possible values 1000000000, 1000000, 1000, or 1
}else{
  measurementUnit <- 1000000000; #possible values 1000000000, 1000000, 1000, or 1
}
dataFrequency <- "quarterly"; #the options are monthly, quarterly, annual, daily (does not make sense since it does not distingush the year)
forecastDate <- Sys.Date();##Set date the forecast was run on. This will be used to save the forecast outups to prevent
##overwriting previous results
forecastHorizen  <- 12; ##coincides with 3yrs of quarterly forecasts (Finance Corpoerate Funding Model) and 1yr of monthly forecasts (COO Dashboard)
confidenceLevel <- 95 #c(80,95)


arimaOrder <- c(1,0,1); 
arimaDrift <- TRUE;
arimaConstant <- TRUE;
includeDummy <- FALSE;

seasonalOrder <- c(0,1,2); ##Seasonal order component for SARIMA model
nSPeriods <- 1; ##number of periods in seasonal model component
modelSelection <- "ARIMA";##can take the form SARIMA, ARIMA, etc
sarimaDrift <- FALSE;
sarimaConstant <- FALSE;



keyField <- "sender"; #the colum to be used as the unique key forthe data table (can be "sender" or "reciever")'
chartRangeMultiplier <- 0.95;
dataFrequencyColumnName <- NULL; #used to capture the name of the time colum for use in the time series generation method/function.
#dataFrequencyColumnName can take values "Year_Month", "Year_Quarter", "Year" "date" and will be set automatically 
#based on the dataFrequency
extractedACSSUSBEFITimeSeriesLoadPath <- NULL; #used to store the location of the RData file from which the time sereis are to be generate
timeSeriesPlotsACSSUSBE <- NULL;
timeSeriesDecompositionPlotsACSSUSBE <- NULL;
timeSeriesPACFPlotsACSSUSBE <- NULL;
timeSeriesACFPlotsACSSUSBE <- NULL;
timeSeriesDecompositionACSSUSBE <- NULL;
timeSeriesARIMAForecastsACSSUSBE <- NULL;
dtListFILevelARIMAForecastsACSSUSBE <- NULL;
dtListFILevelARIMACoefficientsACSSUSBE <- NULL;
chartListFILevelARIMAForecastACSSUSBE <- NULL;
seasonallyAdjustedTimeSeriesPlotsACSSUSBE <- NULL;
forecastsSavePath <- NULL;



getwd();
#will load the data table: ACSSUSBETransDataSeriesSentRecieved
#and set the name of the time column (dataFrequencyColumnName)
#if monthly
if(dataFrequency == "monthly"){  
  forecastHorizen  <- (forecastEndYear - endYear)*12;
    if(measurementUnitType == "Volume"){
      extractedACSSUSBEFITimeSeriesLoadPath <- 
        c(paste("C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/ExtractedTimeSeriesData/ACSSUSBEFIMonthlyTimeSeriesData",
                startYear,"-",endYear,".RData", sep = "", collapse = NULL));
      # forecastsSavePath <- 
      #   c(paste("C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/MonthlyForecasts/FI_Level/ACSS_USBE/",
      #           forecastDate, "/",sep = ""));
      ##Save path must be broken up to allow R to create directories. It cannot create full directory paths of 
      ##main directory and sub directory in one command
      forecastsSavePathMainDir <- 
        c(paste("C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/MonthlyForecasts/FI_Level/ACSS_USBE/Volume/",
                forecastDate,sep = ""));
      # forecastsErrorsSavePath <- 
      #   c(paste0("C:/Users/sbewaji/Documents/ModelResults/FundingForecastModel/MonthlyForecasts/FI_Level/ACSS_USBE/",
      #           forecastDate, "/","BenchMarking","/", modelSelection,"ForecastErrorsACSSUSBE.RData", sep = ""));
      
      ##Save path must be broken up to allow R to create directories. It cannot create full directory paths of 
      ##main directory and sub directory in one command
      forecastsErrorsSavePathMainDir<- 
        c(paste("C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/MonthlyForecasts/FI_Level/ACSS_USBE/Volume/",
                forecastDate, "/","Model Benchmarking","/", sep = ""));

    }else{
      extractedACSSUSBEFITimeSeriesLoadPath <- 
        c(paste("C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/ExtractedTimeSeriesData/ACSSUSBEFIMonthlyValueTimeSeriesData",
                startYear,"-",endYear,".RData", sep = "", collapse = NULL));
      
      forecastsSavePathMainDir <- 
        c(paste("C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/MonthlyForecasts/FI_Level/ACSS_USBE/Value/",
                forecastDate,sep = ""));
      
      forecastsErrorsSavePathMainDir<- 
        c(paste("C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/MonthlyForecasts/FI_Level/ACSS_USBE/Value/",
                forecastDate, "/","Model Benchmarking","/", sep = ""));
    }
    ACSSUSBEFITimeSeriesMissingDataDummyLoadPath<- 
      c("C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/ExtractedTimeSeriesData/ACSSUSBEFIMonthlyTSMissingDataDummy.RData");
    
    dataFrequencyColumnName <- "Year_Month";
    nSPeriods <- 12;
    #if quarterly  
  }else if(dataFrequency == "quarterly"){
    forecastHorizen  <- (forecastEndYear - endYear)*4;
    
    if(measurementUnitType == "Volume"){
      extractedACSSUSBEFITimeSeriesLoadPath <- 
        c(paste("C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/ExtractedTimeSeriesData/ACSSUSBEFIQuarterlyTimeSeriesData",
                startYear,"-",endYear,".RData", sep = "", collapse = NULL));
      # forecastsSavePath <- 
      # c(paste("C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/QuarterlyForecasts/FI_Level/ACSS_USBE/",
      #         forecastDate, "/",sep = ""));
      
      ##Save path must be broken up to allow R to create directories. It cannot create full directory paths of 
      ##main directory and sub directory in one command
      forecastsSavePathMainDir <- 
        c(paste("C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/QuarterlyForecasts/FI_Level/ACSS_USBE/Volume/",
                forecastDate,sep = ""));
      
      # forecastsErrorsSavePath <- 
      #   c(paste0("C:/Users/sbewaji/Documents/ModelResults/FundingForecastModel/QuarterlyForecasts/FI_Level/ACSS_USBE/",
      #           forecastDate, "/","BenchMarking","/", modelSelection,"ForecastErrorsACSSUSBE.RData", sep = ""));
      
      ##Save path must be broken up to allow R to create directories. It cannot create full directory paths of 
      ##main directory and sub directory in one command
      forecastsErrorsSavePathMainDir<- 
        c(paste("C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/QuarterlyForecasts/FI_Level/ACSS_USBE/Volume/",
                forecastDate, "/","Model Benchmarking","/", sep = ""));
    }else{
      extractedACSSUSBEFITimeSeriesLoadPath <- 
        c(paste("C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/ExtractedTimeSeriesData/ACSSUSBEFIQuarterlyValueTimeSeriesData",
                startYear,"-",endYear,".RData", sep = "", collapse = NULL));
      forecastsSavePathMainDir <- 
        c(paste("C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/QuarterlyForecasts/FI_Level/ACSS_USBE/Value/",
                forecastDate,sep = ""));
      forecastsErrorsSavePathMainDir<- 
        c(paste("C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/QuarterlyForecasts/FI_Level/ACSS_USBE/Value/",
                forecastDate, "/","Model Benchmarking","/", sep = ""));
    }
    
    ACSSUSBEFITimeSeriesMissingDataDummyLoadPath <- 
      c("C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/ExtractedTimeSeriesData/ACSSUSBEFIQuarterlyTSMissingDataDummy.RData");
    
    dataFrequencyColumnName <- "Year_Quarter";
    nSPeriods <- 4;
    #if annual 
  }else if(dataFrequency == "annual"){
    forecastHorizen  <- (forecastEndYear - endYear);
    if(measurementUnitType == "Volume"){
      extractedACSSUSBEFITimeSeriesLoadPath <- 
        c(paste("C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/ExtractedTimeSeriesData/ACSSUSBEFIAnnualTimeSeriesData",
                startYear,"-",endYear,".RData", sep = "", collapse = NULL));
      # forecastsSavePath <- 
      # c(paste("C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/AnnualForecasts/FI_Level/ACSS_USBE/",
      #         forecastDate,"/", sep = ""));
      
      ##Save path must be broken up to allow R to create directories. It cannot create full directory paths of 
      ##main directory and sub directory in one command
      forecastsSavePathMainDir <- 
        c(paste("C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/AnnualForecasts/FI_Level/ACSS_USBE/Volume/",
                forecastDate,sep = ""));
      # forecastsErrorsSavePath <- 
      #   c(paste0("C:/Users/sbewaji/Documents/ModelResults/FundingForecastModel/AnnualForecasts/FI_Level/ACSS_USBE/",
      #           forecastDate, "/","BenchMarking","/", modelSelection,"ForecastErrorsACSSUSBE.RData", sep = ""));
      
      ##Save path must be broken up to allow R to create directories. It cannot create full directory paths of 
      ##main directory and sub directory in one command
      forecastsErrorsSavePathMainDir<- 
        c(paste("C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/AnnualForecasts/FI_Level/ACSS_USBE/Volume/",
                forecastDate, "/","Model Benchmarking","/", sep = ""));
    }else{
      extractedACSSUSBEFITimeSeriesLoadPath <- 
        c(paste("C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/ExtractedTimeSeriesData/ACSSUSBEFIAnnualValueTimeSeriesData",
                startYear,"-",endYear,".RData", sep = "", collapse = NULL));
      forecastsSavePathMainDir <- 
        c(paste("C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/AnnualForecasts/FI_Level/ACSS_USBE/Value/",
                forecastDate,sep = ""));
      forecastsErrorsSavePathMainDir<- 
        c(paste("C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/AnnualForecasts/FI_Level/ACSS_USBE/Value/",
                forecastDate, "/","Model Benchmarking","/", sep = ""));
    }
    ACSSUSBEFITimeSeriesMissingDataDummyLoadPath <- 
      c("C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/ExtractedTimeSeriesData/ACSSUSBEFIAnnualTSMissingDataDummy.RData");
    
    dataFrequencyColumnName <- "Year";
    nSPeriods <- 1;
    #if daily   
  }else if(dataFrequency == "daily"){
    forecastHorizen  <- (forecastEndYear - endYear)*250; ##this has to be fixed to allow leap years
    if(measurementUnitType == "Volume"){
      extractedACSSUSBEFITimeSeriesLoadPath <- 
        c(paste("C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/ExtractedTimeSeriesData/ACSSUSBEFIDailyTimeSeriesData",
                startYear,"-",endYear,".RData", sep = "", collapse = NULL));
      # forecastsSavePath <- 
      #   c(paste("C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/DailyForecasts/FI_Level/ACSS_USBE/",
      #           forecastDate, "/",sep = ""));
      ##Save path must be broken up to allow R to create directories. It cannot create full directory paths of 
      ##main directory and sub directory in one command
      forecastsSavePathMainDir <- 
        c(paste("C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/DailyForecasts/FI_Level/ACSS_USBE/Volume/",
                forecastDate,sep = ""));
      # forecastsErrorsSavePath <- 
      #   c(paste0("C:/Users/sbewaji/Documents/ModelResults/FundingForecastModel/DailyForecasts/FI_Level/ACSS_USBE/",
      #           forecastDate, "/","BenchMarking","/", modelSelection,"ForecastErrorsACSSUSBE.RData", sep = ""));
      ##Save path must be broken up to allow R to create directories. It cannot create full directory paths of 
      ##main directory and sub directory in one command
      forecastsErrorsSavePathMainDir<- 
        c(paste("C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/DailyForecasts/FI_Level/ACSS_USBE/Volume/",
                forecastDate, "/","Model Benchmarking","/", sep = ""));
    }else{
      extractedACSSUSBEFITimeSeriesLoadPath <- 
        c(paste("C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/ExtractedTimeSeriesData/ACSSUSBEFIDailyValueTimeSeriesData",
                startYear,"-",endYear,".RData", sep = "", collapse = NULL));
      forecastsSavePathMainDir <- 
        c(paste("C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/DailyForecasts/FI_Level/ACSS_USBE/Value/",
                forecastDate,sep = ""));
      forecastsErrorsSavePathMainDir<- 
        c(paste("C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/DailyForecasts/FI_Level/ACSS_USBE/Value/",
                forecastDate, "/","Model Benchmarking","/", sep = ""));
    }
    ACSSUSBEFITimeSeriesMissingDataDummyLoadPath <- 
      c("C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/ExtractedTimeSeriesData/ACSSUSBEFIDailyTSMissingDataDummyTS.RData");
    dataFrequencyColumnName <- "date";
    nSPeriods <- 250;
  }

forecastsSavePathSubDir <- 
  c(paste(modelSelection,"/",sep = ""));
forecastsSavePath<- 
  c(paste(forecastsSavePathMainDir,"/",modelSelection,"/",sep = ""));
forecastsErrorsSavePath <- 
  c(paste(forecastsErrorsSavePathMainDir, modelSelection,"ForecastErrorsACSSUSBE.RData", sep = ""));



####<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<RESULTS SAVE PATH DIRECTORY>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
##Save path must be broken up to allow R to create directories. It cannot create full directory paths of 
##main directory and sub directory in one command
dir.create(file.path(forecastsSavePathMainDir));###Create the directory in which all analysis results and charts will be saved

##Save path must be broken up to allow R to create directories. It cannot create full directory paths of 
##main directory and sub directory in one command
dir.create(file.path(forecastsSavePathMainDir,forecastsSavePathSubDir));###Create the directory in which all analysis results and charts will be saved

##Save path must be broken up to allow R to create directories. It cannot create full directory paths of 
##main directory and sub directory in one command
dir.create(file.path(forecastsErrorsSavePathMainDir));###Create the directory in which all analysis results and charts will be saved


##<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<START DATA/STATISTICAL ANALYSIS>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
###Load used functions
gc();
print("Loading Used Functions");
source('C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/acssusbe_timeSeries_analysis_functions_V03.R', encoding = 'UTF-8', echo=TRUE);
gc();

###ACSS-USBE Timeseries Analysis
gc();
if(modelSelection=="ARIMA"){
  print("ACSS-USBE ARIMA Timeseries Forecasts Generation");
  source('C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/acssusbe_timeSeries_analysis_ARIMA_V01.R', encoding = 'UTF-8', echo=TRUE);
  gc();
}else if(modelSelection=="SARIMA"){
  print("ACSS-USBE SARIMA Timeseries Forecasts Generation");
  source('C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/acssusbe_timeSeries_analysis_SARIMA_V03.R', encoding = 'UTF-8', echo=TRUE);
  gc();
}
gc();
