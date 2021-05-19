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
#require(xts);
require(PerformanceAnalytics);
require(rugarch);
require(fGarch);
require(forecast);



##<<<<<<<<<<<<<<<<<<<<<<<<<DECLARE GLOBAL VARIABLES>>>>>>>>>>>>>>>>>>>>>>>>
##Declare all global variables to be used in this class
startYr <- 2000;
endYr <- 2021;
minObservations <- 12; #minimum number of observations required for the collected time series
keyField <- "sender"; #the colum to be used as the unique key forthe data table (can be "sender" or "reciever")'
onlyACSS <- FALSE;##Determine if the timeseries only looks at ACSS data or if it combines ACSS and USBE trannsactions data
forecastDate <- Sys.Date();
modelSelection <- "GARCH";
measurementUnitType <- "Value";##Volume or Value
if(measurementUnitType == "Volume"){
  measurementUnit <- 1000000; #possible values 1000000000, 1000000, 1000, or 1
}else{
  measurementUnit <- 1000000000; #possible values 1000000000, 1000000, 1000, or 1
}




getwd();

######<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<INPUT DATA SOURCE FILES>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
LVTSFITimeSeriesDataTableSavePath <- 
  c(paste("C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/ExtractedTimeSeriesData/LVTSFIMonthlyTimeSeriesData",
          startYr,"-",endYr,".RData", sep = "", collapse = NULL));

ACSSFITimeSeriesDataTableSavePath <- 
  c(paste("C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/ExtractedTimeSeriesData/ACSSFIMonthlyTimeSeriesData",
          startYr,"-",endYr,".RData", sep = "", collapse = NULL));


ACSSUSBEFITimeSeriesDataTableSavePath <- 
  c(paste("C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/ExtractedTimeSeriesData/ACSSUSBEFIMonthlyTimeSeriesData",
          startYr,"-",endYr,".RData", sep = "", collapse = NULL));


ACSSUSBEValueFITimeSeriesDataTableSavePath <- 
  c(paste("C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/ExtractedTimeSeriesData/ACSSFIMonthlyValueTimeSeriesData",
          startYr,"-",endYr,".RData", sep = "", collapse = NULL));

######<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<END INPUT DATA SOURCE FILES>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>



######<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<OUTPUT DATA SOURCE FILES>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

forecastsSavePathACSSUSBEMainDir <- 
  c(paste("C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/VolotilityForecasts/ACSS_USBE/",
          measurementUnitType,"/",forecastDate,sep = ""));

forecastsSavePathLVTSMainDir <- 
  c(paste("C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/VolotilityForecasts/LVTS/",
          measurementUnitType,"/",forecastDate,sep = ""));


forecastsSavePathSubDir <- 
  c(paste(modelSelection,sep = ""));
forecastsSavePathFileLevelSubDir <- 
  c(paste(startYr,"-",endYr,sep = ""));


modelSavePathSubDirACSSUSBE <- 
  c(paste(modelSelection,"/",sep = ""));

modelSavePathSubDirLVTS <- 
  c(paste(modelSelection,"/",sep = ""));

forecastsSavePathSubDirACSSUSBE <- 
  c(paste(forecastsSavePathFileLevelSubDir,"/",sep = ""));

forecastsSavePathSubDirLVTS <- 
  c(paste(forecastsSavePathFileLevelSubDir,"/",sep = ""));

forecastsFileSavePathACSSUSBE <- 
  c(paste(forecastsSavePathACSSUSBEMainDir,"/",modelSelection,"/",
          forecastsSavePathSubDirACSSUSBE,"VolatilityForecastACSSUSBE.csv",sep = ""));

forecastsFileSavePathLVTS <- 
  c(paste(forecastsSavePathLVTSMainDir,"/",modelSelection,"/",
          forecastsSavePathSubDirLVTS,"VolatilityForecastLVTS.csv",sep = ""));

######<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<END OUTPUT DATA SOURCE FILES>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

####<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<CREATE RESULTS SAVE PATH DIRECTORY>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
##Save path must be broken up to allow R to create directories. It cannot create full directory paths of 
##main directory and sub directory in one command
dir.create(file.path(forecastsSavePathACSSUSBEMainDir));###Create the directory in which all analysis results and charts will be saved
dir.create(file.path(forecastsSavePathLVTSMainDir));###Create the directory in which all analysis results and charts will be saved

##Save path must be broken up to allow R to create directories. It cannot create full directory paths of 
##main directory and sub directory in one command
dir.create(file.path(forecastsSavePathACSSUSBEMainDir,modelSavePathSubDirACSSUSBE));###Create the directory in which all analysis results and charts will be saved
dir.create(file.path(forecastsSavePathLVTSMainDir,modelSavePathSubDirLVTS));###Create the directory in which all analysis results and charts will be saved

##Save path must be broken up to allow R to create directories. It cannot create full directory paths of 
##main directory and sub directory in one command
dir.create(file.path(forecastsSavePathACSSUSBEMainDir,modelSavePathSubDirACSSUSBE,forecastsSavePathSubDirACSSUSBE));###Create the directory in which all analysis results and charts will be saved
dir.create(file.path(forecastsSavePathLVTSMainDir,modelSavePathSubDirLVTS,forecastsSavePathSubDirLVTS));###Create the directory in which all analysis results and charts will be saved




######<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<LOAD INPUT DATA SOURCE FILES>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
load(LVTSFITimeSeriesDataTableSavePath);
LVTSFITimeSeriesDataTable <- x;
if(onlyACSS){
  load(ACSSFITimeSeriesDataTableSavePath);
  ACSSUSBEFITimeSeriesDataTable <- x;
}else{
  load(ACSSUSBEFITimeSeriesDataTableSavePath);
  ACSSUSBEFITimeSeriesDataTable <- x;
}
######<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<END LOAD INPUT DATA SOURCE FILES>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>



##Compute portfolio level returns
if(onlyACSS){
  ACSSUSBEFITimeSeriesDataTableDeltaAll <- 
  na.omit(CalculateReturns(ACSSUSBEFITimeSeriesDataTable[length(ACSSUSBEFITimeSeriesDataTable)]$ACSSFITimeSeriesList$TimeSeries, 
                           method ="discrete"));
}else{
  ACSSUSBEFITimeSeriesDataTableDeltaAll <- 
    na.omit(CalculateReturns(ACSSUSBEFITimeSeriesDataTable[length(ACSSUSBEFITimeSeriesDataTable)]$ACSSUSBEFITimeSeriesList$TimeSeries, 
                             method ="discrete"));
  
}

LVTSFITimeSeriesDataTableDeltaAll <- 
  na.omit(CalculateReturns(LVTSFITimeSeriesDataTable[length(LVTSFITimeSeriesDataTable)]$LVTSFITimeSeriesList$TimeSeries, 
                           method ="discrete"));


ACSSUSBEFITimeSeriesDataTableDeltaAll[!is.na(index(ACSSUSBEFITimeSeriesDataTableDeltaAll))];

LVTSFITimeSeriesDataTableDeltaAll[!is.na(index(LVTSFITimeSeriesDataTableDeltaAll))];

plot(ACSSUSBEFITimeSeriesDataTableDeltaAll,type="l");

plot(LVTSFITimeSeriesDataTableDeltaAll,type="l");

##compute standard deviation/volatility
sd(ACSSUSBEFITimeSeriesDataTableDeltaAll, na.rm=TRUE);
sd(LVTSFITimeSeriesDataTableDeltaAll, na.rm=TRUE);

##Annual standard deviation/volatility
12*sd(ACSSUSBEFITimeSeriesDataTableDeltaAll, na.rm=TRUE);
12*sd(LVTSFITimeSeriesDataTableDeltaAll, na.rm=TRUE);


chart.RollingPerformance(R = ACSSUSBEFITimeSeriesDataTableDeltaAll, 
                         width = 2,
                         FUN = "sd.annualized",
                         scale = 24,
                         main = "Rolling 1 month volatility");


chart.RollingPerformance(R = LVTSFITimeSeriesDataTableDeltaAll, 
                         width = 2,
                         FUN = "sd.annualized",
                         scale = 24,
                         main = "Rolling 1 month volatility");

ACSSUSBEGARCH=garchFit(formula=~arma(1,1)+garch(1,1),data=ACSSUSBEFITimeSeriesDataTableDeltaAll,trace=F);
#summary(ACSSUSBEGARCH);
LVTSGARCH=garchFit(formula=~arma(1,1)+garch(1,1),data=LVTSFITimeSeriesDataTableDeltaAll,trace=F);
#summary(LVTSGARCH);
#m2=garchFit(formula=~garch(1,1),data=ACSSUSBEFITimeSeriesDataTableDeltaAll,trace=F,cond.dist="std");
#summary(m2);
stresiACSSUSBEGARCH=residuals(ACSSUSBEGARCH,standardize=T);
plot(stresiACSSUSBEGARCH,type="l");
Box.test(stresiACSSUSBEGARCH,10,type="Ljung");

stresiLVTSGARCH=residuals(LVTSGARCH,standardize=T);
plot(stresiLVTSGARCH,type="l");
Box.test(stresiLVTSGARCH,10,type="Ljung");

#stresiM2=residuals(m2,standardize=T);
#plot(stresiM2,type="l");
#Box.test(stresiM2,10,type="Ljung");
#predict(m2,60);
predict(ACSSUSBEGARCH, n.ahead=60, mse='cond', plot=T);
predict(LVTSGARCH, n.ahead=60, mse='cond', plot=T);

ACSSUSBEGARCHForecast <- predict(ACSSUSBEGARCH, n.ahead=60, mse='cond', plot=T);
LVTSGARCHForecast <- predict(LVTSGARCH, n.ahead=60, mse='cond', plot=T);

##Export forecasts to csv files
write.csv(ACSSUSBEGARCHForecast, file=forecastsFileSavePathACSSUSBE);
write.csv(LVTSGARCHForecast, file=forecastsFileSavePathLVTS);
