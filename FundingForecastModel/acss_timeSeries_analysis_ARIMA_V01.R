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

##<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<START DATA/STATISTICAL ANALYSIS>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
##<<<<<<<<<<<<<<<<<<<<<<<<<LOAD DATA>>>>>>>>>>>>>>>>>>>>>>>>
##Now load the individual FI's complete data tables
print("Loading data table(s)");#used to record speed of computations
print(now());#used to record speed of computations
load(extractedACSSFITimeSeriesLoadPath);
##for some reason loading the ACSSFITimeSeriesList from the RData file loads the list as a variable "x".
##this if-statement is used to rename the loaded list from x to the more meaningfull ACSSFITimeSeriesList
if(exists("x")){ #check if variable x exists
  ACSSFITimeSeriesList <- x; #if x exists, create a new variable ACSSFITimeSeriesList and set its value to the value of x
#  rm(x);#remove the default system named x list
}



gc();
print("data table(s) loaded");#used to record speed of computations
print(now());#used to record speed of computations
gc();

timeSeriesPlotsACSS <- createTSPlots(ACSSFITimeSeriesList, measurementUnit, measurementUnitType, dataFrequency, startYear, endYear);
timeSeriesDecompositionPlotsACSS <- 
  createTSDecompositionPlots(ACSSFITimeSeriesList, measurementUnit, measurementUnitType, dataFrequency, startYear, endYear, forecastsSavePath);
timeSeriesPACFPlotsACSS <- createTSPACFPlots(ACSSFITimeSeriesList, measurementUnit);
timeSeriesACFPlotsACSS <- createTSACFPlots(ACSSFITimeSeriesList, measurementUnit);
timeSeriesDecompositionACSS <- createTSDecomposition(ACSSFITimeSeriesList, measurementUnit);
timeSeriesARIMAForecastsACSS <- generateARIMAForecasts(timeSeriesDecompositionACSS, arimaOrder, forecastHorizen, arimaConstant, arimaDrift, confidenceLevel);
dtListFILevelARIMAForecastsACSS <- exportListOfForecastsToExcel(timeSeriesARIMAForecastsACSS, measurementUnitType, forecastsSavePath);
dtListFILevelARIMACoefficientsACSS <- exportListOfForecastModelCoefficientsToExcel(timeSeriesARIMAForecastsACSS, measurementUnitType, forecastsSavePath);
chartListFILevelARIMAForecastACSS <- 
  exportForecastChartList(timeSeriesARIMAForecastsACSS, dataFrequency, measurementUnit, measurementUnitType, forecastsSavePath);
gc();
seasonallyAdjustedTimeSeriesPlotsACSS <- 
  createSeasonallyAdjustedTSPlots(ACSSFITimeSeriesList, measurementUnit, measurementUnitType, dataFrequency, startYear, endYear, forecastsSavePath);

gc();

