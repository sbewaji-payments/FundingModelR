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
load(extractedACSSUSBEFITimeSeriesLoadPath);
##for some reason loading the ACSSUSBEFITimeSeriesList from the RData file loads the list as a variable "x".
##this if-statement is used to rename the loaded list from x to the more meaningfull ACSSUSBEFITimeSeriesList
if(exists("x")){ #check if variable x exists
  ACSSUSBEFITimeSeriesList <- x; #if x exists, create a new variable ACSSUSBEFITimeSeriesList and set its value to the value of x
#  rm(x);#remove the default system named x list
}



gc();
print("data table(s) loaded");#used to record speed of computations
print(now());#used to record speed of computations
gc();

timeSeriesPlotsACSSUSBE <- createTSPlots(ACSSUSBEFITimeSeriesList, measurementUnit, measurementUnitType, dataFrequency, 
                                     startYear, endYear);
timeSeriesDecompositionPlotsACSSUSBE <- 
  createTSDecompositionPlots(ACSSUSBEFITimeSeriesList, measurementUnit, measurementUnitType, dataFrequency, startYear, 
                             endYear, forecastsSavePath);
timeSeriesPACFPlotsACSSUSBE <- createTSPACFPlots(ACSSUSBEFITimeSeriesList, measurementUnit);
timeSeriesACFPlotsACSSUSBE <- createTSACFPlots(ACSSUSBEFITimeSeriesList, measurementUnit);
timeSeriesDecompositionACSSUSBE <- createTSDecomposition(ACSSUSBEFITimeSeriesList, measurementUnit);
timeSeriesSARIMAForecastsACSSUSBE <- generateSARIMAForecasts(timeSeriesDecompositionACSSUSBE, arimaOrder, seasonalOrder, nSPeriods,
                                                        forecastHorizen, arimaConstant, arimaDrift, confidenceLevel);
dtListFILevelSARIMAForecastsACSSUSBE <- exportListOfForecastsToExcel(timeSeriesSARIMAForecastsACSSUSBE, measurementUnitType, 
                                                                 forecastsSavePath);
dtListFILevelSARIMACoefficientsACSSUSBE <- exportListOfForecastModelCoefficientsToExcel(timeSeriesSARIMAForecastsACSSUSBE, 
                                                                                    measurementUnitType, forecastsSavePath);
chartListFILevelSARIMAForecastACSSUSBE <- 
  exportForecastChartList(timeSeriesARIMAForecastsACSSUSBE, dataFrequency, measurementUnit, 
                          measurementUnitType, forecastsSavePath);
gc();
seasonallyAdjustedTimeSeriesPlotsACSSUSBE <- 
  createSeasonallyAdjustedTSPlots(ACSSUSBEFITimeSeriesList, measurementUnit, measurementUnitType, 
                                  dataFrequency, startYear, endYear, forecastsSavePath);

gc();

