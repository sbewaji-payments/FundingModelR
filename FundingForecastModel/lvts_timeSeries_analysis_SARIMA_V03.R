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
# print(now());#used to record speed of computations
# load(extractedLVTSFITimeSeriesLoadPath);
# ##for some reason loading the LVTSFITimeSeriesList from the RData file loads the list as a variable "x".
# ##this if-statement is used to rename the loaded list from x to the more meaningfull LVTSFITimeSeriesList
# if(exists("x")){ #check if variable x exists
#   LVTSFITimeSeriesList <- x; #if x exists, create a new variable LVTSFITimeSeriesList and set its value to the value of x
# #  rm(x);#remove the default system named x list
# }



gc();
print("data table(s) loaded");#used to record speed of computations
print(now());#used to record speed of computations
gc();


if(ExistingProductionModel){
  timeSeriesPlotsLVTS <- createTSPlots(LVTSFITimeSeriesList, measurementUnit, measurementUnitType, dataFrequency, 
                                       startYear, endYear);
  timeSeriesDecompositionPlotsLVTS <- 
    createTSDecompositionPlots(LVTSFITimeSeriesList, measurementUnit, measurementUnitType, dataFrequency, startYear, 
                               endYear, forecastsSavePath);
  timeSeriesPACFPlotsLVTS <- createTSPACFPlots(LVTSFITimeSeriesList, measurementUnit);
  timeSeriesACFPlotsLVTS <- createTSACFPlots(LVTSFITimeSeriesList, measurementUnit);
  timeSeriesDecompositionLVTS <- createTSDecomposition(LVTSFITimeSeriesList, measurementUnit);
  timeSeriesSARIMAForecastsLVTS <- generateSARIMAForecasts(LVTSFITimeSeriesList, arimaOrder, seasonalOrder, nSPeriods, 
                                                           forecastHorizen, sarimaConstant, sarimaDrift, confidenceLevel);
  dtListFILevelSARIMAForecastsLVTS <- exportListOfForecastsToExcel(timeSeriesSARIMAForecastsLVTS, measurementUnitType, 
                                                                   forecastsSavePath);
  dtListFILevelSARIMAForecastErrorsLVTS <- exportListOfForecastErrorsToExcel(timeSeriesSARIMAForecastsLVTS, 
                                                                             measurementUnitType, forecastsSavePath);
  
  ##Save forecast errors into data table for later use in benchmarking
  save(dtListFILevelSARIMAForecastErrorsLVTS, file=forecastsErrorsSavePath);
  
  dtListFILevelSARIMACoefficientsLVTS <- exportListOfForecastModelCoefficientsToExcel(timeSeriesSARIMAForecastsLVTS, 
                                                                                      measurementUnitType, forecastsSavePath);
  chartListFILevelSARIMAForecastLVTS <- 
    exportForecastChartList(timeSeriesSARIMAForecastsLVTS, dataFrequency, measurementUnit, measurementUnitType, 
                            forecastsSavePath);
  gc();
  seasonallyAdjustedTimeSeriesPlotsLVTS <- 
    createSeasonallyAdjustedTSPlots(LVTSFITimeSeriesList, measurementUnit, measurementUnitType, dataFrequency, 
                                    startYear, endYear, forecastsSavePath);
  
  gc();
  
} else {
  if(measurementUnitType == "Value"){
    timeSeriesPlotsLVTS <- createTSPlots(LVTSFITimeSeriesList, measurementUnit, measurementUnitType, dataFrequency, 
                                         startYear, endYear);
    timeSeriesDecompositionPlotsLVTS <- 
      createTSDecompositionPlots(LVTSFITimeSeriesList, measurementUnit, measurementUnitType, dataFrequency, startYear, 
                                 endYear, forecastsValueSavePath);
    timeSeriesPACFPlotsLVTS <- createTSPACFPlots(LVTSFITimeSeriesList, measurementUnit);
    timeSeriesACFPlotsLVTS <- createTSACFPlots(LVTSFITimeSeriesList, measurementUnit);
    timeSeriesDecompositionLVTS <- createTSDecomposition(LVTSFITimeSeriesList, measurementUnit);
    timeSeriesSARIMAForecastsLVTS <- generateSARIMAForecasts(LVTSFITimeSeriesList, arimaOrder, seasonalOrder, nSPeriods, 
                                                             forecastHorizen, sarimaConstant, sarimaDrift, confidenceLevel);
    dtListFILevelSARIMAForecastsLVTS <- exportListOfForecastsToExcel(timeSeriesSARIMAForecastsLVTS, measurementUnitType, 
                                                                     forecastsValueSavePath);
    dtListFILevelSARIMAForecastErrorsLVTS <- exportListOfForecastErrorsToExcel(timeSeriesSARIMAForecastsLVTS, 
                                                                               measurementUnitType, forecastsValueSavePath);
    
    ##Save forecast errors into data table for later use in benchmarking
    save(dtListFILevelSARIMAForecastErrorsLVTS, file=forecastsValueErrorsSavePath);
    
    dtListFILevelSARIMACoefficientsLVTS <- exportListOfForecastModelCoefficientsToExcel(timeSeriesSARIMAForecastsLVTS, 
                                                                                        measurementUnitType, forecastsValueSavePath);
    chartListFILevelSARIMAForecastLVTS <- 
      exportForecastChartList(timeSeriesSARIMAForecastsLVTS, dataFrequency, measurementUnit, measurementUnitType, 
                              forecastsValueSavePath);
    gc();
    seasonallyAdjustedTimeSeriesPlotsLVTS <- 
      createSeasonallyAdjustedTSPlots(LVTSFITimeSeriesList, measurementUnit, measurementUnitType, dataFrequency, 
                                      startYear, endYear, forecastsValueSavePath);
    
    gc();
    
  } else if(measurementUnitType == "Volume"){
    timeSeriesPlotsLVTS <- createTSPlots(LVTSFITimeSeriesList, measurementUnit, measurementUnitType, dataFrequency, 
                                         startYear, endYear);
    timeSeriesDecompositionPlotsLVTS <- 
      createTSDecompositionPlots(LVTSFITimeSeriesList, measurementUnit, measurementUnitType, dataFrequency, startYear, 
                                 endYear, forecastsVolumeSavePath);
    timeSeriesPACFPlotsLVTS <- createTSPACFPlots(LVTSFITimeSeriesList, measurementUnit);
    timeSeriesACFPlotsLVTS <- createTSACFPlots(LVTSFITimeSeriesList, measurementUnit);
    timeSeriesDecompositionLVTS <- createTSDecomposition(LVTSFITimeSeriesList, measurementUnit);
    timeSeriesSARIMAForecastsLVTS <- generateSARIMAForecasts(LVTSFITimeSeriesList, arimaOrder, seasonalOrder, nSPeriods, 
                                                             forecastHorizen, sarimaConstant, sarimaDrift, confidenceLevel);
    dtListFILevelSARIMAForecastsLVTS <- exportListOfForecastsToExcel(timeSeriesSARIMAForecastsLVTS, measurementUnitType, 
                                                                     forecastsVolumeSavePath);
    dtListFILevelSARIMAForecastErrorsLVTS <- exportListOfForecastErrorsToExcel(timeSeriesSARIMAForecastsLVTS, 
                                                                               measurementUnitType, forecastsVolumeSavePath);
    
    ##Save forecast errors into data table for later use in benchmarking
    save(dtListFILevelSARIMAForecastErrorsLVTS, file=forecastsVolumeErrorsSavePath);
    
    dtListFILevelSARIMACoefficientsLVTS <- exportListOfForecastModelCoefficientsToExcel(timeSeriesSARIMAForecastsLVTS, 
                                                                                        measurementUnitType, forecastsVolumeSavePath);
    chartListFILevelSARIMAForecastLVTS <- 
      exportForecastChartList(timeSeriesSARIMAForecastsLVTS, dataFrequency, measurementUnit, measurementUnitType, 
                              forecastsVolumeSavePath);
    gc();
    seasonallyAdjustedTimeSeriesPlotsLVTS <- 
      createSeasonallyAdjustedTSPlots(LVTSFITimeSeriesList, measurementUnit, measurementUnitType, dataFrequency, 
                                      startYear, endYear, forecastsVolumeSavePath);
    
    gc();
    
  }

}

