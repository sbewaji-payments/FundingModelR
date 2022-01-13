
##<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<START DATA/STATISTICAL ANALYSIS>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
##<<<<<<<<<<<<<<<<<<<<<<<<<LOAD DATA>>>>>>>>>>>>>>>>>>>>>>>>
##Now load the individual FI's complete data tables
print("Loading data table(s)");#used to record speed of computations
print(now());#used to record speed of computations
load(extractedACSSUSBEFITimeSeriesLoadPath);
load(ACSSUSBEFITimeSeriesMissingDataDummyLoadPath);

##for some reason loading the ACSSUSBEFITimeSeriesList from the RData file loads the list as a variable "x".
##this if-statement is used to rename the loaded list from x to the more meaningfull ACSSUSBEFITimeSeriesList
if(exists("x")){ #check if variable x exists
  ACSSUSBEFITimeSeriesList <- x; #if x exists, create a new variable ACSSUSBEFITimeSeriesList and set its value to the
  # value of x
  #  rm(x);#remove the default system named x list
  rm(x);
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
if(dataFrequency == "monthly"){
  timeSeriesARIMAForecastsACSSUSBE <- generateARIMAForecasts(timeSeriesDecompositionACSSUSBE, usbeMonthlyMissingDataDummyTS,
                                                             arimaOrder, forecastHorizen, arimaConstant, arimaDrift, 
                                                             confidenceLevel);
  }else if(dataFrequency == "quarterly"){
    if(includeDummy==TRUE){timeSeriesARIMAForecastsACSSUSBE <- generateARIMAForecasts(timeSeriesDecompositionACSSUSBE, 
                                                                                q42007MissingDataDummyTS, arimaOrder,
                                                                                forecastHorizen, arimaConstant, 
                                                                                arimaDrift, confidenceLevel);
    }else{
      timeSeriesARIMAForecastsACSSUSBE <- generateARIMAForecasts(timeSeriesDecompositionACSSUSBE, arimaOrder, 
                                                                 forecastHorizen, arimaConstant, arimaDrift, 
                                                                 confidenceLevel);
      }
  }else if(dataFrequency == "annual"){
  
}else{
  
}
gc();
#the following method/function call is used to remove forecasts beyond the forecast horizen. This is done because by 
#default including the dummy 
#variable into the ARIMA model generates more point forecasts than required
#the function uses while loops which are not efficient
timeSeriesARIMAForecastsACSSUSBE <- removeForecastsAfterHorizen(timeSeriesARIMAForecastsACSSUSBE, forecastHorizen)
gc();
dtListFILevelARIMAForecastsACSSUSBE <- exportListOfForecastsToExcel(timeSeriesARIMAForecastsACSSUSBE, measurementUnitType, 
                                                                    forecastsSavePath);
dtListFILevelARIMAForecastErrorsACSSUSBE <- exportListOfForecastErrorsToExcel(timeSeriesARIMAForecastsACSSUSBE, measurementUnitType, 
                                                                          forecastsSavePath);

##Save forecast errors into data table for later use in benchmarking
save(dtListFILevelARIMAForecastErrorsACSSUSBE, file=forecastsErrorsSavePath);

dtListFILevelARIMACoefficientsACSSUSBE <- exportListOfForecastModelCoefficientsToExcel(timeSeriesARIMAForecastsACSSUSBE, 
                                                                                       measurementUnitType, 
                                                                                       forecastsSavePath);

chartListFILevelARIMAForecastACSSUSBE <- 
  exportForecastChartList(timeSeriesARIMAForecastsACSSUSBE, dataFrequency, measurementUnit, measurementUnitType, 
                          forecastsSavePath, forecastEndYear, forecastHorizen, chartRangeMultiplier);

gc();
seasonallyAdjustedTimeSeriesPlotsACSSUSBE <- 
  createSeasonallyAdjustedTSPlots(ACSSUSBEFITimeSeriesList, measurementUnit, measurementUnitType, dataFrequency, 
                                  startYear, endYear, forecastsSavePath);

gc();

