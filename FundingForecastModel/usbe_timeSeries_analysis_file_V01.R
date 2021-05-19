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



##<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<DECLARE METHODS/FUNCTIONS>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
##This function removes time series entries from a list of time series objects where some end
##period condition for the data is not met
##i.e. if end(ts)< specified end period
removeTSItemFromList <- function(inputList, minObserv){
}

##<<<Specify plot settings for the LATTICE plots>>>
graphSettings <- list(#superpose.line = list(lty = 1:3, lwd = 2, alpha = 1),
  superpose.symbol = list(pch = 1:4, alpha = 0.7)) ;


createTSPlots <- function(inputTSList, measUnit, measUnitType, tsFreq, sYr,eYr){
  #declare local variables
  tmpPlot <- NULL;
  tmpPlotList <- NULL;
  measurementUnitTypeString <- NULL;
  measurementUnitString <- NULL;
  tsFreqString <- NULL;
  counter <- 1;
  
  #set the value of the measurment unit as reported in the chart
  if(measUnit==1000000){
    measurementUnitString = "(millions)";
  }else if(measUnit==1000){
    measurementUnitString = "(thousands)";
  }else if(measUnit==1){
    measurementUnitString = "(amount)";
  }else{
    print("wrong measurment unit provided: Please check and re-enter")
  }
  
  #set values for file naming, y-axis labels and main plot title
  if(measUnitType %in% c("volume","Volume","Vol","VOLUME")){
    measurementUnitTypeString = "Transactions Volume";
  }
  
  #set the value of the x-axis label
  #if monthly
  if(dataFrequency == "monthly"){
    tsFreqString <- "Year and Month";
    #if quarterly  
  }else if(dataFrequency == "quarterly"){
    tsFreqString <- "Year and Quarter";
    #if annual 
  }else if(dataFrequency == "annual"){
    extractedUSBEFITimeSeriesLoadPath <- 
      tsFreqString <- "Year";
    #if daily   
  }else if(dataFrequency == "daily"){
    extractedUSBEFITimeSeriesLoadPath <- 
      tsFreqString <- "date";
  }
  
  while(counter <= length(inputTSList)){
    #create the lattice plot
    tmpPlot <- xyplot.ts(na.interp(inputTSList[counter]$USBEFITimeSeriesList$TimeSeries/measUnit), 
                         screens = list(inputTSList[counter]$USBEFITimeSeriesList$FinancialInstitution),
                         superpose = TRUE, xlim = extendrange(sYr:eYr),par.settings = graphSettings, 
                         scales=list(cex=0.7, tck=c(1,0), y=list(tick.number=10, rot=360), x=list(tick.number=15)), 
                         xlab = paste(tsFreqString), ylab =paste(measurementUnitTypeString, measurementUnitString, sep = " "));
    #add the lattice plot to the list of plots
    #This has been made to reference the FI name as a primary key to prevent having to loop through every element of the list
    #to pull up a chart for a specific FI later on
    tmpPlotList[[counter]] <- list(FinancialInstitution=inputTSList[counter]$USBEFITimeSeriesList$FinancialInstitution, TimeSeriesChart=tmpPlot);
    counter <- counter+1;
  }
  
  return(tmpPlotList);
}


##This function creates and stores plots of the seasonally adjusted volume/value timeseries
createSeasonallyAdjustedTSPlots <- function(inputTSList, measUnit, measUnitType, tsFreq, sYr,eYr, savePath){
  #declare local variables
  tmpPlot <- NULL;
  tmpPlotList <- NULL;
  measurementUnitTypeString <- NULL;
  measurementUnitString <- NULL;
  mainTitleLeadingString <- NULL;
  tsFreqString <- NULL;
  counter <- 1;#declare and initialise the reference counter
  
  #set the value of the measurment unit as reported in the chart
  if(measUnit==1000000){
    measurementUnitString = "(millions)";
  }else if(measUnit==1000){
    measurementUnitString = "(thousands)";
  }else if(measUnit==1){
    measurementUnitString = "(amount)";
  }else{
    print("wrong measurment unit provided: Please check and re-enter")
  }
  
  #set values for file naming, y-axis labels and main plot title
  if(measUnitType %in% c("volume","Volume","Vol","VOLUME")){
    measurementUnitTypeString = "Transactions Volume";
    measurementUnitTypeSaveString = "TransactionsVolumeSeasonallyAdjusted.png";
    mainTitleLeadingString = "USBE Transactions Volume for";
  } else if(measUnitType %in% c("value","Value","Val","VALUE")){
    measurementUnitTypeString = "Transactions Value";
    measurementUnitTypeSaveString = "TransactionsValueSeasonallyAdjusted.png";
    mainTitleLeadingString = "USBE Transactions Value for";
  }
  
  #set the value of the x-axis lable
  #if monthly
  if(dataFrequency == "monthly"){
    tsFreqString <- "Year and Month";
    #if quarterly  
  }else if(dataFrequency == "quarterly"){
    tsFreqString <- "Year and Quarter";
    #if annual 
  }else if(dataFrequency == "annual"){
    extractedUSBEFITimeSeriesLoadPath <- 
      tsFreqString <- "Year";
    #if daily   
  }else if(dataFrequency == "daily"){
    extractedUSBEFITimeSeriesLoadPath <- 
      tsFreqString <- "date";
  }
  
  #loop through each element of the input list of time series data
  while(counter <= length(inputTSList)){
    #create the lattice plot
    testSTL <- stl(na.interp(inputTSList[counter]$USBEFITimeSeriesList$TimeSeries/measUnit),s.window="periodic");#recalling stl is not efficient 
    #since it is used to generate the forecasts and the output store at that point 
    seasAdjTestSTL <- seasadj(testSTL);
    #add the seasonally adjusted sereis to the chart
    #1: start recording chart panel window
    #2: plot the chart
    #3: print the chart
    #4: bring the panel into focus
    #5: add the seasonally adjusted plot to the panel
    #6: take the panel out of focus
    #7: stop recording an save
    fileLocation <- paste(savePath, inputTSList[counter]$USBEFITimeSeriesList$FinancialInstitution, measurementUnitTypeSaveString, sep="");
    png(fileLocation);
    tmpPlot <- xyplot(na.interp(inputTSList[counter]$USBEFITimeSeriesList$TimeSeries/measUnit), 
                      #screens = list(inputTSList[counter]$USBEFITimeSeriesList$FinancialInstitution),
                      superpose = TRUE, xlim = extendrange(sYr:eYr),par.settings = graphSettings, 
                      main=paste(mainTitleLeadingString, inputTSList[counter]$USBEFITimeSeriesList$FinancialInstitution),
                      scales=list(cex=0.7, tck=c(1,0), y=list(tick.number=10, rot=360), x=list(tick.number=15)), 
                      xlab = paste(tsFreqString), ylab =paste(measurementUnitTypeString, measurementUnitString, sep = " "));
    print(tmpPlot);#printing into the plots display window is required so as to allow the storing of the plot object
    trellis.focus("panel", 1, 1);
    panel.lines(seasAdjTestSTL,col="red",ylab=paste("Seasonally Adjusted", measurementUnitTypeString, measurementUnitString, sep = " "));
    trellis.unfocus();
    dev.off();#captures the chart in the plots display window and then clears the window
    
    #add the lattice plot to the list of plots
    #This has been made to reference the FI name as a primary key to prevent having to loop through every element of the list
    #to pull up a chart for a specific FI later on
    tmpPlotList[[counter]] <- list(FinancialInstitution=inputTSList[counter]$USBEFITimeSeriesList$FinancialInstitution, SeasonallyAdjustedChart=tmpPlot);
    counter <- counter+1;
    
  }
  return(tmpPlotList);
}


##This function is used to generate a list of plots of decomposed time series from the provided list of raw time series objects
createTSDecompositionPlots <- function(inputTSList, measUnit, measUnitType, tsFreq, sYr,eYr, savePath){
  #declare local variables
  tmpPlot <- NULL;
  tmpPlotList <- NULL;
  measurementUnitTypeString <- NULL;
  measurementUnitString <- NULL;
  tsFreqString <- NULL;
  counter <- 1;
  fileLocation <- NULL;
  
  #set the value of the measurment unit as reported in the chart
  if(measUnit==1000000){
    measurementUnitString = "(millions)";
  }else if(measUnit==1000){
    measurementUnitString = "(thousands)";
  }else if(measUnit==1){
    measurementUnitString = "(amount)";
  }else{
    print("wrong measurment unit provided: Please check and re-enter")
  }
  
  #set values for file naming, y-axis labels and main plot title
  if(measUnitType %in% c("volume","Volume","Vol","VOLUME")){
    measurementUnitTypeString = "Transactions Volume";
    measurementUnitTypeSaveString = "TransactionsVolumeTSDecomposition.png";
  } else if(measUnitType %in% c("value","Value","Val","VALUE")){
    measurementUnitTypeString = "Transactions Value";
    measurementUnitTypeSaveString = "TransactionsValueTSDecomposition.png";
  }
  
  #set the value of the x-axis lable
  #if monthly
  if(dataFrequency == "monthly"){
    tsFreqString <- "Year and Month";
    #if quarterly  
  }else if(dataFrequency == "quarterly"){
    tsFreqString <- "Year and Quarter";
    #if annual 
  }else if(dataFrequency == "annual"){
    extractedUSBEFITimeSeriesLoadPath <- 
      tsFreqString <- "Year";
    #if daily   
  }else if(dataFrequency == "daily"){
    extractedUSBEFITimeSeriesLoadPath <- 
      tsFreqString <- "date";
  }
  
  while(counter <= length(inputTSList)){
    
    fileLocation <- paste(savePath, inputTSList[counter]$USBEFITimeSeriesList$FinancialInstitution, measurementUnitTypeSaveString, sep="");
    png(fileLocation);
    #create the lattice plot
    tmpPlot <- xyplot(stl(na.interp(inputTSList[counter]$USBEFITimeSeriesList$TimeSeries/measUnit),s.window="periodic", robust=TRUE), 
                      main= paste("Time Series Decomposition for ",list(inputTSList[counter]$USBEFITimeSeriesList$FinancialInstitution)), 
                      scales=list(cex=0.7, tck=c(1,0), y=list(tick.number=10, rot=360), x=list(tick.number=15)),
                      xlab = paste(tsFreqString), ylab =paste(measurementUnitTypeString, measurementUnitString, sep = " "));
    #add the lattice plot to the list of plots
    #This has been made to reference the FI name as a primary key to prevent having to loop through every element of the list
    #to pull up a chart for a specific FI later on
    tmpPlotList[[counter]] <- 
      list(FinancialInstitution=inputTSList[counter]$USBEFITimeSeriesList$FinancialInstitution, TimeSeriesDecompositionChart=tmpPlot);
    print(tmpPlot);#printing into the plots display window is required so as to allow the storing of the plot object
    dev.off();#captures the chart in the plots display window and then clears the window
    
    counter <- counter+1;
  }
  
  return(tmpPlotList); 
}

##This function plots the PACF (partial autocorreleation function) of the supplied time series data and returns a list of ACF plots
createTSPACFPlots <- function(inputTSList, measUnit){
  #declare local variables
  tmpPlot <- NULL;
  tmpPlotList <- NULL;
  counter <- 1;
  
  while(counter <= length(inputTSList)){
    #create the PACF plot
    tmpPlot <- pacf(na.interp(inputTSList[counter]$USBEFITimeSeriesList$TimeSeries/measUnit),
                    main= paste("PACF of ", list(inputTSList[counter]$USBEFITimeSeriesList$FinancialInstitution)));
    #add the lattice plot to the list of plots
    #This has been made to reference the FI name as a primary key to prevent having to loop through every element of the list
    #to pull up a chart for a specific FI later on
    tmpPlotList[[counter]] <- list(FinancialInstitution=inputTSList[counter]$USBEFITimeSeriesList$FinancialInstitution, TimeSeriesPACFChart=tmpPlot);
    counter <- counter + 1;
  }
  return(tmpPlotList) 
}

##This function plots the ACF (autocorreleation function) of the supplied time series data and returns a list of ACF plots
createTSACFPlots <- function(inputTSList, measUnit){
  
  tmpPlot <- NULL;
  tmpPlotList <- NULL;
  counter <- 1;
  
  while(counter <= length(inputTSList)){
    #create the ACF plot
    #NOTE: The "Acf" uses the forecast package not the default "stats" package which is "acf"
    tmpPlot <- Acf(na.interp(inputTSList[counter]$USBEFITimeSeriesList$TimeSeries/measUnit),
                   main= paste("ACF of ", list(inputTSList[counter]$USBEFITimeSeriesList$FinancialInstitution)));
    #add the lattice plot to the list of plots
    #This has been made to reference the FI name as a primary key to prevent having to loop through every element of the list
    #to pull up a chart for a specific FI later on
    tmpPlotList[[counter]] <- list(FinancialInstitution=inputTSList[counter]$USBEFITimeSeriesList$FinancialInstitution, TimeSeriesACFChart=tmpPlot);
    counter <-  counter + 1;
  }
  return(tmpPlotList) 
}


##This function is used to generate a list of decomposed time series from the provided list of raw time series objects
createTSDecomposition <- function(inputTSList, measUnit){
  #declare local variables
  tsDecom <- NULL;
  tsDecomList <- NULL;
  counter <- 1;
  
  
  while(counter <= length(inputTSList)){
    #create the TS decompositions
    tsDecom <- stl(na.interp(inputTSList[counter]$USBEFITimeSeriesList$TimeSeries/measUnit),s.window="periodic", robust=TRUE);
    
    #add the lattice plot to the list of plots
    tsDecomList[[counter]] <- list(FinancialInstitution=inputTSList[counter]$USBEFITimeSeriesList$FinancialInstitution, TimeSeriesDecomposition=tsDecom);
    counter <- counter+1;
  }
  return(tsDecomList); 
}


##This is the function that computes the forecasts. The estimation method used is the un-seasonal ARIMA(p,d,q)
generateARIMAForecasts <- function(inputTSDecompList, dummyVector, inputArimaOrder, fcstHrzn, cnst, drft, confLev){
  #declare local variables
  tmpSTL <- NULL;
  fcasting <- NULL;
  tsfcastList <- NULL;
  counter <- 1;
  
  while(counter <= length(inputTSDecompList)){
    #select the TS decompositions
    tmpSTL <- inputTSDecompList[[counter]]$TimeSeriesDecomposition;
    
    #add the lattice plot to the list of plots
    if(cnst == TRUE & drft == TRUE){
      #fcasting <- forecast(forecastFunctionDriftConstant(tmpSTL, inputArimaOrder, fcstHrzn));
      
      fcasting <- forecast(tmpSTL, h=fcstHrzn, 
                           forecastfunction=function(x,h,level, ...){
                             fit <- Arima(x, xreg=dummyVector, order=inputArimaOrder, include.constant = TRUE, include.drift = TRUE) 
                             #note including the constant has no impact with the drift included
                             #with just the constant the forecasts are far too low
                             #with drift the forecasts are similar to previous monthly/annual but with wider bands
                             return(forecast(fit, h=fcstHrzn,level=confLev,xreg=dummyVector,...))});
    }
    fcastCoefs <- fcasting$model$coef;
    fcstValues <- fcasting;
    fcstErrors <- residuals(fcasting);
    bjTest <- Box.test(fcstErrors, lag=16, fitdf=4, type="Ljung-Box");
    fcastSummary <- summary(fcasting);
    
    
    #add the forecast results to the forecast list
    tsfcastList[[counter]] <- list(FinancialInstitution=inputTSDecompList[[counter]]$FinancialInstitution, 
                                   ForecastCoefficients=fcastCoefs,
                                   ForecastedSeries=fcstValues,
                                   ForecastErrors=fcstErrors,
                                   ForecastSummary=fcastSummary,
                                   BoxLjungTest=bjTest);
    counter <- counter+1;
  }
  return(tsfcastList); 
}

##this function is used to export the point forecasts and confidence intervals into an excel file by FI
exportListOfForecastsToExcel <- function(inputForecastsList, measUnitType, savePath){
  #declare local variables
  counter = 1;
  dtList <- NULL;
  fileLocation <- NULL;
  
  #set values for file naming, y-axis labels and main plot title
  if(measUnitType %in% c("volume","Volume","Vol","VOLUME")){
    measurementUnitTypeString = "TransactionsVolumeForecast.xlsx";
  } else if(measUnitType %in% c("value","Value","Val","VALUE")){
    measurementUnitTypeString = "TransactionsValueForecast.xlsx";
  }
  
  while(counter<=length(inputForecastsList)){
    dtList[[counter]] <- as.data.table(inputForecastsList[[counter]]$ForecastedSeries);
    fileLocation <- paste(savePath, inputForecastsList[[counter]]$FinancialInstitution, measurementUnitTypeString, sep="");
    write.xlsx2(dtList[[counter]], file = fileLocation);
    #assign(paste(savePath,inputForecastsList[[counter]]$FinancialInstitution,measurementUnitTypeString,sep=""), (dtList[[counter]]));
    counter <- counter + 1;
  }
  
  return(dtList);
}

##this function is used to export the Forecast model Coefficients into an excel file by FI
exportListOfForecastModelCoefficientsToExcel <- function(inputForecastsList, measUnitType, savePath){
  #declare local variables
  counter = 1;
  dtList <- NULL;
  fileLocation <- NULL;
  
  #set values for file naming, y-axis labels and main plot title
  if(measUnitType %in% c("volume","Volume","Vol","VOLUME")){
    measurementUnitTypeString = "TransactionsVolumeForecastCoefficients.xlsx";
  } else if(measUnitType %in% c("value","Value","Val","VALUE")){
    measurementUnitTypeString = "TransactionsValueForecastCoefficients.xlsx";
  }
  
  while(counter<=length(inputForecastsList)){
    dtList[[counter]] <- as.data.table(inputForecastsList[[counter]]$ForecastCoefficients);
    fileLocation <- paste(savePath, inputForecastsList[[counter]]$FinancialInstitution, measurementUnitTypeString, sep="");
    write.xlsx2(dtList[[counter]], file = fileLocation);
    #assign(paste(savePath,inputForecastsList[[counter]]$FinancialInstitution,measurementUnitTypeString,sep=""), (dtList[[counter]]));
    counter <- counter + 1;
  }
  
  return(dtList);
}


##This function creates and stores charts of the volume/value forecast results
exportForecastChartList <- function(inputForecastsList, dataFrequency, measUnit, measUnitType, savePath, fcstEndYear){
  #declare local variables
  counter = 1;
  ctList <- NULL;
  cht <- NULL;
  fileLocation <- NULL;
  xAxisMin <- NULL;
  xAxisMax <- fcstEndYear+1;
  
  
  #set the value of the measurment unit as reported in the chart
  if(measUnit==1000000){
    measurementUnitString = "(millions)";
  }else if(measUnit==1000){
    measurementUnitString = "(thousands)";
  }else if(measUnit==1){
    measurementUnitString = "(amount)";
  }else{
    print("wrong measurment unit provided: Please check and re-enter")
  }
  
  #set values for file naming, y-axis labels and main plot title
  if(measUnitType %in% c("volume","Volume","Vol","VOLUME")){
    measurementUnitTypeString = "Transactions Volume";
    measurementUnitTypeSaveString = "TransactionsVolumeForecast.png";
    mainTitleLeadingString = "USBE Transactions Volume Forecast for";
  } else if(measUnitType %in% c("value","Value","Val","VALUE")){
    measurementUnitTypeString = "Transactions Value";
    measurementUnitTypeSaveString = "TransactionsValueForecast.png";
    mainTitleLeadingString = "USBE Transactions Value Forecast for";
  }
  
  #set the value of the x-axis lable
  #if monthly
  if(dataFrequency == "monthly"){
    tsFreqString <- "Year and Month";
    #if quarterly  
  }else if(dataFrequency == "quarterly"){
    tsFreqString <- "Year and Quarter";
    #if annual 
  }else if(dataFrequency == "annual"){
    extractedUSBEFITimeSeriesLoadPath <- 
      tsFreqString <- "Year";
    #if daily   
  }else if(dataFrequency == "daily"){
    extractedUSBEFITimeSeriesLoadPath <- 
      tsFreqString <- "date";
  }
  
  while(counter<=length(inputForecastsList)){
    fileLocation <- paste(savePath, inputForecastsList[[counter]]$FinancialInstitution, measurementUnitTypeSaveString, sep="");
    xAxisMin <- start(inputForecastsList[[counter]]$ForecastedSeries$x)[1];##start(ts) returns a vector where the first element is the year
    print(xAxisMin);
    png(fileLocation);
    cht <- autoplot(inputForecastsList[[counter]]$ForecastedSeries, las=1,
                    #main=paste(mainTitleLeadingString, inputForecastsList[[counter]]$FinancialInstitution), 
                    main=paste(""),
                    xlab=tsFreqString, ylab=paste(measurementUnitTypeString, measurementUnitString, sep = " "), cex.axis=0.75) + xlim(c(xAxisMin,xAxisMax));
    ctList[[counter]] <- cht;
    print(cht);#printing into the plots display window is required so as to allow the storing of the plot object
    dev.off();#captures the chart in the plots display window and then clears the window
    #assign(paste(savePath,inputForecastsList[[counter]]$FinancialInstitution,measurementUnitTypeString,sep=""), (dtList[[counter]]));
    counter <- counter + 1;
  }
  
  return(ctList);
}



##<<<<<<<<<<<<<<<<<<<<<<<<<DECLARE GLOBAL VARIABLES>>>>>>>>>>>>>>>>>>>>>>>>
##Declare all global variables to be used in this class
startYear <- 2000;
endYear <- 2015;
forecastEndYear <- 2018;
measurementUnitType <- "Volume";
measurementUnit <- 1000000; #possible values 1000000, 1000, or 1
keyField <- "sender"; #the colum to be used as the unique key forthe data table (can be "sender" or "reciever")'
dataFrequency <- "quarterly"; #the options are monthly, quarterly, annual, daily (does not make sense since it does not distingush the year)
dataFrequencyColumnName <- NULL; #used to capture the name of the time colum for use in the time series generation method/function.
#dataFrequencyColumnName can take values "Year_Month", "Year_Quarter", "Year" "date" and will be set automatically 
#based on the dataFrequency
extractedUSBEFITimeSeriesLoadPath <- NULL; #used to store the location of the RData file from which the time sereis are to be generate
timeSeriesPlotsUSBE <- NULL;
timeSeriesDecompositionPlotsUSBE <- NULL;
timeSeriesPACFPlotsUSBE <- NULL;
timeSeriesACFPlotsUSBE <- NULL;
timeSeriesDecompositionUSBE <- NULL;
timeSeriesARIMAForecastsUSBE <- NULL;
dtListFILevelARIMAForecastsUSBE <- NULL;
dtListFILevelARIMACoefficientsUSBE <- NULL;
chartListFILevelARIMAForecastUSBE <- NULL;
seasonallyAdjustedTimeSeriesPlotsUSBE <- NULL;
forecastsSavePath <- NULL;
arimaOrder <- c(1,0,1); 
arimaDrift <- TRUE;
arimaConstant <- TRUE;
confidenceLevel <- 95 #c(80,95)




#will load the data table: USBETransDataSeriesSentRecieved
#and set the name of the time column (dataFrequencyColumnName)
#if monthly
if(dataFrequency == "monthly"){
  extractedUSBEFITimeSeriesLoadPath <- 
    c("C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/ExtractedTimeSeriesData/USBEFIMonthlyTimeSeriesData2000-15.RData");
  forecastsSavePath <- 
    c("C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/MonthlyForecasts/FI_Level/USBE/");
  dataFrequencyColumnName <- "Year_Month";
  USBEFITimeSeriesMissingDataDummyLoadPath <- 
    c("C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/ExtractedTimeSeriesData/USBEFIMonthlyTSMissingDataDummy.RData");
  
  forecastHorizen  <- (forecastEndYear - endYear)*12;
  
    #if quarterly  
}else if(dataFrequency == "quarterly"){
  extractedUSBEFITimeSeriesLoadPath <- 
    c("C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/ExtractedTimeSeriesData/USBEFIQuarterlyTimeSeriesData2000-15.RData");
  forecastsSavePath <- 
    c("C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/QuarterlyForecasts/FI_Level/USBE/");
  dataFrequencyColumnName <- "Year_Quarter";

  USBEFITimeSeriesMissingDataDummyLoadPath <- 
    c("C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/ExtractedTimeSeriesData/USBEFIQuarterlyTSMissingDataDummy.RData");
  
  forecastHorizen  <- (forecastEndYear - endYear)*4;
  
  #if annual 
}else if(dataFrequency == "annual"){
  extractedUSBEFITimeSeriesLoadPath <- 
    c("C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/ExtractedTimeSeriesData/USBEFIAnnualTimeSeriesData2000-15.RData");
  forecastsSavePath <- 
    c("C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/AnnualForecasts/FI_Level/USBE/");
  dataFrequencyColumnName <- "Year";
 
  USBEFITimeSeriesMissingDataDummyLoadPath <- 
    c("C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/ExtractedTimeSeriesData/USBEFIAnnualTSMissingDataDummy.RData");
  
  forecastHorizen  <- (forecastEndYear - endYear);
  
  #if daily   
}else if(dataFrequency == "daily"){
  extractedUSBEFITimeSeriesLoadPath <- 
    c("C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/ExtractedTimeSeriesData/USBEFIDailyTimeSeriesData2000-15.RData");
  dataFrequencyColumnName <- "date";
 
  USBEFITimeSeriesMissingDataDummyLoadPath <- 
    c("C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/ExtractedTimeSeriesData/USBEFIDailyTSMissingDataDummyTS.RData");

  forecastHorizen  <- (forecastEndYear - endYear)*365; ##this has to be fixed to allow leap years
  
}

##<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<START DATA/STATISTICAL ANALYSIS>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
##<<<<<<<<<<<<<<<<<<<<<<<<<LOAD DATA>>>>>>>>>>>>>>>>>>>>>>>>
##Now load the individual FI's complete data tables
print("Loading data table(s)");#used to record speed of computations
print(now());#used to record speed of computations
load(extractedUSBEFITimeSeriesLoadPath);
load(USBEFITimeSeriesMissingDataDummyLoadPath);##load the missing data dummy varriable
##for some reason loading the USBEFITimeSeriesList from the RData file loads the list as a variable "x".
##this if-statement is used to rename the loaded list from x to the more meaningfull USBEFITimeSeriesList
if(exists("x")){ #check if variable x exists
  USBEFITimeSeriesList <- x; #if x exists, create a new variable USBEFITimeSeriesList and set its value to the value of x
  #  rm(x);#remove the default system named x list
}



gc();
print("data table(s) loaded");#used to record speed of computations
print(now());#used to record speed of computations
gc();

timeSeriesPlotsUSBE <- createTSPlots(USBEFITimeSeriesList, measurementUnit, measurementUnitType, dataFrequency, startYear, endYear);
timeSeriesDecompositionPlotsUSBE <- 
  createTSDecompositionPlots(USBEFITimeSeriesList, measurementUnit, measurementUnitType, dataFrequency, startYear, endYear, forecastsSavePath);
timeSeriesPACFPlotsUSBE <- createTSPACFPlots(USBEFITimeSeriesList, measurementUnit);
timeSeriesACFPlotsUSBE <- createTSACFPlots(USBEFITimeSeriesList, measurementUnit);
timeSeriesDecompositionUSBE <- createTSDecomposition(USBEFITimeSeriesList, measurementUnit);
timeSeriesARIMAForecastsUSBE <- generateARIMAForecasts(timeSeriesDecompositionUSBE, q42007MissingDataDummyTS, arimaOrder, forecastHorizen, arimaConstant, arimaDrift, confidenceLevel);
dtListFILevelARIMAForecastsUSBE <- exportListOfForecastsToExcel(timeSeriesARIMAForecastsUSBE, measurementUnitType, forecastsSavePath);
dtListFILevelARIMACoefficientsUSBE <- exportListOfForecastModelCoefficientsToExcel(timeSeriesARIMAForecastsUSBE, measurementUnitType, forecastsSavePath);
chartListFILevelARIMAForecastUSBE <- 
  exportForecastChartList(timeSeriesARIMAForecastsUSBE, dataFrequency, measurementUnit, measurementUnitType, forecastsSavePath, forecastEndYear);
gc();
seasonallyAdjustedTimeSeriesPlotsUSBE <- 
  createSeasonallyAdjustedTSPlots(USBEFITimeSeriesList, measurementUnit, measurementUnitType, dataFrequency, startYear, endYear, forecastsSavePath);

gc();

