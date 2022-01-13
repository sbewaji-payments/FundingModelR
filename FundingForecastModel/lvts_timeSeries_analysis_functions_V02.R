##This is the class in which the statistical analysis and forecasting is carried out
##The forecasts are carried out by looping through each element of the Timeseries list and assessing the timeseries 
## (TS) object contained therein
##
##
## Author : Segun Bewaji
## Modified : Segun Bewaji
## Modifications Made: from Version 01
##        1: Included function to compute Seasonal ARIMA(p,d,q)(P,D,Q,n). Lines 430 to 529
##            generateSARIMAForecastsWithDummy <- function(inputTSDecompList, dummyVector, inputArimaOrder, 
##                                                          inputSeasonalOrder, inputNumberOfSeasonPeriods, 
##                                                          fcstHrzn, cnst, drft, confLev){}
##            and
##            generateSARIMAForecasts <- function(inputTSDecompList, inputArimaOrder, 
##                                                          inputSeasonalOrder, inputNumberOfSeasonPeriods, 
##                                                          fcstHrzn, cnst, drft, confLev){}
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

## Creation Date : 25 Sept 2018
## Modification Date : 
## $Id$


##clear system memory and workspace
gc();

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
  }else if(measUnit== 1000000000){
    measurementUnitString = "(billions)";
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
    extractedLVTSFITimeSeriesLoadPath <- 
      tsFreqString <- "Year";
  #if daily   
  }else if(dataFrequency == "daily"){
    extractedLVTSFITimeSeriesLoadPath <- 
      tsFreqString <- "date";
  }
  
  while(counter <= length(inputTSList)){
    #create the lattice plot
    tmpPlot <- xyplot.ts(inputTSList[counter]$LVTSFITimeSeriesList$TimeSeries/measUnit, 
                         screens = list(inputTSList[counter]$LVTSFITimeSeriesList$FinancialInstitution),
              superpose = TRUE, xlim = extendrange(sYr:eYr),par.settings = graphSettings, 
              scales=list(cex=0.7, tck=c(1,0), y=list(tick.number=10, rot=360), x=list(tick.number=15)), 
              xlab = paste(tsFreqString), ylab =paste(measurementUnitTypeString, measurementUnitString, sep = " "));
    #add the lattice plot to the list of plots
    #This has been made to reference the FI name as a primary key to prevent having to loop through every element of the 
    ##list to pull up a chart for a specific FI later on
    tmpPlotList[[counter]] <- list(FinancialInstitution=inputTSList[counter]$LVTSFITimeSeriesList$FinancialInstitution, 
                                   TimeSeriesChart=tmpPlot);
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
  }else if(measUnit== 1000000000){
    measurementUnitString = "(billions)";
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
    mainTitleLeadingString = "LVTS Transactions Volume for";
  } else if(measUnitType %in% c("value","Value","Val","VALUE")){
    measurementUnitTypeString = "Transactions Value";
    measurementUnitTypeSaveString = "TransactionsValueSeasonallyAdjusted.png";
    mainTitleLeadingString = "LVTS Transactions Value for";
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
    extractedLVTSFITimeSeriesLoadPath <- 
      tsFreqString <- "Year";
    #if daily   
  }else if(dataFrequency == "daily"){
    extractedLVTSFITimeSeriesLoadPath <- 
      tsFreqString <- "date";
  }
  
  #loop through each element of the input list of time series data
  while(counter <= length(inputTSList)){
    #create the lattice plot
    testSTL <- stl(inputTSList[counter]$LVTSFITimeSeriesList$TimeSeries/measUnit,s.window="periodic");#recalling stl is not 
    ##efficient since it is used to generate the forecasts and the output store at that point 
    seasAdjTestSTL <- seasadj(testSTL);
    #add the seasonally adjusted sereis to the chart
    #1: start recording chart panel window
    #2: plot the chart
    #3: print the chart
    #4: bring the panel into focus
    #5: add the seasonally adjusted plot to the panel
    #6: take the panel out of focus
    #7: stop recording an save
    fileLocation <- paste(savePath, inputTSList[counter]$LVTSFITimeSeriesList$FinancialInstitution, 
                          measurementUnitTypeSaveString, sep="");
    png(fileLocation);
    tmpPlot <- xyplot(inputTSList[counter]$LVTSFITimeSeriesList$TimeSeries/measUnit, 
                         #screens = list(inputTSList[counter]$LVTSFITimeSeriesList$FinancialInstitution),
                         superpose = TRUE, xlim = extendrange(sYr:eYr),par.settings = graphSettings, 
                         main=paste(mainTitleLeadingString, inputTSList[counter]$LVTSFITimeSeriesList$FinancialInstitution),
                         scales=list(cex=0.7, tck=c(1,0), y=list(tick.number=10, rot=360), x=list(tick.number=15)), 
                         xlab = paste(tsFreqString), ylab =paste(measurementUnitTypeString, 
                                                                 measurementUnitString, sep = " "));
    print(tmpPlot);#printing into the plots display window is required so as to allow the storing of the plot object
    trellis.focus("panel", 1, 1);
    panel.lines(seasAdjTestSTL,col="red",ylab=paste("Seasonally Adjusted", measurementUnitTypeString, 
                                                    measurementUnitString, sep = " "));
    trellis.unfocus();
    dev.off();#captures the chart in the plots display window and then clears the window

    #add the lattice plot to the list of plots
    #This has been made to reference the FI name as a primary key to prevent having to loop through every element of 
    ##the list to pull up a chart for a specific FI later on
    tmpPlotList[[counter]] <- list(FinancialInstitution=inputTSList[counter]$LVTSFITimeSeriesList$FinancialInstitution, 
                                   SeasonallyAdjustedChart=tmpPlot);
    counter <- counter+1;
    
  }
  return(tmpPlotList);
}


##This function is used to generate a list of plots of decomposed time series from the provided list of 
##raw time series objects
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
  }else if(measUnit== 1000000000){
    measurementUnitString = "(billions)";
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
    extractedLVTSFITimeSeriesLoadPath <- 
      tsFreqString <- "Year";
    #if daily   
  }else if(dataFrequency == "daily"){
    extractedLVTSFITimeSeriesLoadPath <- 
      tsFreqString <- "date";
  }
  
  while(counter <= length(inputTSList)){
    
    fileLocation <- paste(savePath, inputTSList[counter]$LVTSFITimeSeriesList$FinancialInstitution, 
                          measurementUnitTypeSaveString, sep="");
    png(fileLocation);
    #create the lattice plot
    tmpPlot <- xyplot(stl(inputTSList[counter]$LVTSFITimeSeriesList$TimeSeries/measUnit,s.window="periodic", robust=TRUE), 
                      main= paste("Time Series Decomposition for ",
                                  list(inputTSList[counter]$LVTSFITimeSeriesList$FinancialInstitution)), 
                      scales=list(cex=0.7, tck=c(1,0), y=list(tick.number=10, rot=360), x=list(tick.number=15)),
                      xlab = paste(tsFreqString), ylab =paste(measurementUnitTypeString, measurementUnitString, sep = " "), 
                      na.action = na.omit);
    #add the lattice plot to the list of plots
    #This has been made to reference the FI name as a primary key to prevent having to loop through every element of the 
    ##list to pull up a chart for a specific FI later on
    tmpPlotList[[counter]] <- 
      list(FinancialInstitution=inputTSList[counter]$LVTSFITimeSeriesList$FinancialInstitution, 
           TimeSeriesDecompositionChart=tmpPlot);
    print(tmpPlot);#printing into the plots display window is required so as to allow the storing of the plot object
    dev.off();#captures the chart in the plots display window and then clears the window
    
    counter <- counter+1;
  }
  
  return(tmpPlotList); 
}

##This function plots the PACF (partial autocorreleation function) of the supplied time series data and 
##returns a list of ACF plots
createTSPACFPlots <- function(inputTSList, measUnit){
  #declare local variables
  tmpPlot <- NULL;
  tmpPlotList <- NULL;
  counter <- 1;
  
  while(counter <= length(inputTSList)){
    #create the PACF plot
    tmpPlot <- pacf(inputTSList[counter]$LVTSFITimeSeriesList$TimeSeries/measUnit,
                    main= paste("PACF of ", list(inputTSList[counter]$LVTSFITimeSeriesList$FinancialInstitution)));
    #add the lattice plot to the list of plots
    #This has been made to reference the FI name as a primary key to prevent having to loop through every element of 
    ##the list to pull up a chart for a specific FI later on
    tmpPlotList[[counter]] <- list(FinancialInstitution=inputTSList[counter]$LVTSFITimeSeriesList$FinancialInstitution, 
                                   TimeSeriesPACFChart=tmpPlot);
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
    tmpPlot <- Acf(inputTSList[counter]$LVTSFITimeSeriesList$TimeSeries/measUnit,
                    main= paste("ACF of ", list(inputTSList[counter]$LVTSFITimeSeriesList$FinancialInstitution)));
    #add the lattice plot to the list of plots
    #This has been made to reference the FI name as a primary key to prevent having to loop through every element of 
    ##the list to pull up a chart for a specific FI later on
    tmpPlotList[[counter]] <- list(FinancialInstitution=inputTSList[counter]$LVTSFITimeSeriesList$FinancialInstitution, 
                                   TimeSeriesACFChart=tmpPlot);
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
    tsDecom <- stl(inputTSList[counter]$LVTSFITimeSeriesList$TimeSeries/measUnit,s.window="periodic", robust=TRUE);
    
    #add the lattice plot to the list of plots
    tsDecomList[[counter]] <- list(FinancialInstitution=inputTSList[counter]$LVTSFITimeSeriesList$FinancialInstitution, 
                                   TimeSeriesDecomposition=tsDecom);
    counter <- counter+1;
  }
  return(tsDecomList); 
}


##This is the function that computes the forecasts. The estimation method used is the un-seasonal ARIMA(p,d,q)
generateARIMAForecasts <- function(inputTSDecompList, inputArimaOrder, fcstHrzn, cnst, drft, confLev){ ##browser()
  #declare local variables
  tmpSTL <- NULL;
  fcasting <- NULL;
  tsfcastList <- NULL;
  counter <- 1;
  
  #while(counter <= length(inputTSDecompList)){
  while(counter <= length(inputTSDecompList)){
      #select the TS decompositions
    tmpSTL <- inputTSDecompList[[counter]]$TimeSeriesDecomposition;
    
    #add the lattice plot to the list of plots
    if(cnst == TRUE & drft == TRUE){
      #fcasting <- forecast(forecastFunctionDriftConstant(tmpSTL, inputArimaOrder, fcstHrzn));
      
      fcasting <- forecast(tmpSTL, h=fcstHrzn, 
                                 forecastfunction=function(x,h,level, ...){
                                 fit <- Arima(x, order=inputArimaOrder, include.constant = TRUE, include.drift = TRUE, 
                                              method="ML")
                                 #note including the constant has no impact with the drift included
                                 #with just the constant the forecasts are far too low
                                 #with drift the forecasts are similar to previous monthly/annual but with wider bands
                                 return(forecast(fit,h=fcstHrzn,level=confLev, ...))});
    }
    fcastCoefs <- fcasting$model$coef;
    fcstValues <- fcasting;
    fcstErrors <- residuals(fcasting);
    modAccuracy <- as.data.table(accuracy(fcasting));
    bjTest <- Box.test(fcstErrors, lag=16, fitdf=4, type="Ljung-Box");
    fcastSummary <- summary(fcasting);
    
    
    #add the forecast results to the forecast list
    tsfcastList[[counter]] <- list(FinancialInstitution=inputTSDecompList[[counter]]$FinancialInstitution, 
                             ForecastCoefficients=fcastCoefs,
                             ForecastedSeries=fcstValues,
                             ForecastErrors=fcstErrors,
                             ForecastModelAccuracy=modAccuracy,
                             ForecastSummary=fcastSummary,
                             BoxLjungTest=bjTest);
    counter <- counter+1;
  }
  print("generateARIMAForecasts: complete")
  return(tsfcastList); 
}



##This is the function that computes the forecasts. The estimation method used is the un-seasonal ARIMA(p,d,q)
generateARIMAForecastsWithDummy <- function(inputTSDecompList, dummyVector, inputArimaOrder, fcstHrzn, cnst, drft, confLev){
  #declare local variables
  tmpSTL <- NULL;
  fcasting <- NULL;
  tsfcastList <- NULL;
  counter <- 1;
  #print("entering while loop");
  #print(inputTSDecompList);
  #print(dummyVector);
  while(counter <= length(inputTSDecompList)){
    #select the TS decompositions
    tmpSTL <- inputTSDecompList[[counter]]$TimeSeriesDecomposition;
    
    #print(tmpSTL);
    print("entering if constant and drift are true statement");
    #add the lattice plot to the list of plots
    if(cnst == TRUE & drft == TRUE){
      #fcasting <- forecast(forecastFunctionDriftConstant(tmpSTL, inputArimaOrder, fcstHrzn));
      
      fcasting <- forecast(tmpSTL, h=fcstHrzn, 
                           forecastfunction=function(x,h,level, ...){
                             fit <- Arima(x, xreg=dummyVector, order=inputArimaOrder, include.constant = TRUE, 
                                          include.drift = TRUE) 
                             #note including the constant has no impact with the drift included
                             #with just the constant the forecasts are far too low
                             #with drift the forecasts are similar to previous monthly/annual but with wider bands
                             return(forecast(fit, h=fcstHrzn,level=confLev,xreg=dummyVector,...))});
    }
    fcastCoefs <- fcasting$model$coef;
    fcstValues <- fcasting;
    fcstErrors <- residuals(fcasting);
    modAccuracy <- as.data.table(accuracy(fcasting));
    bjTest <- Box.test(fcstErrors, lag=16, fitdf=4, type="Ljung-Box");
    fcastSummary <- summary(fcasting);
    
    
    print("adding the forecast results to the forecast list");
    #add the forecast results to the forecast list
    tsfcastList[[counter]] <- list(FinancialInstitution=inputTSDecompList[[counter]]$FinancialInstitution, 
                                   ForecastCoefficients=fcastCoefs,
                                   ForecastedSeries=fcstValues,
                                   ForecastErrors=fcstErrors,
                                   ForecastModelAccuracy=modAccuracy,
                                   ForecastSummary=fcastSummary,
                                   BoxLjungTest=bjTest);
    counter <- counter+1;
  }
  return(tsfcastList); 
}

##This is the function that computes the forecasts. The estimation method used is the seasonal ARIMA(p,d,q)(P,D,Q, n)
generateSARIMAForecasts <- function(inputTSDecompList, inputArimaOrder, inputSeasonalOrder, inputNumberOfSeasonPeriods, 
                                    fcstHrzn, cnst, drft, confLev){ ##browser()
  #declare local variables
  tmpSTL <- NULL;
  fcasting <- NULL;
  tsfcastList <- NULL;
  counter <- 1;
  
  #while(counter <= length(inputTSDecompList)){
  while(counter <= length(inputTSDecompList)){
    #select the TS decompositions
    #tmpSTL <- inputTSDecompList[[counter]]$TimeSeriesDecomposition;
    tmpSTL <- inputTSDecompList[counter]$LVTSFITimeSeriesList$TimeSeries;
    
    #add the lattice plot to the list of plots
    if(cnst == FALSE & drft == FALSE){
      #fcasting <- forecast(forecastFunctionDriftConstant(tmpSTL, inputArimaOrder, fcstHrzn));
      
      # fit <- sarima(tmpSTL, inputArimaOrder[1], inputArimaOrder[2], inputArimaOrder[3],
      #               inputSeasonalOrder[1], inputSeasonalOrder[2], inputSeasonalOrder[3],
      #               inputNumberOfSeasonPeriods, details = FALSE);
      # fcasting <- forecast(fit$fit,h=fcstHrzn,level=confLev);
      fit <- Arima(tmpSTL, order=inputArimaOrder, seasonal=list(order=inputSeasonalOrder,
                                                           period=inputNumberOfSeasonPeriods),
                   include.constant = FALSE, include.drift = TRUE);
      fcasting <- forecast(fit,h=fcstHrzn,level=confLev);
      # fcasting <- forecast(tmpSTL, h=fcstHrzn,
      #                      forecastfunction=function(x,h,level, ...){
      #                        fit <- Arima(x, order=inputArimaOrder, seasonal=list(order=inputSeasonalOrder,
      #                                                                             period=inputNumberOfSeasonPeriods),
      #                                     include.constant = FALSE, include.drift = FALSE,
      #                                     method="ML")
      #                        # fit <- sarima(x, inputArimaOrder[1], inputArimaOrder[2], inputArimaOrder[3],
      #                        #               inputSeasonalOrder[1], inputSeasonalOrder[2], inputSeasonalOrder[3],
      #                        #               inputNumberOfSeasonPeriods, no.constant = FALSE)
      #                        #note including the constant has no impact with the drift included
      #                        #with just the constant the forecasts are far too low
      #                        #with drift the forecasts are similar to previous monthly/annual but with wider bands
      #                        return(forecast(fit,h=fcstHrzn,level=confLev, ...))
      #                        }
      #                      );
    }
    fcastCoefs <- fcasting$model$coef;
    fcstValues <- fcasting;
    fcstErrors <- residuals(fcasting);
    modAccuracy <- as.data.table(accuracy(fcasting));
    bjTest <- Box.test(fcstErrors, lag=16, fitdf=4, type="Ljung-Box");
    fcastSummary <- summary(fcasting);
    
    
    #add the forecast results to the forecast list
    tsfcastList[[counter]] <- list(FinancialInstitution=
                                     inputTSDecompList[counter]$LVTSFITimeSeriesList$FinancialInstitution, 
                                   ForecastCoefficients=fcastCoefs,
                                   ForecastedSeries=fcstValues,
                                   ForecastErrors=fcstErrors,
                                   ForecastModelAccuracy=modAccuracy,
                                   ForecastSummary=fcastSummary,
                                   BoxLjungTest=bjTest);
    counter <- counter+1;
  }
  print("generateARIMAForecasts: complete")
  return(tsfcastList); 
}



##This is the function that computes the forecasts. The estimation method used is the seasonal ARIMA(p,d,q)(P,D,Q)
generateSARIMAForecastsWithDummy <- function(inputTSDecompList, dummyVector, inputArimaOrder, inputSeasonalOrder, 
                                             inputNumberOfSeasonPeriods, fcstHrzn, cnst, drft, confLev){
  #declare local variables
  tmpSTL <- NULL;
  fcasting <- NULL;
  tsfcastList <- NULL;
  counter <- 1;
  #print("entering while loop");
  #print(inputTSDecompList);
  #print(dummyVector);
  while(counter <= length(inputTSDecompList)){
    #select the TS decompositions
    tmpSTL <- inputTSDecompList[[counter]]$TimeSeriesDecomposition;
    
    #print(tmpSTL);
    print("entering if constant and drift are true statement");
    #add the lattice plot to the list of plots
    if(cnst == TRUE & drft == TRUE){
      #fcasting <- forecast(forecastFunctionDriftConstant(tmpSTL, inputArimaOrder, fcstHrzn));
      
      fcasting <- forecast(tmpSTL, h=fcstHrzn, 
                           forecastfunction=function(x,h,level, ...){
                             fit <- Arima(x, xreg=dummyVector, order=inputArimaOrder, 
                                          seasonal=list(order=inputSeasonalOrder, period=inputNumberOfSeasonPeriods),
                                          include.constant = TRUE, include.drift = TRUE) 
                             #note including the constant has no impact with the drift included
                             #with just the constant the forecasts are far too low
                             #with drift the forecasts are similar to previous monthly/annual but with wider bands
                             return(forecast(fit, h=fcstHrzn,level=confLev,xreg=dummyVector,...))});
    }
    fcastCoefs <- fcasting$model$coef;
    fcstValues <- fcasting;
    fcstErrors <- residuals(fcasting);
    modAccuracy <- as.data.table(accuracy(fcasting));
    bjTest <- Box.test(fcstErrors, lag=16, fitdf=4, type="Ljung-Box");
    fcastSummary <- summary(fcasting);
    
    
    print("adding the forecast results to the forecast list");
    #add the forecast results to the forecast list
    tsfcastList[[counter]] <- list(FinancialInstitution=inputTSDecompList[[counter]]$FinancialInstitution, 
                                   ForecastCoefficients=fcastCoefs,
                                   ForecastedSeries=fcstValues,
                                   ForecastErrors=fcstErrors,
                                   ForecastModelAccuracy=modAccuracy,
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
    #assign(paste(savePath,inputForecastsList[[counter]]$FinancialInstitution,measurementUnitTypeString,sep=""), 
    ##(dtList[[counter]]));
    counter <- counter + 1;
  }
  
  return(dtList);
}


##this function is used to export the point forecasts errors into an excel file by FI
exportListOfForecastErrorsToExcel <- function(inputForecastsList, measUnitType, savePath){
  #declare local variables
  counter = 1;
  dtList <- NULL;
  fileLocation <- NULL;
  
  #set values for file naming, y-axis labels and main plot title
  if(measUnitType %in% c("volume","Volume","Vol","VOLUME")){
    measurementUnitTypeString = "TxnVolumeForecastErrors.xlsx";
  } else if(measUnitType %in% c("value","Value","Val","VALUE")){
    measurementUnitTypeString = "TxnValueForecastErrors.xlsx";
  }
  
  while(counter<=length(inputForecastsList)){
    dtList[[counter]] <- as.data.table(inputForecastsList[[counter]]$ForecastErrors);
    fileLocation <- paste(savePath, inputForecastsList[[counter]]$FinancialInstitution, measurementUnitTypeString, sep="");
    write.xlsx2(dtList[[counter]], file = fileLocation);
    #assign(paste(savePath,inputForecastsList[[counter]]$FinancialInstitution,measurementUnitTypeString,sep=""), 
    ##(dtList[[counter]]));
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
    #assign(paste(savePath,inputForecastsList[[counter]]$FinancialInstitution,measurementUnitTypeString,sep=""), 
    ##(dtList[[counter]]));
    counter <- counter + 1;
  }
  
  return(dtList);
}


##This function creates and stores charts of the volume/value forecast results
exportForecastChartList <- function(inputForecastsList, dataFrequency, measUnit, measUnitType, savePath){
  #declare local variables
  counter = 1;
  ctList <- NULL;
  cht <- NULL;
  fileLocation <- NULL;
  
  
  #set the value of the measurment unit as reported in the chart
  if(measUnit==1000000){
    measurementUnitString = "(millions)";
  }else if(measUnit== 1000000000){
    measurementUnitString = "(billions)";
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
    mainTitleLeadingString = "LVTS Transactions Volume Forecast for";
  } else if(measUnitType %in% c("value","Value","Val","VALUE")){
    measurementUnitTypeString = "Transactions Value";
    measurementUnitTypeSaveString = "TransactionsValueForecast.png";
    mainTitleLeadingString = "LVTS Transactions Value Forecast for";
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
    extractedLVTSFITimeSeriesLoadPath <- 
      tsFreqString <- "Year";
    #if daily   
  }else if(dataFrequency == "daily"){
    extractedLVTSFITimeSeriesLoadPath <- 
      tsFreqString <- "date";
  }
  
  while(counter<=length(inputForecastsList)){
    fileLocation <- paste(savePath, inputForecastsList[[counter]]$FinancialInstitution, 
                          measurementUnitTypeSaveString, sep="");
    png(fileLocation);
    cht <- autoplot(inputForecastsList[[counter]]$ForecastedSeries, las=1,
               # main=paste(mainTitleLeadingString, inputForecastsList[[counter]]$FinancialInstitution),
               main=paste(""),
                xlab=tsFreqString, ylab=paste(measurementUnitTypeString, measurementUnitString, sep = " "), cex.axis=0.75)
    ctList[[counter]] <- cht;
    print(cht);#printing into the plots display window is required so as to allow the storing of the plot object
    dev.off();#captures the chart in the plots display window and then clears the window
    #assign(paste(savePath,inputForecastsList[[counter]]$FinancialInstitution,measurementUnitTypeString,sep=""), 
    ##(dtList[[counter]]));
    counter <- counter + 1;
  }
  
  return(ctList);
}


##This function is used to remove (or set to zero) all point forecasts after the forecast horizen
##It is needed because including the dummy variable causes forecasts to be generated for a period equaling the length of
##the time sereis being forecasted 
removeForecastsAfterHorizen <- function(inputForecastsList, fcstHorizen){
  
  counter = 1;#counter to index each element of the inputForecastsList
  
  #loop through each element of the inputForecastsList and for each point forecast and lower and upper bound set to zero
  while(counter<=length(inputForecastsList)){
    startIndex <- fcstHorizen + 1;#initialise the start index to that of the index imediately after the forecast horizen
    while(startIndex<=length(inputForecastsList[[counter]]$ForecastedSeries$mean)){
      inputForecastsList[[counter]]$ForecastedSeries$mean[startIndex] <- NA;
      inputForecastsList[[counter]]$ForecastedSeries$lower[startIndex] <- 0;
      inputForecastsList[[counter]]$ForecastedSeries$upper[startIndex] <- 0;
      startIndex <- startIndex + 1; # increment the index and repeat
    }    
    counter <- counter + 1;#increment the counter and repeat until inputForecastsList is exhasted
  }
  return(inputForecastsList);
}

gc();

