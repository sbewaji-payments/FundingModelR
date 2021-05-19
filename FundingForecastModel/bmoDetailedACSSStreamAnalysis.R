##the usual
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
require(plotly);#3-party package to export data into excel xlsx format
require(lattice);#3-party package to plot data
require(forecast);#3-party package to forecast and plot seasonally adjusted data

##Varriables
startYr <- 2000;
endYr <- 2018;
minObservations <- 4; #minimum number of observations required for the collected time series
keyField <- "stream"; #the colum to be used as the unique key forthe data table (can be "sender" or "reciever")'
dataFrequency <- "quarterly"; #the options are monthly, quarterly, annual, daily (does not make sense since it does not distingush the year)
measurementUnitType <- "Volume";##Volume or Value
measurementUnit <- 1000000; #possible values 1000000, 1000, or 1
forecastDate <- Sys.Date();##Set date the forecast was run on. This will be used to save the forecast outups to prevent
studiedFI <- "BOFMCA";

##Data save path
bmoDataLoadPath <- c("C:/Projects/FundingDataTables/Cleaned Transactions Data/BMOACSSTXNsDataSeriesSentRecieved.RData");
RETAILStreamDataLoadPath <- c("C:/Projects/FundingDataTables/RETAILStreamData.Rdata");#will load the data table: ACSSStreamDataFile

 if(dataFrequency == "monthly"){
    minObservations <- 24; # 12*2 number of observations per period multiplied by 2 full periods of data minimum number of observations required for the collected time series
    ##The ARIMA analysis requires 2 full periods
    BMOACSSStreamTXNsDataSeriesDataTableSavePath <- 
      c(paste("C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/ExtractedTimeSeriesData/BMOACSSStreamMonthlyTimeSeriesData",
              startYr,"-",endYr,".RData", sep = "", collapse = NULL));
    FIOutputSavePath <- 
      c(paste("C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/DetailedStreamsCharts/Monthly/",
              studiedFI,"/", sep = "", collapse = NULL));
    ChartOutputSavePath <- 
      c(paste("C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/DetailedStreamsCharts/Monthly/",
              studiedFI,"/",forecastDate,"/", sep = "", collapse = NULL));
    dataFrequencyColumnName <- "Year_Month";
    #if quarterly  
  }else if(dataFrequency == "quarterly"){
    minObservations <- 8; # 4*2 number of observations per period multiplied by 2 full periods of data minimum number of observations required for the collected time series
    ##The ARIMA analysis requires 2 full periods
    BMOACSSStreamTXNsDataSeriesDataTableSavePath <- 
      c(paste("C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/ExtractedTimeSeriesData/BMOACSSStreamQuarterlyTimeSeriesData",
              startYr,"-",endYr,".RData", sep = "", collapse = NULL));
    FIOutputSavePath <- 
      c(paste("C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/DetailedStreamsCharts/Quarterly/",
              studiedFI,"/", sep = "", collapse = NULL));
    ChartOutputSavePath <- 
      c(paste("C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/DetailedStreamsCharts/Quarterly/",
              studiedFI,"/",forecastDate,"/", sep = "", collapse = NULL));
    dataFrequencyColumnName <- "Year_Quarter";
    #if annual 
  }else if(dataFrequency == "annual"){
    minObservations <- 2; #1*2 number of observations per period multiplied by 2 full periods of data minimum number of observations required for the collected time series
    ##The ARIMA analysis requires 2 full periods
   # save(ACSSUSBETXNsDataSeriesSentRecieved,file=extractedACSSUSBEDataLoadPath);
    BMOACSSStreamTXNsDataSeriesDataTableSavePath <- 
      c(paste("C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/ExtractedTimeSeriesData/BMOACSSStreamAnnualTimeSeriesData",
              startYr,"-",endYr,".RData", sep = "", collapse = NULL));
    
    dataFrequencyColumnName <- "Year";
    #if daily   
  }else if(dataFrequency == "daily"){
    minObservations <- 500; #250*2 number of observations per period multiplied by 2 full periods of data minimum number of observations required for the collected time series
    ##The ARIMA analysis requires 2 full periods
    # save(ACSSUSBETXNsDataSeriesSentRecieved,file=extractedACSSUSBEDataLoadPath);
    BMOACSSStreamTXNsDataSeriesDataTableSavePath <- 
      c(paste("C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/ExtractedTimeSeriesData/BMOACSSStreamDailyTimeSeriesData",
              startYr,"-",endYr,".RData", sep = "", collapse = NULL));

    dataFrequencyColumnName <- "date";
  }

####<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<RESULTS SAVE PATH DIRECTORY>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
dir.create(FIOutputSavePath);###Create the directory in which all analysis results and charts will be saved
dir.create(ChartOutputSavePath);###Create the directory in which all analysis results and charts will be saved


##Load Functions
gc();
print("Loading Used Functions");
source('C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/timeSeries_extraction_functions_V01.R', 
       encoding = 'UTF-8', echo=TRUE);
source('C:/Users/sbewaji/Documents/Development Workspaces/R-Workspace/FundingForecastModel/stream_timeSeries_analysis_functions_V01.R',
       encoding = 'UTF-8', echo=TRUE);
gc();


##load data
load(bmoDataLoadPath);
load(RETAILStreamDataLoadPath);


##convert to data.frame for merge
BMOACSSTXNsDataSeriesSentRecieved_df <- BMOACSSTXNsDataSeriesSentRecieved;
RETAILStreamData_df <- RETAILStreamData;

##merge on stream data
BMOACSSTXNsDataSeries <-  merge(BMOACSSTXNsDataSeriesSentRecieved_df, RETAILStreamData_df, by.x="stream", by.y="streamID", all.x = TRUE, all.y = TRUE);
gc(); #free up memory
BMOACSSTXNsDataSeries <- as.data.table(BMOACSSTXNsDataSeries);


##Extract Steams into time series
vectorColumnNames <- getColumnNames(BMOACSSTXNsDataSeries);
vectorStreamNames <- getListOfStreamNames(BMOACSSTXNsDataSeries, vectorColumnNames, "stream");
vectorOfTimeSeriesDates <- getListOfTImeSeriesDates(BMOACSSTXNsDataSeries, vectorColumnNames);
vectorOfTimeSeriesDates <- vectorOfTimeSeriesDates[!is.na(vectorOfTimeSeriesDates)];
vectorOfTimeSeriesDates <- as.yearqtr(vectorOfTimeSeriesDates);


print("creating timeSeries Tables/Collection");
print(now());#used to record speed of computations
#getFITimeSeriesDataTable <- function(inputDataTable, colNames, freqCol, seriesValue, keyFld)
#ferqCol can be "Year_Month", "Year_Quarter", "Year" "date"
BMOACSSStreamTXNsDataSeriesDataTable <- getStreamTimeSeriesDataTable(BMOACSSTXNsDataSeries,vectorColumnNames, 
                               dataFrequencyColumnName, "Volume", "stream",
                               minObservations);
BMOACSSStreamTXNsDataSeriesDataTable <- list.names(BMOACSSStreamTXNsDataSeriesDataTable,
                                                   "SingleFIACSSStreamTimeSeriesList");
list.save(BMOACSSStreamTXNsDataSeriesDataTable, BMOACSSStreamTXNsDataSeriesDataTableSavePath);
gc();#free up some memory



timeSeriesPlotsBMOACSSStreamTXNs <- createTSPlots(BMOACSSStreamTXNsDataSeriesDataTable,
                                                  measurementUnit, measurementUnitType,
                                                  dataFrequency, startYr, endYr);
gc();
seasonallyAdjustedTimeSeriesPlotsBMOACSSStreamTXNs <- 
  createSeasonallyAdjustedTSPlots(BMOACSSStreamTXNsDataSeriesDataTable, measurementUnit, 
                                  measurementUnitType, dataFrequency, startYr, endYr,
                                  ChartOutputSavePath);
