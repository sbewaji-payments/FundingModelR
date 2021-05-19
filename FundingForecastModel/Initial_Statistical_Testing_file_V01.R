##This is class to plot and carry out initial statistica analysis on the ACSS and LVTS transactions data
##
## Author : Segun Bewaji
## Modified : Segun Bewaji
## Modifications Made:
##        1:  
##           NOTE: 
##        2: 
##           NOTE: 
##        3: 
##
##
## Creation Date : 17 February 2016
## Modification Date : 
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

##<<<<<<<<<<<<<<<<<<<<<<<<<DECLARE GLOBAL VARIABLES>>>>>>>>>>>>>>>>>>>>>>>>
##Declare all global variables to be used in this class

dataFrequency <-  NULL;
ACSSTransDataSeriesLoadPath <- NULL;
LVTSTransDataSeriesLoadPath <- NULL;


receiverFIACSSTotalVolumeSeriesLinePlot <- NULL;
receiverFIACSSTotalValueSeriesLinePlot <- NULL;

receiverFILVTSTotalValueSeriesLinePlot <- NULL;
receiverFILVTSTotalVolumeSeriesLinePlot <- NULL;

senderFIACSSTotalVolumeSeriesLinePlot <- NULL;
senderFIACSSTotalValueSeriesLinePlot <- NULL;

senderFILVTSTotalValueSeriesLinePlot <- NULL;
senderFILVTSTotalVolumeSeriesLinePlot <- NULL;


##<<<<<<<<<<<<<<<<<<<<<<<<<LOAD DATA>>>>>>>>>>>>>>>>>>>>>>>>
##Now load the individual FI's complete data tables
print("Loading data table");#used to record speed of computations
print(now());#used to record speed of computations
dataFrequency <- "monthly"; #the options are monthly, quarterly, annual, daily (does not make sense since it does not distingush the year)

##load the data based on the required data frequency
if(dataFrequency == "monthly"){
  ACSSTransDataSeriesLoadPath <- c("C:/Projects/FundingDataTables/monthlyACSSTransDataSeries.Rdata");
  LVTSTransDataSeriesLoadPath <- c("C:/Projects/FundingDataTables/monthlyLVTSTransDataSeries.Rdata");
}else if(dataFrequency == "quarterly"){
  ACSSTransDataSeriesLoadPath <- c("C:/Projects/FundingDataTables/quarterlyACSSTransDataSeries.Rdata");
  LVTSTransDataSeriesLoadPath <- c("C:/Projects/FundingDataTables/quarterlyLVTSTransDataSeries.Rdata");
}else if(dataFrequency == "annual"){
  ACSSTransDataSeriesLoadPath <- c("C:/Projects/FundingDataTables/annualACSSTransDataSeries.Rdata");
  LVTSTransDataSeriesLoadPath <- c("C:/Projects/FundingDataTables/annualLVTSTransDataSeries.Rdata");
}

#Load the transaction files
load(ACSSTransDataSeriesLoadPath);#load ACSS TXNs
load(LVTSTransDataSeriesLoadPath);#load LVTS TXNs
gc();
print("data table loaded");#used to record speed of computations
print(now());#used to record speed of computations
gc();

 
  ##Sender Specific charts
  
  senderFILVTSPaymentsVolumeBoxPlot <- ggplot(senderFILVTSVolumeValueSummaryStatisticsMonthlyByYear, aes(x=sender, y=Total_Payments_Volume, color=sender)) +
    geom_boxplot();
  
  senderFILVTSPaymentsValueBoxPlot <- ggplot(senderFILVTSVolumeValueSummaryStatisticsMonthlyByYear, aes(x=sender, y=Total_Payments_Value, color=sender)) +
    geom_boxplot() + scale_y_continuous(label=dollar);
  
  senderFILVTSMeanVolumeSeriesLinePlot <- ggplot(senderFILVTSVolumeValueSummaryStatisticsMonthlyByYear, aes(x=Year_Month, y=Mean_Payments_Volume, color=sender)) +
    geom_line(aes(order = sample(seq_along(Year_Month)))) + facet_wrap(~ sender);
  
  senderFILVTSMeanValueSeriesLinePlot <- ggplot(senderFILVTSVolumeValueSummaryStatisticsMonthlyByYear, aes(x=Year_Month, y=Mean_Payments_Value, color=sender)) +
    geom_line(aes(order = sample(seq_along(Year_Month))))+ scale_y_continuous(label=dollar) + facet_wrap(~ sender);
  
  senderFILVTSTotalVolumeSeriesLinePlot <- ggplot(senderFILVTSVolumeValueSummaryStatisticsMonthlyByYear, 
                                                  aes(x=Year_Month, y=Total_Payments_Volume, color=sender)) +
    geom_line(aes(order = sample(seq_along(Year_Month)))) + facet_wrap(~ sender);
  
  senderFILVTSTotalValueSeriesLinePlot <- ggplot(senderFILVTSVolumeValueSummaryStatisticsMonthlyByYear, 
                                                 aes(x=Year_Month, y=Total_Payments_Value, color=sender)) +
    geom_line(aes(order = sample(seq_along(Year_Month)))) + scale_y_continuous(label=dollar) + 
    facet_wrap(~ sender);
  
  
  gc();#free up momory
  
  print("publishing charts Monthly Frequency");
  print(now());#used to record speed of computations
  ##Display/Print charts
  print(monthlyPaymentsValueRecievedRangeScatterPlot);
  print(monthlyPaymentsValueSentRangeScatterPlot);
  
  print(monthlyPaymentsVolumeRecievedRangeScatterPlot);
  print(monthlyPaymentsVolumeSentRangeScatterPlot);
  
  print(senderFILVTSPaymentsValueBoxPlot);
  print(senderFILVTSPaymentsVolumeBoxPlot);
  
  print(senderFILVTSMeanValueSeriesLinePlot);
  print(senderFILVTSMeanVolumeSeriesLinePlot);
  
  print(senderFILVTSTotalValueSeriesLinePlot);
  print(senderFILVTSTotalVolumeSeriesLinePlot);
  
  gc();
  
}else{
  ##<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<VISUALISING THE DAILY DATA>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
  
  print("This file only handles monthly frequency analysis. For daily analysis, use lvts_Initial_Statistical_Testing_file_V03_Year_Month_Day.R file");
  print(now());#used to record speed of computations
  gc();#free up some memory
}



print("Build and Processing Complete");
print(now());
gc();