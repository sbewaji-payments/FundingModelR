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


options(scipen=999)



startYr =2000;
endYr = 2019;

##ACSS
extractedACSSDataLoadPath <- c(paste("C:/Projects/FundingDataTables/Cleaned Transactions Data/acssusbeTXNsSentRecievedByFIQuarterlySeries",
                                     startYr,"-",endYr,".RData", sep = "", collapse = NULL));
load(extractedACSSDataLoadPath)

##LVTS
extractedLVTSDataLoadPath <- extractedLVTSDataLoadPath <- c(paste("C:/Projects/FundingDataTables/Cleaned Transactions Data/lvtsTXNsSentRecievedByFIQuarterlySeries",
                                                                  startYr,"-",endYr,".RData", sep = "", collapse = NULL));
load(extractedLVTSDataLoadPath)


##ACSS By Stream
extractedLVTSDataLoadPath <- c(paste("C:/Projects/FundingDataTables/Cleaned Transactions Data/acssStreamTXNsSentRecievedByFIQuarterlySeries",
                                     startYr,"-",endYr,".RData", sep = "", collapse = NULL));
load(extractedLVTSDataLoadPath)

##LVTS
BMOLVTSTXNsDataSeriesSentRecieved <- LVTSTXNsDataSeriesSentRecieved[sender=="BOFMCA",];
write.csv(BMOLVTSTXNsDataSeriesSentRecieved, file="BMOLVTSTXNsDataSeriesSentRecieved.csv");
BMOLVTSTXNsDataSeriesSentRecieved$YQ <- as.yearqtr(BMOACSSTXNsDataSeriesSentRecieved$date);
ggplot(BMOLVTSTXNsDataSeriesSentRecieved, aes(date, TotalVolume)) + geom_line()
+ geom_point();


##ACSS
BMOACSSTXNsDataSeriesSentRecieved <- ACSSTXNsDataSeriesSentRecieved[sender=="BOFMCA",];
BMOACSSTXNsDataSeriesSentRecieved$YQ <- as.yearqtr(BMOACSSTXNsDataSeriesSentRecieved$date);
ggplot(BMOACSSTXNsDataSeriesSentRecieved, aes(date, TotalVolume,colour=stream)) + geom_line()
+ geom_point();

BMOACSSTXNsDataSeriesSentRecievedYQ <- BMOACSSTXNsDataSeriesSentRecieved[,{TotalYQVolume = 
  sum(TotalVolume)},by=c("stream", "senderFullName", "senderID", "sender","YQ")];

load("C:/Projects/FundingDataTables/ACSSStreamData.Rdata");

BMOACSSTXNsDataSeriesSentRecievedYQ <- merge(BMOACSSTXNsDataSeriesSentRecievedYQ, 
                                             ACSSStreamData, by.x = "stream", by.y = "streamID");

ggplot(BMOACSSTXNsDataSeriesSentRecievedYQ, aes(YQ, TotalYQVolume,colour=StreamDescription)) 
+ geom_line() + geom_point();

BMOACSSTXNsDataSeriesSentRecievedYQ2010 <- BMOACSSTXNsDataSeriesSentRecievedYQ[year(YQ)>2009,];

ggplot(BMOACSSTXNsDataSeriesSentRecievedYQ2010, aes(YQ, TotalYQVolume,colour=StreamDescription)) 
+ geom_line() + geom_point()+ scale_x_yearqtr(format="%YQ%q", n=5);




##ACSS By Stream
BMOACSSTXNsDataSeriesSentRecieved <- ACSSTXNsDataSeriesSentRecieved[sender=="BOFMCA",];
BMOACSSTXNsDataSeriesSentRecieved$Year_Quarter <- as.yearqtr(BMOACSSTXNsDataSeriesSentRecieved$Year_Quarter);
BMOACSSTXNsDataSeriesSentRecieved20182019 <-  BMOACSSTXNsDataSeriesSentRecieved[year(Year_Quarter)%in% c(2018,2019),];
BMOACSSTXNsDataSeriesSentRecieved20182019 <-  BMOACSSTXNsDataSeriesSentRecieved20182019[,c("ShareSentVolume","ShareSentValue","ShareRecievedVolume","ShareRecievedValue"):=NULL];

ggplot(BMOACSSTXNsDataSeriesSentRecieved20182019, aes(Year_Quarter, TotalVolume,colour=stream)) + geom_line()
+ geom_point()+ scale_x_yearqtr(format="%YQ%q", n=5);

ggplot(BMOACSSTXNsDataSeriesSentRecieved20182019, aes(Year_Quarter, TotalValue,colour=stream)) + geom_line()
+ geom_point()+ scale_x_yearqtr(format="%YQ%q", n=5);


