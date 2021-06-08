##Forecast ACSS Streams I and E
##clear system memory and workspace
gc();
rm(list=ls());
gc();

##import require libraries 
require(plyr);# contains method and function calls to be used to filter and select subsets of the data
require(lubridate);# handles that date elements of the data
require(dplyr);# contains method and function calls to be used to filter and select subsets of the data
require(data.table);#used to store and manipiulate data in the form of a data table similar to a mySQL database table
require(xts);#used to manipulate date-time data into quarters
require(ggplot2);#used to plot data
require(magrittr);#contains method/function calls to use for chaining e.g. %>%
require(scales);#component of ggplot2 that requires to be manually loaded to handle the scaling of various axies
require(foreach);#handles computations for each element of a vector/data frame/array/etc required the use of %do% or %dopar% operators in base R
require(iterators);#handles iteration through for each element of a vector/data frame/array/etc
require(stringr);#handles strings. Will be used to remove all quotation marks from the data
require(foreign);#to export data into formats readable by STATA and other statistical packages
require(xlsx);#3-party package to export data into excel xlsx format
require(timeDate);


##<<<<<<<<<<<<<<<<<<<<<<<<<DECLARE GLOBAL VARIABLES>>>>>>>>>>>>>>>>>>>>>>>>
##Declare all global variables to be used in this class
##Data extraction configiration information
##<<<<<<<<<<<<<<<<<<<<<<<<<DECLARE GLOBAL VARIABLES>>>>>>>>>>>>>>>>>>>>>>>>
##Declare all global variables to be used in this class
##Data extraction configiration information
##Declare all global variables to be used in this class
startYr <- 2015;
endYr <- 2017;


load("C:/Projects/FundingDataTables/Cleaned Transactions Data/acssStreamTXNsSentRecievedByFIMonthlySeries2015-2017.Rdata")

ACSSTXNsDataSeriesSentRecieved <- ACSSTXNsDataSeriesSentRecieved %>% mutate_if(is.factor, as.character);

ACSSTXNsDataSeriesSentRecieved <- as.data.table(ACSSTXNsDataSeriesSentRecieved);

ACSSTXNsDataSeriesSentRecievedStreamsPaper <- ACSSTXNsDataSeriesSentRecieved[
  (stream=="O" | stream=="I" |stream=="E"| stream=="L" | stream=="U" | stream=="G" | stream=="Z" ),];

ACSSTXNsDataSeriesSentRecievedStreamsPaperExculedImage <- ACSSTXNsDataSeriesSentRecieved[
  (stream=="E"| stream=="L" | stream=="U" | stream=="G" | stream=="Z" ),];


ACSSTXNsDataSeriesSentRecievedStreamImage <- ACSSTXNsDataSeriesSentRecievedStreamsPaper[stream=="O",];


ACSSTXNsDataSeriesSentRecievedStreamsPaperSum <- 
  ACSSTXNsDataSeriesSentRecievedStreamsPaper[, list(TotalVolume = sum(RecievedVolume,SentVolume), 
                                                 TotalValue = sum(RecievedValue,SentValue)), by=c("sender", "Year_Month")];

ACSSTXNsDataSeriesSentRecievedStreamsPaperExculedImageSum <- 
  ACSSTXNsDataSeriesSentRecievedStreamsPaperExculedImage[, list(TotalVolume = sum(RecievedVolume,SentVolume), 
                                                    TotalValue = sum(RecievedValue,SentValue)), by=c("sender", "Year_Month")];


ACSSTXNsDataSeriesSentRecievedStreamImageSum <- 
  ACSSTXNsDataSeriesSentRecievedStreamImage[, list(TotalVolume = sum(RecievedVolume,SentVolume), 
                                                 TotalValue = sum(RecievedValue,SentValue)), by=c("sender", "Year_Month")];


monthlyACSSTXNsDataSeriesSentRecievedStreamPaperImage <- 
  merge(ACSSTXNsDataSeriesSentRecievedStreamsPaperSum,ACSSTXNsDataSeriesSentRecievedStreamImageSum, by=c("sender","Year_Month"));


colnames(monthlyACSSTXNsDataSeriesSentRecievedStreamPaperImage)[colnames(monthlyACSSTXNsDataSeriesSentRecievedStreamPaperImage)=="TotalVolume.x"] <- "PaperTotalVolume";
colnames(monthlyACSSTXNsDataSeriesSentRecievedStreamPaperImage)[colnames(monthlyACSSTXNsDataSeriesSentRecievedStreamPaperImage)=="TotalValue.x"] <- "PaperTotalValue";
colnames(monthlyACSSTXNsDataSeriesSentRecievedStreamPaperImage)[colnames(monthlyACSSTXNsDataSeriesSentRecievedStreamPaperImage)=="TotalVolume.y"] <- "ImageTotalVolume";
colnames(monthlyACSSTXNsDataSeriesSentRecievedStreamPaperImage)[colnames(monthlyACSSTXNsDataSeriesSentRecievedStreamPaperImage)=="TotalValue.y"] <- "ImageTotalValue";


monthlyACSSTXNsDataSeriesSentRecievedStreamPaperImage[,imageShare:=(ImageTotalVolume/PaperTotalVolume), by=c("sender", "Year_Month")];


monthlyACSSTXNsDataSeriesSentRecievedStreamsPaperDataPath <- 
  c(paste("C:/Projects/FundingDataTables/Cleaned Transactions Data/monthlyACSSTXNsDataSeriesSentRecievedStreamsPaper",
          startYr,"-",endYr,".Rdata", sep = "", collapse = NULL));

monthlyACSSTXNsDataSeriesSentRecievedStreamImageDataPath <- 
  c(paste("C:/Projects/FundingDataTables/Cleaned Transactions Data/monthlyACSSTXNsDataSeriesSentRecievedStreamImage",
          startYr,"-",endYr,".Rdata", sep = "", collapse = NULL));

monthlyACSSTXNsDataSeriesSentRecievedStreamsPaperSumDataPath <- 
  c(paste("C:/Projects/FundingDataTables/Cleaned Transactions Data/monthlyACSSTXNsDataSeriesSentRecievedStreamsPaperSum",
          startYr,"-",endYr,".Rdata", sep = "", collapse = NULL));

monthlyACSSTXNsDataSeriesSentRecievedStreamsPaperSumDataPath <- 
  c(paste("C:/Projects/FundingDataTables/Cleaned Transactions Data/monthlyACSSTXNsDataSeriesSentRecievedStreamsPaperSum",
          startYr,"-",endYr,".Rdata", sep = "", collapse = NULL));

monthlyACSSTXNsDataSeriesSentRecievedStreamsPaperExculedImageSumDataPath <- 
  c(paste("C:/Projects/FundingDataTables/Cleaned Transactions Data/monthlyACSSTXNsDataSeriesSentRecievedStreamsPaperExculedImageSum",
          startYr,"-",endYr,".Rdata", sep = "", collapse = NULL));

monthlyACSSTXNsDataSeriesSentRecievedStreamPaperImageDataPath <- 
  c(paste("C:/Projects/FundingDataTables/Cleaned Transactions Data/monthlymonthlyACSSTXNsDataSeriesSentRecievedStreamPaperImage",
          startYr,"-",endYr,".Rdata", sep = "", collapse = NULL));

save(ACSSTXNsDataSeriesSentRecievedStreamsPaper, file=monthlyACSSTXNsDataSeriesSentRecievedStreamsPaperDataPath);

save(ACSSTXNsDataSeriesSentRecievedStreamImage, file=monthlyACSSTXNsDataSeriesSentRecievedStreamImageDataPath);

save(ACSSTXNsDataSeriesSentRecievedStreamsPaperSum, file=monthlyACSSTXNsDataSeriesSentRecievedStreamsPaperSumDataPath);

save(ACSSTXNsDataSeriesSentRecievedStreamsPaperExculedImageSum, file=monthlyACSSTXNsDataSeriesSentRecievedStreamsPaperExculedImageSumDataPath);

save(ACSSTXNsDataSeriesSentRecievedStreamImageSum, file=monthlyACSSTXNsDataSeriesSentRecievedStreamImageSumDataPath);

save(monthlyACSSTXNsDataSeriesSentRecievedStreamPaperImage, file=monthlyACSSTXNsDataSeriesSentRecievedStreamPaperImageDataPath);
