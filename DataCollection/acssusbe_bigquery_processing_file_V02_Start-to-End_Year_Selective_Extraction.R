gc();
rm(list=ls());
gc();



##Make the list of packages to be used in this script
usedPackagesList <- c("bigrquery","DBI","stringi","stringr","ggplot2","tidyr","dplyr","gghighlight",
                      "scales","extrafont","zoo","rvest","tidyverse","gridExtra","grid","lubridate",
                      "ggrepel", "readr", "bdscale", "bizdays", "data.table", "Quandl", 
                      "quantmod",  "timeDate", "zoo", "ggsci", "purrr", "tibble","gtools"
                      ,"lmtest", "tseries", "urca" ,"forecast","timetk","xlsx","foreign","iterators","foreach","magrittr","xts"
);

##check to see if required packages are installed.
#if not install and load them them
##else looad them
checkPagagesInstalled <- lapply(
  usedPackagesList,
  FUN = function(x){
    if(!require(x,character.only = TRUE)){
      install.packages(x, dependencies = TRUE);
      require(x,character.only = TRUE)
    }
  }
);



##Define file save locations
##Manual TXN File for Funding Model
##Will need to change the intervals based on timing of when the file is being pulled and
##rename the 
startyear  <- 2021; ##ACSS and USBE Data in BigQuery only starts in mid-2012. So start year for bigquery extraction is set to 2021 and the processed data will be 
                    ##merged with the existing data files from 2000 to 2020
endyear <- 2021


##Old txt data start and end year
oldDataStartYr <- 2000

oldDataEndYr <- 2020

workingDataStartYr <- 2001

##Old txt file data
extractedACSSDataLoadPath <- c(paste("E:/Projects/FundingDataTables/Raw Transactions Data/YearsSpecificACSSTXNsYM", oldDataStartYr,"-",oldDataEndYr,"DataSeries.Rdata", sep = "", collapse = NULL));
extractedUSBEDataLoadPath <- c(paste("E:/Projects/FundingDataTables/Raw Transactions Data/YearsSpecificUSBETXNsYM", oldDataStartYr,"-",oldDataEndYr,"DataSeries.Rdata", sep = "", collapse = NULL));




ACSSUSBERawBigQueryDataExtractSavePath <- 
  c(paste("E:/Projects/FundingDataTables/Raw Transactions Data/ACSSUSBERawBigQueryData",startyear,"-",endyear,".RData", sep = "", collapse = NULL));


mergedACSSTXNsYearSpecificBigQueryExtractSavePath <- 
  c(paste("E:/Projects/FundingDataTables/Raw Transactions Data/mergedACSSTXNsYearSpecificBigQueryData",startyear,"-",endyear,".RData", sep = "", collapse = NULL));


mergedUSBETXNsYearSpecificBigQueryExtractSavePath <- 
  c(paste("E:/Projects/FundingDataTables/Raw Transactions Data/mergedUSBETXNsYearSpecificBigQueryData",startyear,"-",endyear,".RData", sep = "", collapse = NULL));



mergedACSSTXNsYearSpecificTXTAndBigQueryExtractSavePath <- 
  c(paste("E:/Projects/FundingDataTables/Raw Transactions Data/YearsSpecificACSSTXNsYM",workingDataStartYr,"-",endyear,"DataSeries.RData", sep = "", collapse = NULL));


mergedUSBETXNsYearSpecificTXTAndBigQueryExtractSavePath <- 
  c(paste("E:/Projects/FundingDataTables/Raw Transactions Data/YearsSpecificUSBETXNsYM",workingDataStartYr,"-",endyear,"DataSeries.RData", sep = "", collapse = NULL));


# 
# monthlyACSSCollateralPoolExtractCSVSavePath <- 
#   c(paste("E:/Projects/FundingDataTables/Raw Transactions Data/monthlyACSSCollateralPool_",startyear,"-",endyear,".csv", sep = "", collapse = NULL));




# load("G:/Development Workspaces/R-Workspace/SystemAndStreamMigration/ExtractedTimeSeriesData/
#      ACSS Collateral Pool Data/Stream Migration Collateral Data/2019 Model Core Data/acssCollPool20062018.RData");

##Define BigQuery connection parameters
bQConnection <- dbConnect(
  bigrquery::bigquery(),
  project = "prod-data-storage-prj", #Database server conection name
  dataset = "acss_marts", #database schema name (data source to query data from)
  billing = "acs-research-prj" #Database connection user/owner name (billing refers to who the onnection is billed to)
)

##Open the connect to BigQuery
bQConnection;
1 ##option selection
##Test that the connection has been successful by listing the aviable tables
dbListTables(bQConnection);
1 ##Option selection

##List/write all the SQL queries to run in BigQuery using the connection 
# monthlyPayTransExtSQL <- "SELECT *
# FROM `prod-lvtsdata-prj.lvts_boc.transaction_t1_boc8`
# WHERE cycle_date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 40 DAY) AND DATE_SUB(CURRENT_DATE(), INTERVAL 9 DAY)
# ORDER BY cycle_date ASC;"


# ACSSUSBERawBigQueryDataExtSQL <- c(paste("SELECT *
#   FROM `prod-data-storage-prj.acss_marts.vol_val_fact` WHERE settlement_year > ", (startyear-1), "
#  AND settlement_year < ", (endyear+1), "
#  AND DATE_TRUNC(CURRENT_DATE(), MONTH) > settlement_date 
# ORDER BY settlement_date ASC;", sep = "", collapse = NULL));



ACSSUSBERawBigQueryDataExtSQL <- c(paste(
"SELECT 
  S.systemCode,
  S.date,
  S.stream,
  S.year,
  S.sender,
  S.senderID,
  S.receiver,
  S.receiverID,
  S.streamName,
  S.volumeSent,
  S.valueSent,
  R.volumeRecieved,
  R.valueRecieved
FROM
(
SELECT 
  system_code As systemCode,
  settlement_date AS date,
  settlement_year AS year,
  sending_inst_current_name_en AS sender,
  sending_inst_irn AS senderID,
  receiving_inst_current_name_en AS receiver,
  receiving_inst_irn AS receiverID,
  stream_code AS stream,
  current_stream_name_en AS streamName,
  SUM(volume) AS volumeSent,
  SUM(value) AS valueSent
FROM prod-data-storage-prj.acss_marts.exchange_vol_val_fact
WHERE settlement_year BETWEEN ", (startyear), " AND  ", (endyear), "
AND sending_inst_current_name_en != 'Payments Canada' AND receiving_inst_current_name_en != 'Payments Canada'
AND DATE_TRUNC(CURRENT_DATE(), MONTH) > settlement_date 
GROUP BY systemCode, date, stream, year, sender, senderID, receiver, receiverID, streamName
) S
JOIN
(
SELECT 
  system_code As systemCode,
  settlement_date AS date,
  settlement_year AS year,
  receiving_inst_current_name_en AS sender,
  receiving_inst_irn AS senderID,
  sending_inst_current_name_en AS receiver,
  sending_inst_irn AS receiverID,
  stream_code AS stream,
  current_stream_name_en AS streamName,
  SUM(volume) AS volumeRecieved,
  SUM(value) AS valueRecieved
FROM prod-data-storage-prj.acss_marts.exchange_vol_val_fact
WHERE settlement_year BETWEEN ", (startyear), " AND  ", (endyear), "
AND sending_inst_current_name_en != 'Payments Canada' AND receiving_inst_current_name_en != 'Payments Canada'
AND DATE_TRUNC(CURRENT_DATE(), MONTH) > settlement_date 
GROUP BY systemCode, date, stream, year, sender, senderID, receiver, receiverID, streamName
) R
ON
  S.systemCode=R.systemCode AND
  S.date=R.date AND
  S.year=R.year AND
  S.sender=R.sender AND
  S.senderID=R.senderID AND
  S.receiver=R.receiver AND
  S.receiverID=R.receiverID AND
  S.stream=R.stream AND
  S.streamName=R.streamName
ORDER BY systemCode, date, stream, year, senderID, sender, receiverID, receiver"
, sep = "", collapse = NULL));



###Pull Previous Day's BoC extracts data from BigQuery into respective data.table objects t osave to file
ACSSUSBERawBigQueryData <- as.data.table(dbGetQuery(bQConnection, ACSSUSBERawBigQueryDataExtSQL));

ACSSRawBigQueryData <- ACSSUSBERawBigQueryData[systemCode =="A",];
USBERawBigQueryData <- ACSSUSBERawBigQueryData[systemCode =="U",];



gc();
mergedACSSTXNsYearSpecificBigQuery <- ACSSRawBigQueryData;

mergedACSSTXNsYearSpecificBigQuery$Year_Month <- as.yearmon(mergedACSSTXNsYearSpecificBigQuery$date);
mergedACSSTXNsYearSpecificBigQuery$Year_Month <- format(mergedACSSTXNsYearSpecificBigQuery$Year_Month, "%YM%m");#set the format for reporting of the quaters field
mergedACSSTXNsYearSpecificBigQuery$Year_Quarter <- as.yearqtr(mergedACSSTXNsYearSpecificBigQuery$date);#add a field to set the date-time in form of quarters
mergedACSSTXNsYearSpecificBigQuery$Year_Quarter <- format(mergedACSSTXNsYearSpecificBigQuery$Year_Quarter, "%YQ%q");#set the format for reporting of the quaters field
mergedACSSTXNsYearSpecificBigQuery$Year <- year(mergedACSSTXNsYearSpecificBigQuery$date);#add a field to set the date-time in form of years
mergedACSSTXNsYearSpecificBigQuery[,c("year", "systemCode", "sender", "receiver", "streamName"):=NULL];
#mergedACSSTXNsYearSpecificBigQuery$date <- as.POSIXct(mergedACSSTXNsYearSpecificBigQuery$date, format="%Y-%m-%d");

##Added this for command line validation of results from the merge 
# ACSSUSBETXNsDataSeriesSentRecieved <- ACSSUSBETXNsDataSeriesSentRecieved[order(ACSSUSBETXNsDataSeriesSentRecieved$Year_Quarter, ACSSUSBETXNsDataSeriesSentRecieved$sender];


##USBD Data Extraction
gc();
mergedUSBETXNsYearSpecificBigQuery <-USBERawBigQueryData;

mergedUSBETXNsYearSpecificBigQuery$Year_Month <- as.yearmon(mergedUSBETXNsYearSpecificBigQuery$date);
mergedUSBETXNsYearSpecificBigQuery$Year_Month <- format(mergedUSBETXNsYearSpecificBigQuery$Year_Month, "%YM%m");#set the format for reporting of the quaters field
mergedUSBETXNsYearSpecificBigQuery$Year_Quarter <- as.yearqtr(mergedUSBETXNsYearSpecificBigQuery$date);#add a field to set the date-time in form of quarters
mergedUSBETXNsYearSpecificBigQuery$Year_Quarter <- format(mergedUSBETXNsYearSpecificBigQuery$Year_Quarter, "%YQ%q");#set the format for reporting of the quaters field
mergedUSBETXNsYearSpecificBigQuery$Year <- year(mergedUSBETXNsYearSpecificBigQuery$date)#add a field to set the date-time in form of years
mergedUSBETXNsYearSpecificBigQuery[,c("year", "systemCode", "sender", "receiver", "streamName"):=NULL];
#mergedUSBETXNsYearSpecificBigQuery$date <- as.POSIXct(mergedUSBETXNsYearSpecificBigQuery$date, format="%Y-%m-%d");



print(now());#used to record speed of computations
load(extractedACSSDataLoadPath);
load(extractedUSBEDataLoadPath);
gc();
print("data tables loaded");#used to record speed of computations
print(now());#used to record speed of computations
gc();

##Make sure numeric values are not loaded as character
mergedACSSTXNsYearsSpecYM$valueRecieved <- as.double(mergedACSSTXNsYearsSpecYM$valueRecieved);
mergedACSSTXNsYearsSpecYM$volumeRecieved <- as.double(mergedACSSTXNsYearsSpecYM$volumeRecieved);
mergedACSSTXNsYearsSpecYM$valueSent <- as.double(mergedACSSTXNsYearsSpecYM$valueSent);
mergedACSSTXNsYearsSpecYM$volumeSent <- as.double(mergedACSSTXNsYearsSpecYM$volumeSent);
mergedACSSTXNsYearsSpecYM$date <- as.Date(mergedACSSTXNsYearsSpecYM$date)


mergedUSBETXNsYearsSpecYM$valueRecieved <- as.double(mergedUSBETXNsYearsSpecYM$valueRecieved);
mergedUSBETXNsYearsSpecYM$volumeRecieved <- as.double(mergedUSBETXNsYearsSpecYM$volumeRecieved);
mergedUSBETXNsYearsSpecYM$valueSent <- as.double(mergedUSBETXNsYearsSpecYM$valueSent);
mergedUSBETXNsYearsSpecYM$volumeSent <- as.double(mergedUSBETXNsYearsSpecYM$volumeSent);
mergedUSBETXNsYearsSpecYM$date <- as.Date(mergedUSBETXNsYearsSpecYM$date)
##merge bigquery data with old txt file data


##Pull only data from the working data start year 
mergedACSSTXNsYearsSpecYM<- mergedACSSTXNsYearsSpecYM[Year > workingDataStartYr-1, ];

mergedUSBETXNsYearsSpecYM <- mergedUSBETXNsYearsSpecYM[Year > workingDataStartYr-1, ];

gc();
mergedACSSTXNsYearsSpecYM <- rbind(mergedACSSTXNsYearsSpecYM, mergedACSSTXNsYearSpecificBigQuery);
mergedUSBETXNsYearsSpecYM <- rbind(mergedUSBETXNsYearsSpecYM, mergedUSBETXNsYearSpecificBigQuery);

##Save to file
save(ACSSUSBERawBigQueryData, file = ACSSUSBERawBigQueryDataExtractSavePath);
save(mergedACSSTXNsYearSpecificBigQuery, file = mergedACSSTXNsYearSpecificBigQueryExtractSavePath);
save(mergedUSBETXNsYearSpecificBigQuery, file = mergedUSBETXNsYearSpecificBigQueryExtractSavePath);


save(mergedACSSTXNsYearsSpecYM, file = mergedACSSTXNsYearSpecificTXTAndBigQueryExtractSavePath);
save(mergedUSBETXNsYearsSpecYM, file = mergedUSBETXNsYearSpecificTXTAndBigQueryExtractSavePath);

