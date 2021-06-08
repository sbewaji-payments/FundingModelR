gc();
rm(list=ls());
gc();


require(bigrquery);
require(DBI);
require(data.table);
require(lubridate);
require(timeDate);
require(dplyr);
require(gtools);
require(xts);#used to manipulate date-time data into quarters
require(ggplot2);#used to plot data
require(magrittr);#contains method/function calls to use for chaining e.g. %>%
require(scales);#component of ggplot2 that requires to be manually loaded to handle the scaling of various axies
require(foreach);#handles computations for each element of a vector/data frame/array/etc required the use of %do% or %dopar% operators in base R
require(iterators);#handles iteration through for each element of a vector/data frame/array/etc
require(stringr);#handles strings. Will be used to remove all quotation marks from the data

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
  c(paste("E:/Projects/FundingDataTables/Raw Transactions Data/YearsSpecificACSSTXNsYM",oldDataStartYr,"-",endyear,"DataSeries.RData", sep = "", collapse = NULL));


mergedUSBETXNsYearSpecificTXTAndBigQueryExtractSavePath <- 
  c(paste("E:/Projects/FundingDataTables/Raw Transactions Data/YearsSpecificUSBETXNsYM",oldDataStartYr,"-",endyear,"DataSeries.RData", sep = "", collapse = NULL));


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


ACSSUSBERawBigQueryDataExtSQL <- c(paste("SELECT *
  FROM `prod-data-storage-prj.acss_marts.vol_val_fact` WHERE settlement_year > ", (startyear-1), "
 AND settlement_year < ", (endyear+1), "
 AND DATE_TRUNC(CURRENT_DATE(), MONTH) > settlement_date 
ORDER BY settlement_date ASC;", sep = "", collapse = NULL));



###Pull Previous Day's BoC extracts data from BigQuery into respective data.table objects t osave to file
ACSSUSBERawBigQueryData <- as.data.table(dbGetQuery(bQConnection, ACSSUSBERawBigQueryDataExtSQL));

ACSSRawBigQueryData <- ACSSUSBERawBigQueryData[system_code =="A",];
USBERawBigQueryData <- ACSSUSBERawBigQueryData[system_code =="U",];



###ACSS Extraction
ACSSRawBigQueryDataSent <- ACSSRawBigQueryData[,  list(volume=sum(volume) , value=sum(value)), 
                                               by=c("settlement_date","stream_code","sending_inst_irn","receiving_inst_irn")];

ACSSRawBigQueryDataReceived <- ACSSRawBigQueryData[,  list(volume=sum(volume) , value=sum(value)), 
                                               by=c("settlement_date","stream_code","receiving_inst_irn","sending_inst_irn")];


#ACSSRawBigQueryDataReceived <- ACSSRawBigQueryDataSent


mergedACSSTXNsYearSpecificBigQuery <- as.data.table(inner_join(ACSSRawBigQueryDataSent, ACSSRawBigQueryDataReceived, by = c("sending_inst_irn" = "receiving_inst_irn", 
                                                                                                    # "receiving_inst_irn" = "sending_inst_irn",
                                                                                                     "settlement_date" = "settlement_date",
                                                                                                    "stream_code" = "stream_code"
                                                                                                    )))

gc();
mergedACSSTXNsYearSpecificBigQuery <- mergedACSSTXNsYearSpecificBigQuery[order(mergedACSSTXNsYearSpecificBigQuery$settlement_date, mergedACSSTXNsYearSpecificBigQuery$sending_inst_irn,
                                                                               mergedACSSTXNsYearSpecificBigQuery$receiving_inst_irn, mergedACSSTXNsYearSpecificBigQuery$stream_code),];

##data %>% rename(new_name1 = old_name1, new_name2 = old_name2, ....)
##2000-2020 extract names 
##[1] "date"           "stream"         "senderID"       "receiverID"     "volumeSent"     "valueSent"      "volumeRecieved" "valueRecieved"  "Year_Month"     "Year_Quarter"  
##[11] "Year" 
# mergedACSSTXNsYearSpecificBigQuery <- mergedACSSTXNsYearSpecificBigQuery %>% rename(date = settlement_date, senderID = sending_inst_irn, receiverID = receiving_inst_irn, stream = stream_code, 
#                                       volumeSent = volume.x, valueSent = value.x, volumeRecieved = volume.y, valueRecieved =  value.y);

setnames(mergedACSSTXNsYearSpecificBigQuery, old=c("settlement_date"), new= c("date"));
setnames(mergedACSSTXNsYearSpecificBigQuery, old=c("sending_inst_irn"), new= c("senderID"));
setnames(mergedACSSTXNsYearSpecificBigQuery, old=c("receiving_inst_irn"), new= c("receiverID"));
setnames(mergedACSSTXNsYearSpecificBigQuery, old=c("stream_code"), new= c("stream"));
setnames(mergedACSSTXNsYearSpecificBigQuery, old=c("volume.x"), new= c("volumeSent"));
setnames(mergedACSSTXNsYearSpecificBigQuery, old=c("value.x"), new= c("valueSent"));
setnames(mergedACSSTXNsYearSpecificBigQuery, old=c("volume.y"), new= c("volumeRecieved"));
setnames(mergedACSSTXNsYearSpecificBigQuery, old=c("value.y"), new= c("valueRecieved"));


#mergedACSSTXNsYearSpecificBigQuery <- as.data.table(mergedACSSTXNsYearSpecificBigQuery);

mergedACSSTXNsYearSpecificBigQuery$Year_Month <- as.yearmon(mergedACSSTXNsYearSpecificBigQuery$date);
mergedACSSTXNsYearSpecificBigQuery$Year_Month <- format(mergedACSSTXNsYearSpecificBigQuery$Year_Month, "%YM%m");#set the format for reporting of the quaters field
mergedACSSTXNsYearSpecificBigQuery$Year_Quarter <- as.yearqtr(mergedACSSTXNsYearSpecificBigQuery$date);#add a field to set the date-time in form of quarters
mergedACSSTXNsYearSpecificBigQuery$Year_Quarter <- format(mergedACSSTXNsYearSpecificBigQuery$Year_Quarter, "%YQ%q");#set the format for reporting of the quaters field
mergedACSSTXNsYearSpecificBigQuery$Year <- year(mergedACSSTXNsYearSpecificBigQuery$date)#add a field to set the date-time in form of years
#mergedACSSTXNsYearSpecificBigQuery$date <- as.POSIXct(mergedACSSTXNsYearSpecificBigQuery$date, format="%Y-%m-%d");

##Added this for command line validation of results from the merge 
# ACSSUSBETXNsDataSeriesSentRecieved <- ACSSUSBETXNsDataSeriesSentRecieved[order(ACSSUSBETXNsDataSeriesSentRecieved$Year_Quarter, ACSSUSBETXNsDataSeriesSentRecieved$sender];


##USBD Data Extraction
USBERawBigQueryDataSent <- USBERawBigQueryData[,  list(volume=sum(volume) , value=sum(value)), 
                                               by=c("settlement_date","stream_code","sending_inst_irn","receiving_inst_irn")];

USBERawBigQueryDataReceived <- USBERawBigQueryData[,  list(volume=sum(volume) , value=sum(value)), 
                                                   by=c("settlement_date","stream_code","receiving_inst_irn","sending_inst_irn")];




mergedUSBETXNsYearSpecificBigQuery <- as.data.table(inner_join(USBERawBigQueryDataSent, USBERawBigQueryDataReceived, by = c("sending_inst_irn" = "receiving_inst_irn", 
                                                                                                              # "receiving_inst_irn" = "sending_inst_irn",
                                                                                                              "settlement_date" = "settlement_date",
                                                                                                              "stream_code" = "stream_code"
)));

gc();
mergedUSBETXNsYearSpecificBigQuery <- mergedUSBETXNsYearSpecificBigQuery[order(mergedUSBETXNsYearSpecificBigQuery$settlement_date, mergedUSBETXNsYearSpecificBigQuery$sending_inst_irn,
                                                                               mergedUSBETXNsYearSpecificBigQuery$receiving_inst_irn, mergedUSBETXNsYearSpecificBigQuery$stream_code),];

##data %>% rename(new_name1 = old_name1, new_name2 = old_name2, ....)
##2000-2020 extract names 
##[1] "date"           "stream"         "senderID"       "receiverID"     "volumeSent"     "valueSent"      "volumeRecieved" "valueRecieved"  "Year_Month"     "Year_Quarter"  
##[11] "Year" 
# mergedUSBETXNsYearSpecificBigQuery <- mergedUSBETXNsYearSpecificBigQuery %>% rename(date = settlement_date, senderID = sending_inst_irn, receiverID = receiving_inst_irn, stream = stream_code, 
#                                                                                     volumeSent = volume.x, valueSent = value.x, volumeRecieved = volume.y, valueRecieved =  value.y);


setnames(mergedUSBETXNsYearSpecificBigQuery, old=c("settlement_date"), new= c("date"));
setnames(mergedUSBETXNsYearSpecificBigQuery, old=c("sending_inst_irn"), new= c("senderID"));
setnames(mergedUSBETXNsYearSpecificBigQuery, old=c("receiving_inst_irn"), new= c("receiverID"));
setnames(mergedUSBETXNsYearSpecificBigQuery, old=c("stream_code"), new= c("stream"));
setnames(mergedUSBETXNsYearSpecificBigQuery, old=c("volume.x"), new= c("volumeSent"));
setnames(mergedUSBETXNsYearSpecificBigQuery, old=c("value.x"), new= c("valueSent"));
setnames(mergedUSBETXNsYearSpecificBigQuery, old=c("volume.y"), new= c("volumeRecieved"));
setnames(mergedUSBETXNsYearSpecificBigQuery, old=c("value.y"), new= c("valueRecieved"));


mergedUSBETXNsYearSpecificBigQuery <- as.data.table(mergedUSBETXNsYearSpecificBigQuery);

mergedUSBETXNsYearSpecificBigQuery$Year_Month <- as.yearmon(mergedUSBETXNsYearSpecificBigQuery$date);
mergedUSBETXNsYearSpecificBigQuery$Year_Month <- format(mergedUSBETXNsYearSpecificBigQuery$Year_Month, "%YM%m");#set the format for reporting of the quaters field
mergedUSBETXNsYearSpecificBigQuery$Year_Quarter <- as.yearqtr(mergedUSBETXNsYearSpecificBigQuery$date);#add a field to set the date-time in form of quarters
mergedUSBETXNsYearSpecificBigQuery$Year_Quarter <- format(mergedUSBETXNsYearSpecificBigQuery$Year_Quarter, "%YQ%q");#set the format for reporting of the quaters field
mergedUSBETXNsYearSpecificBigQuery$Year <- year(mergedUSBETXNsYearSpecificBigQuery$date)#add a field to set the date-time in form of years
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

gc();
mergedACSSTXNsYearsSpecYM <- rbind(mergedACSSTXNsYearsSpecYM, mergedACSSTXNsYearSpecificBigQuery);
mergedUSBETXNsYearsSpecYM <- rbind(mergedUSBETXNsYearsSpecYM, mergedUSBETXNsYearSpecificBigQuery);

##Save to file
save(ACSSUSBERawBigQueryData, file = ACSSUSBERawBigQueryDataExtractSavePath);
save(mergedACSSTXNsYearSpecificBigQuery, file = mergedACSSTXNsYearSpecificBigQueryExtractSavePath);
save(mergedUSBETXNsYearSpecificBigQuery, file = mergedUSBETXNsYearSpecificBigQueryExtractSavePath);


save(mergedACSSTXNsYearsSpecYM, file = mergedACSSTXNsYearSpecificTXTAndBigQueryExtractSavePath);
save(mergedUSBETXNsYearsSpecYM, file = mergedUSBETXNsYearSpecificTXTAndBigQueryExtractSavePath);

