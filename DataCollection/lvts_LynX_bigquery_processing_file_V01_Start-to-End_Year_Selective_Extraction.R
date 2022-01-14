##THis script pulls Lynx data from bigQuery, saves that data locally and then merges the Lynx data to 
##previously saved historical data from LVTS that was pulled and saved locally using the following two scripts
## 1: lvts_bigquery_payments_transaction_processing_file_Level_1_V01.R
##
## 2: lvts_bigquery_payments_transaction_processing_file_Level_2_V01.R
##
## This script was redone to use BigQuery data exclusively because the previous LVTS data in the old txt files did not contain the 
## stream/payment message type code (i.e. MT100, MT103, MT205) consistently prior to 2013

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
lvtsDataStartYr <- 2001;
lvtsDataEndYr <- 2021;

lynxStatus  <-  "'Settled'";

lynxGoLiveDate <- as.Date("2021-08-28");##DO NOT CHANGE THIS REPRESENTS A FIXED POINT IN HISTORY
##For whatever reason LVTS was cut off on August 27, 2021 and Lynx started August 30, 2021. Perhaps they wanted to roll out over the weekend
##rather than run both systems in parallel until LVTS was turned off



##LVTS data file
LVTSRawBigQueryDataExtractLoadPath <- 
  c(paste("E:/Projects/FundingDataTables/Raw Transactions Data/LVTSRawBigQueryData",lvtsDataStartYr,"-",lvtsDataEndYr,".RData", sep = "", collapse = NULL));



LYNXRawBigQueryDataExtractSavePath <- 
  c(paste("E:/Projects/FundingDataTables/Raw Transactions Data/LYNXRawBigQueryData",startyear,"-",endyear,".RData", sep = "", collapse = NULL));


mergedLVTSTXNsYearSpecificTXTAndBigQueryExtractSavePath <- 
  c(paste("E:/Projects/FundingDataTables/Raw Transactions Data/YearsSpecificLVTSTXNsYM",lvtsDataStartYr,"-",endyear,"DataSeries.RData", sep = "", collapse = NULL));



gc();
print(now());#used to record speed of computations
##Load all saved historical LVTS Data
load(LVTSRawBigQueryDataExtractLoadPath);
gc();

##Remove unused fields from loaded historical LVTS data
LVTSRawBigQueryData[ ,c("msg_ref_id", "cycle_seq_num", "trans_time", "part_brch_cd_from", "part_loc_cd_from", "part_brch_cd_to", "part_loc_cd_to",
                        "trans_class", "rjt_rsn_cd","mir_date" ,"mir_sess_nb","mir_seq_nb", "acp_time", "status") := NULL];



gc();
print("data tables loaded");#used to record speed of computations
print(now());#used to record speed of computations
gc();



##Define BigQuery connection parameters
bQConnection <- dbConnect(
  bigrquery::bigquery(),
  project = "prod-data-storage-prj", #Database server conection name
  dataset = "lvts_boc", #database schema name (data source to query data from)
  billing = "acs-research-prj" #Database connection user/owner name (billing refers to who the onnection is billed to)
)

##Open the connect to BigQuery
bQConnection;
1 ##option selection
##Test that the connection has been successful by listing the aviable tables
dbListTables(bQConnection);
1 ##Option selection

##Query to pull Lynx Data
LYNXRawBigQueryDataExtSQL <- c(paste("SELECT *
  FROM `prod-data-storage-prj.lynx_research.payment` ", "  
  WHERE EXTRACT(year FROM cycle_date) > ", (startyear-1), "
 AND EXTRACT(year FROM cycle_date) < ", (endyear+1),"
 AND DATE_TRUNC(CURRENT_DATE(), MONTH)>cycle_date 
 ORDER BY cycle_date ASC;", sep = "", collapse = NULL));


##Clear memory and pull Lynx data from BigQuery
gc();
gc();
LYNXRawBigQueryData <- as.data.table(dbGetQuery(bQConnection, LYNXRawBigQueryDataExtSQL));
gc();

##Adjust time stamp from UTC to EST, create a time field, ensure that only transactions from Lynx go live date are kept and then save the data locally
LYNXRawBigQueryData$last_modified <- with_tz(LYNXRawBigQueryData$last_modified, "America/New_York"); ##convert UTC to EST
LYNXRawBigQueryData$time <- strftime(LYNXRawBigQueryData$last_modified, format="%H:%M:%S"); ##create a time field
LYNXRawBigQueryData <- LYNXRawBigQueryData[cycle_date >= lynxGoLiveDate, ];##Ensuring all Lynx data is collected from the August 28, 2021 go live date

##add fields to pull the first 6 characters of the participant BIC codes to align with LVTS
LYNXRawBigQueryData$part_id_from <- substr(LYNXRawBigQueryData$payer_participant_bic, 1, 6);
LYNXRawBigQueryData$part_id_to <- substr(LYNXRawBigQueryData$receiver_participant_bic, 1, 6);

##add fields to align the Lynx naming of the SWIFT messages to LVTS
LYNXRawBigQueryData$swift_msg_id <-sub("(.{0})(.*)", "\\1MT\\2", LYNXRawBigQueryData$swift_msg_type);

##Save the Lynx data pull with modifications to a local file for later use
save(LYNXRawBigQueryData, file = LYNXRawBigQueryDataExtractSavePath);##Save Lynx data from BQ as a backup before removing unrequited fields

##Remove unused Lynx data fields
LYNXRawBigQueryData[ ,c("payment_id", "system_reference", "payer_institution_number", "payer_institution_irn", "payer_institution_name", 
                        "payer_institution_short_name", "payer_participant_number", "payer_participant_name", "payer_participant_short_name", 
                        "payer_reference", "receiver_institution_number", "receiver_institution_irn", "receiver_institution_name", 
                        "receiver_institution_short_name", "receiver_participant_number", "receiver_participant_name", 
                        "receiver_participant_short_name", "transaction_type", "pcrn" , "gridlock_settled_id", "priority", "payer_participant_bic",
                        "receiver_participant_bic", "swift_msg_type", "status") := NULL];


##Making artificial mapping of payment_tranche in LVTS to settlement_mechanism in Lynx
##The assumption here is that Tranche 2 in LVTS is equivalent to the LSM in Lynx
##and
##Tranche 1 in LVTS is equivalent to the UPM in Lynx
##Lynx mapping to LVTS
LYNXRawBigQueryData[settlement_mechanism == "UPM", payment_tranche := 1,];
LYNXRawBigQueryData[settlement_mechanism == "RTM", payment_tranche := 1,];
LYNXRawBigQueryData[settlement_mechanism == "LSM", payment_tranche := 2,];
##LVTS mapping to Lynx
LVTSRawBigQueryData[payment_tranche ==  1, settlement_mechanism := "UPM",];
LVTSRawBigQueryData[payment_tranche ==  2, settlement_mechanism := "LSM",];

##Add System Identifier To keep track of which system the transactions data came from
LYNXRawBigQueryData$systemID <- "LYNX";
LVTSRawBigQueryData$systemID <- "LVTS";

##Set column names for LVTS and Lynx data sets
setnames(LVTSRawBigQueryData, c("cycle_date" , "payment_tranche", "payment_amt", "part_id_from", "part_id_to", "swift_msg_id", "settlement_mechanism"), 
         c("date", "tranche", "amount", "sender", "receiver", "stream", "settlementMechanism"));

setnames(LYNXRawBigQueryData, c("cycle_date" , "payment_tranche", "last_modified", "part_id_from", "part_id_to", "swift_msg_id", "settlement_mechanism"), 
         c("date", "tranche", "date.time", "sender", "receiver", "stream", "settlementMechanism"));

##Merge LVTS and LYNX data
mergedLVTSTXNsYearsSpecYM <- rbind(LVTSRawBigQueryData, LYNXRawBigQueryData);

mergedLVTSTXNsYearsSpecYM$Year_Month <- as.yearmon(mergedLVTSTXNsYearsSpecYM$date.time);
mergedLVTSTXNsYearsSpecYM$Year_Month <- format(mergedLVTSTXNsYearsSpecYM$Year_Month, "%YM%m");#set the format for reporting of the quaters field
mergedLVTSTXNsYearsSpecYM$Year_Quarter <- as.yearqtr(mergedLVTSTXNsYearsSpecYM$date.time);#add a field to set the date-time in form of quarters
mergedLVTSTXNsYearsSpecYM$Year_Quarter <- format(mergedLVTSTXNsYearsSpecYM$Year_Quarter, "%YQ%q");#set the format for reporting of the quaters field
mergedLVTSTXNsYearsSpecYM$Year <- year(mergedLVTSTXNsYearsSpecYM$date.time)#add a field to set the date-time in form of years
##clean.data <- select(clean.data,-date,-time);#remove time and date fields from final data extract
mergedLVTSTXNsYearsSpecYM$tranche <- as.factor(mergedLVTSTXNsYearsSpecYM$tranche);
mergedLVTSTXNsYearsSpecYM$sender <- as.factor(mergedLVTSTXNsYearsSpecYM$sender);
mergedLVTSTXNsYearsSpecYM$receiver <- as.factor(mergedLVTSTXNsYearsSpecYM$receiver);
mergedLVTSTXNsYearsSpecYM$stream <- as.factor(mergedLVTSTXNsYearsSpecYM$stream);

## compute periodic aggregates by sender and receiver

mergedLVTSTXNsYearsSpecYM[,`:=`( MonthlyCount = .N), by = c("sender","receiver","Year_Month")];
gc(); #free up memory
mergedLVTSTXNsYearsSpecYM[,`:=`( QuarterlyCount = .N), by = c("sender","receiver","Year_Quarter")];
gc(); #free up memory
mergedLVTSTXNsYearsSpecYM[,`:=`( AnnualCount = .N), by = c("sender","receiver","Year")];
gc(); #free up memory
mergedLVTSTXNsYearsSpecYM[,`:=`( DailyCount = .N), by = c("sender","receiver","tranche","date")];
gc(); #free up memory
mergedLVTSTXNsYearsSpecYM[,`:=`( MonthlyCountRec = .N), by = c("receiver","sender","Year_Month")];
gc(); #free up memory
mergedLVTSTXNsYearsSpecYM[,`:=`( QuarterlyCountRec = .N), by = c("receiver","sender","Year_Quarter")];
gc(); #free up memory
mergedLVTSTXNsYearsSpecYM[,`:=`( AnnualCountRec = .N), by = c("receiver","sender","Year")];
gc(); #free up memory
mergedLVTSTXNsYearsSpecYM[,`:=`( DailyCountRec = .N), by = c("receiver","sender","tranche","date")];
gc(); #free up memory



save(mergedLVTSTXNsYearsSpecYM, file = mergedLVTSTXNsYearSpecificTXTAndBigQueryExtractSavePath);

