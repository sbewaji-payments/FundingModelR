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

testingStatus = "'RD'";


##Old txt file data
extractedLVTSataLoadPath <- c(paste("E:/Projects/FundingDataTables/Raw Transactions Data/YearsSpecificLVTSTXNsYM", oldDataStartYr,"-",oldDataEndYr,"DataSeries.Rdata", sep = "", collapse = NULL));




LVTSRawBigQueryDataExtractSavePath <- 
  c(paste("E:/Projects/FundingDataTables/Raw Transactions Data/LVTSRawBigQueryData",startyear,"-",endyear,".RData", sep = "", collapse = NULL));


mergedLVTSTXNsYearSpecificBigQueryExtractSavePath <- 
  c(paste("E:/Projects/FundingDataTables/Raw Transactions Data/mergedLVTSTXNsYearSpecificBigQueryData",startyear,"-",endyear,".RData", sep = "", collapse = NULL));


mergedLVTSTXNsYearSpecificTXTAndBigQueryExtractSavePath <- 
  c(paste("E:/Projects/FundingDataTables/Raw Transactions Data/YearsSpecificLVTSTXNsYM",oldDataStartYr,"-",endyear,"DataSeries.RData", sep = "", collapse = NULL));


# 
# monthlyACSSCollateralPoolExtractCSVSavePath <- 
#   c(paste("E:/Projects/FundingDataTables/Raw Transactions Data/monthlyACSSCollateralPool_",startyear,"-",endyear,".csv", sep = "", collapse = NULL));




# load("G:/Development Workspaces/R-Workspace/SystemAndStreamMigration/ExtractedTimeSeriesData/
#      ACSS Collateral Pool Data/Stream Migration Collateral Data/2019 Model Core Data/acssCollPool20062018.RData");



gc();
print(now());#used to record speed of computations
load(extractedLVTSataLoadPath);
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

##List/write all the SQL queries to run in BigQuery using the connection 
# monthlyPayTransExtSQL <- "SELECT *
# FROM `prod-lvtsdata-prj.lvts_boc.transaction_t1_boc8`
# WHERE cycle_date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 40 DAY) AND DATE_SUB(CURRENT_DATE(), INTERVAL 9 DAY)
# ORDER BY cycle_date ASC;"


LVTSRawBigQueryDataExtSQL <- c(paste("SELECT *
  FROM `prod-data-storage-prj.lvts_boc.payment_transaction` WHERE status !=", testingStatus, "  AND EXTRACT(year FROM cycle_date) > ", (startyear-1), "
 AND EXTRACT(year FROM cycle_date) < ", (endyear+1),"
 AND DATE_TRUNC(CURRENT_DATE(), MONTH)>cycle_date 
 ORDER BY cycle_date ASC;", sep = "", collapse = NULL));

## extract only the fields needed and convert the UTC dates in SQL by using the command TIME_TRUNC(EXTRACT(TIME FROM trans_time AT TIME ZONE 'America/Toronto'), SECOND)
##would be better from a memory use standpoint if done in SQL on the BigQuery end rather than downloading the entire table into R

# 
# LVTSRawBigQueryDataExtSQL <- c(paste("SELECT 
#   cycle_date AS date
#   payment_tranche  AS tranche
#   TIME_TRUNC(EXTRACT(TIME FROM acp_time AT TIME ZONE 'America/Toronto'), SECOND) AS time
#   payment_amt AS amount 
#   part_id_from AS sender
#   part_id_to AS receiver 
#   swift_msg_id AS stream
#   FROM `prod-data-storage-prj.lvts_boc.payment_transaction` WHERE status !=", testingStatus, "  AND EXTRACT(year FROM cycle_date) > ", (startyear-1), "
#  AND EXTRACT(year FROM cycle_date) < ", (endyear+1), "
# ORDER BY cycle_date ASC;", sep = "", collapse = NULL));
# 



###Pull Previous Day's BoC extracts data from BigQuery into respective data.table objects t osave to file
LVTSRawBigQueryData <- as.data.table(dbGetQuery(bQConnection, LVTSRawBigQueryDataExtSQL));

LVTSRawBigQueryData$date.time <- with_tz(LVTSRawBigQueryData$acp_time, "America/New_York"); ##convert UTC to EST

LVTSRawBigQueryData$time <- strftime(LVTSRawBigQueryData$date.time, format="%H:%M:%S");

save(LVTSRawBigQueryData, file = LVTSRawBigQueryDataExtractSavePath);

LVTSRawBigQueryData[ ,c("msg_ref_id", "cycle_seq_num", "trans_time", "part_brch_cd_from", "part_loc_cd_from", "part_brch_cd_to", "part_loc_cd_to",
                        "trans_class", "rjt_rsn_cd","mir_date" ,"mir_sess_nb","mir_seq_nb", "acp_time", "status") := NULL];

LVTSRawBigQueryData <- LVTSRawBigQueryData %>% rename(date = cycle_date , tranche = payment_tranche, amount = payment_amt, 
                                                      sender = part_id_from, receiver = part_id_to, stream = swift_msg_id);


LVTSRawBigQueryData$Year_Month <- as.yearmon(LVTSRawBigQueryData$date.time);
LVTSRawBigQueryData$Year_Month <- format(LVTSRawBigQueryData$Year_Month, "%YM%m");#set the format for reporting of the quaters field
LVTSRawBigQueryData$Year_Quarter <- as.yearqtr(LVTSRawBigQueryData$date.time);#add a field to set the date-time in form of quarters
LVTSRawBigQueryData$Year_Quarter <- format(LVTSRawBigQueryData$Year_Quarter, "%YQ%q");#set the format for reporting of the quaters field
LVTSRawBigQueryData$Year <- year(LVTSRawBigQueryData$date.time)#add a field to set the date-time in form of years
##clean.data <- select(clean.data,-date,-time);#remove time and date fields from final data extract
LVTSRawBigQueryData$tranche <- as.factor(LVTSRawBigQueryData$tranche);
LVTSRawBigQueryData$sender <- as.factor(LVTSRawBigQueryData$sender);
LVTSRawBigQueryData$receiver <- as.factor(LVTSRawBigQueryData$receiver);
LVTSRawBigQueryData$stream <- as.factor(LVTSRawBigQueryData$stream);

## mergedLVTSTXNsYearsSpecYM <- LVTSRawBigQueryData

LVTSRawBigQueryData[,`:=`( MonthlyCount = .N), by = c("sender","receiver","Year_Month")];
gc(); #free up memory
LVTSRawBigQueryData[,`:=`( QuarterlyCount = .N), by = c("sender","receiver","Year_Quarter")];
gc(); #free up memory
LVTSRawBigQueryData[,`:=`( AnnualCount = .N), by = c("sender","receiver","Year")];
gc(); #free up memory
LVTSRawBigQueryData[,`:=`( DailyCount = .N), by = c("sender","receiver","tranche","date")];
gc(); #free up memory
LVTSRawBigQueryData[,`:=`( MonthlyCountRec = .N), by = c("receiver","sender","Year_Month")];
gc(); #free up memory
LVTSRawBigQueryData[,`:=`( QuarterlyCountRec = .N), by = c("receiver","sender","Year_Quarter")];
gc(); #free up memory
LVTSRawBigQueryData[,`:=`( AnnualCountRec = .N), by = c("receiver","sender","Year")];
gc(); #free up memory
LVTSRawBigQueryData[,`:=`( DailyCountRec = .N), by = c("receiver","sender","tranche","date")];
gc(); #free up memory


mergedLVTSTXNsYearsSpecYM$date <- as.Date(mergedLVTSTXNsYearsSpecYM$date.time);

mergedLVTSTXNsYearsSpecYM <- rbind(mergedLVTSTXNsYearsSpecYM, LVTSRawBigQueryData);

save(mergedLVTSTXNsYearsSpecYM, file = mergedLVTSTXNsYearSpecificTXTAndBigQueryExtractSavePath);

