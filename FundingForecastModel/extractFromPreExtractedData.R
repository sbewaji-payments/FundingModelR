ACSSUSBETXNsDataSeriesSentRecieved$Year_Quarter <- as.yearqtr(ACSSUSBETXNsDataSeriesSentRecieved$Year_Quarter)
class(ACSSUSBETXNsDataSeriesSentRecieved$Year_Quarter)
View(ACSSUSBETXNsDataSeriesSentRecieved)
ACSSUSBETXNsDataSeriesSentRecieved$Year_Quarter <- format(ACSSUSBETXNsDataSeriesSentRecieved$Year_Quarter, "%YQ%q");
ACSSUSBETXNsDataSeriesSentRecieved2015 <- ACSSUSBETXNsDataSeriesSentRecieved[Year_Quarter<"2016Q1",]
View(ACSSUSBETXNsDataSeriesSentRecieved2015)
ACSSUSBETXNsDataSeriesSentRecieved <- ACSSUSBETXNsDataSeriesSentRecieved2015
extractedACSSUSBEDataLoadPath <- c(paste("C:/Projects/FundingDataTables/Cleaned Transactions Data/acssusbeTXNsSentRecievedByFIQuarterlySeries",
                                           +                                          2000,"-",2015,".RData", sep = "", collapse = NULL));
extractedACSSUSBEDataLoadPath
"C:/Projects/FundingDataTables/Cleaned Transactions Data/acssusbeTXNsSentRecievedByFIQuarterlySeries2000-2015.RData"
save(ACSSUSBETXNsDataSeriesSentRecieved, file = extractedACSSUSBEDataLoadPath)




LVTSTXNsDataSeriesSentRecieved$Year_Quarter <- as.yearqtr(LVTSTXNsDataSeriesSentRecieved$Year_Quarter)
# class(LVTSTXNsDataSeriesSentRecieved$Year_Quarter)
# View(LVTSTXNsDataSeriesSentRecieved)
LVTSTXNsDataSeriesSentRecieved$Year_Quarter <- format(LVTSTXNsDataSeriesSentRecieved$Year_Quarter, "%YQ%q");
LVTSTXNsDataSeriesSentRecieved2015 <- LVTSTXNsDataSeriesSentRecieved[Year_Quarter<"2016Q1",]
# View(LVTSTXNsDataSeriesSentRecieved2015)
LVTSTXNsDataSeriesSentRecieved <- LVTSTXNsDataSeriesSentRecieved2015
extractedLVTSDataLoadPath <- c(paste("C:/Projects/FundingDataTables/Cleaned Transactions Data/lvtsTXNsSentRecievedByFIQuarterlySeries",
                                        2000,"-",2015,".RData", sep = "", collapse = NULL));
extractedLVTSDataLoadPath
save(LVTSTXNsDataSeriesSentRecieved, file = extractedLVTSDataLoadPath)
