set.seed(seedNumber);
mydf <- data.frame(P_alex = sample(0:5, 101, replace = TRUE),P_hvh = sample(0:5, 101, replace = TRUE), 
                   P_trex = sample(0:5, 101, replace = TRUE),value = sample(0:100, 101, replace = TRUE),
                   value2 = sample(0:100, 101, replace = TRUE),date = as.POSIXct(startingTimeStamp) + 360 * 0:100)
mydt <- as.data.table(mydf);
#mydt <- mydt[order(date)];
#setkey(mydt,date);
#aggregate(cbind(mydf$value), by=list(P_alex, P_hvh), cut(mydf$date, breaks="10 min"),sum)

mydf2 <- aggregate(cbind(value, value2) ~ P_alex + P_hvh + cut(mydf$date,intervals), mydf[setdiff(names(mydf),"date")], max);

mydt2 <- as.data.table(mydf2);
setnames(mydt2, "cut(mydf$date, intervals)", "date");
mydf2 <- as.data.frame(mydt2);
mydf2$date2 <- ymd(paste(year(mydf2$date),month(mydf2$date),day(mydf2$date)));

tempMydt2 <- as.data.table(mydf2);

tempMaxMydt2 <- tempMydt2[, list(maxValue=max(tempMydt2$value)), by=c("P_alex", "P_hvh", "date2")];

tempSumMaxMydt2 <- tempMaxMydt2[, list(sumMaxValue=sum(maxValue)), by=c("P_alex", "date2")];


#mydf3 <- aggregate(cbind(value, value2) ~ P_alex + P_hvh + cut(mydf2$date2,intervals), mydf2[setdiff(names(mydf2),"date2")], max);


mydf2 %>%
  select(P_alex:value2) %>%
  filter(max(value))



mydt3  <-  mydt2[, list(sumMaxValue=sum((value))), by=c("P_alex","date")][which.max(value),];

mydt3  <-  mydt2[, list(sumMaxValue=sum(which.max(value))), by=c("P_alex","date")];

alexNames <- unique(mydt2$P_alex);
hvhNames <- unique(mydt2$P_hvh);




mydt3  <-  mydt2[, list(value=sum(mydt2$value[max(mydt2$value)])), by=c("P_alex","date")][];
#d[,c.sum:=sum(d$val[d$id1 %in% id1]),by=id2][]

#aggregate(.  ~ cut(mydf$date, "5 min"), by=list(mydf$P_alex, mydf$P_hvh) ,mydf[setdiff(names(mydf),"date")], sum)
#aggregate(. ~ cut(mydt$date, "5 min"),mydt[setdiff(names(mydt), "date")], sum)

#mydt[, sum(value),cut(mydf$date, breaks="10 min"), by=c("P_alex", "P_hvh")]


TestSavePath <-  c(paste("C:/Projects/FundingDataTables/STATADataTables/lvtsTransSentRecievedByFIXLSAnnualSeries",startYr,"-",endYr,".xls", sep = "", collapse = NULL));
print(TestSavePath)