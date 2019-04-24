rm(list=ls())

library(sqldf)

hly03<-readRDS("hly0503Test.rds")
hly07<-readRDS("hly0507Test.rds")
hly09<-readRDS("hly0509Test.rds")
hly11<-readRDS("hly0511Test.rds")

hly<-rbind(hly03,hly07,hly09,hly11) # Bring all datasets together

rm(hly03,hly07,hly09,hly11) # Remove individual datasets

str(hly)

hly<-sqldf('select distinct WeatherStationID, AirportID, YearMonthDay, TimeSlot, OrigSkyCond,
           OrigVis,OrigDBT,OrigDewPtTemp,OrigRelHumPerc,OrigWindSp,OrigWindDir,OrigWindGustVal,
           OrigStnPres from hly')

saveRDS(hly, file = "hlyTest.rds")