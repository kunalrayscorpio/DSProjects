rm(list=ls())

library(sqldf)

hly01<-readRDS("hly0401.rds")
hly03<-readRDS("hly0403.rds")
hly05<-readRDS("hly0405.rds")
hly07<-readRDS("hly0407.rds")
hly09<-readRDS("hly0409.rds")
hly11<-readRDS("hly0411.rds")

hly<-rbind(hly01,hly03,hly05,hly07,hly09,hly11) # Bring all datasets together

rm(hly01,hly03,hly05,hly07,hly09,hly11) # Remove individual datasets

str(hly)

hly<-sqldf('select distinct WeatherStationID, AirportID, YearMonthDay, TimeSlot, OrigSkyCond,
           OrigVis,OrigDBT,OrigDewPtTemp,OrigRelHumPerc,OrigWindSp,OrigWindDir,OrigWindGustVal,
           OrigStnPres from hly')

saveRDS(hly, file = "hly.rds")