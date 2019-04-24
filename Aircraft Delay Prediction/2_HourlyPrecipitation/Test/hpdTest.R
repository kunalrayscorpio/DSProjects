rm(list=ls())

library(sqldf)

hpd03<-readRDS("hpd0503Test.rds")
hpd07<-readRDS("hpd0507Test.rds")
hpd09<-readRDS("hpd0509Test.rds")
hpd11<-readRDS("hpd0511Test.rds")

hpd<-rbind(hpd03,hpd07,hpd09,hpd11) # Bring all datasets together

rm(hpd03,hpd07,hpd09,hpd11) # Remove individual datasets

hpd<-sqldf('select distinct WeatherStationID, AirportID, YearMonthDay, TimeSlot, OrigPrecip
           from hpd')

saveRDS(hpd, file = "hpdTest.rds")