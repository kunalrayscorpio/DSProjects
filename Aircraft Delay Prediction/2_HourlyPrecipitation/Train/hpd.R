rm(list=ls())

library(sqldf)

hpd01<-readRDS("hpd0401.rds")
hpd03<-readRDS("hpd0403.rds")
hpd05<-readRDS("hpd0405.rds")
hpd07<-readRDS("hpd0407.rds")
hpd09<-readRDS("hpd0409.rds")
hpd11<-readRDS("hpd0411.rds")

hpd<-rbind(hpd01,hpd03,hpd05,hpd07,hpd09,hpd11) # Bring all datasets together

rm(hpd01,hpd03,hpd05,hpd07,hpd09,hpd11) # Remove individual datasets

hpd<-sqldf('select distinct WeatherStationID, AirportID, YearMonthDay, TimeSlot, OrigPrecip
               from hpd')

saveRDS(hpd, file = "hpd.rds")