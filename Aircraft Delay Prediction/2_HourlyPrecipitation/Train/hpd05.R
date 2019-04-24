rm(list=ls())

library(lubridate)
library(sqldf)
library(DMwR)

hpd200405<- read.table("200405hpd.txt", sep=",",header = T, dec = ".") # Read 01 File

# Changing Format as applicable

hpd200405$YearMonthDay<-ymd(hpd200405$YearMonthDay)
hpd200405$YearMonthDay<-as.factor(hpd200405$YearMonthDay)
hpd200405$WeatherStationID<-as.factor(hpd200405$WeatherStationID)

# Deriving time slots in weather data

hpd200405$TimeSlot<-ifelse(hpd200405$Time<200,'Midnight to 2AM',ifelse(hpd200405$Time<400,'2AM to 4AM',
                                                                       ifelse(hpd200405$Time<600,'4AM to 6AM',
                                                                              ifelse(hpd200405$Time<800,'6AM to 8AM',
                                                                                     ifelse(hpd200405$Time<1000,'8AM to 10AM',
                                                                                            ifelse(hpd200405$Time<1200,'10AM to Noon',
                                                                                                   ifelse(hpd200405$Time<1400,'Noon to 2PM',
                                                                                                          ifelse(hpd200405$Time<1600,'2PM to 4PM',
                                                                                                                 ifelse(hpd200405$Time<1800,'4PM to 6PM',
                                                                                                                        ifelse(hpd200405$Time<2000,'6PM to 8PM',
                                                                                                                               ifelse(hpd200405$Time<2200,'8PM to 10PM','10PM to Midnight')))))))))))

hpd200405$Time<-NULL # Dropping time column

# Aggregating Hourly Precipitation by Station, Date & Slot

hpd200405<-sqldf('select distinct a.WeatherStationID, a.YearMonthDay, a.TimeSlot, avg(a.HourlyPrecip) as AvgPrecip
                 from hpd200405 a group by a.WeatherStationID, a.YearMonthDay, a.TimeSlot')

# Merging with close station data

closestation <- readRDS("closestation.rds")

hpd0405<-merge(hpd200405,closestation,by.x="WeatherStationID",by.y="WeatherStationID")

# Creating Keys for future merging

hpd0405$Key0<-paste(hpd0405$WeatherStationID,hpd0405$YearMonthDay,hpd0405$TimeSlot)
hpd0405$Key1<-paste(hpd0405$ClosestWS,hpd0405$YearMonthDay,hpd0405$TimeSlot)
hpd0405$Key2<-paste(hpd0405$Closest2ndWS,hpd0405$YearMonthDay,hpd0405$TimeSlot)
hpd0405$Key3<-paste(hpd0405$Closest3rdWS,hpd0405$YearMonthDay,hpd0405$TimeSlot)
hpd0405$Key4<-paste(hpd0405$Closest4thWS,hpd0405$YearMonthDay,hpd0405$TimeSlot)
hpd0405$Key5<-paste(hpd0405$Closest5thWS,hpd0405$YearMonthDay,hpd0405$TimeSlot)

# Merging with Closest Weather Stations

rm(hpd200405) # Free up memory
temp<-hpd0405
names(hpd0405)[names(hpd0405) == "AvgPrecip"] = "OrigPrecip"
hpd0405<-sqldf('select a.*, b.AvgPrecip from hpd0405 a left join (select Key0, AvgPrecip from temp) b
               on a.Key1=b.Key0')
names(hpd0405)[names(hpd0405) == "AvgPrecip"] = "ClosestPrecip"
gc()
hpd0405<-sqldf('select a.*, b.AvgPrecip from hpd0405 a left join (select Key0, AvgPrecip from temp) b
               on a.Key2=b.Key0')
names(hpd0405)[names(hpd0405) == "AvgPrecip"] = "Closest2ndPrecip"
gc()
hpd0405<-sqldf('select a.*, b.AvgPrecip from hpd0405 a left join (select Key0, AvgPrecip from temp) b
           on a.Key3=b.Key0')
names(hpd0405)[names(hpd0405) == "AvgPrecip"] = "Closest3rdPrecip"
gc()
hpd0405<-sqldf('select a.*, b.AvgPrecip from hpd0405 a left join (select Key0, AvgPrecip from temp) b
           on a.Key4=b.Key0')
names(hpd0405)[names(hpd0405) == "AvgPrecip"] = "Closest4thPrecip"
gc()
hpd0405<-sqldf('select a.*, b.AvgPrecip from hpd0405 a left join (select Key0, AvgPrecip from temp) b
           on a.Key5=b.Key0')
names(hpd0405)[names(hpd0405) == "AvgPrecip"] = "Closest5thPrecip"
gc()

# Check for null values & impute using closest neighbours

rm(temp) # Remove temp file
rm(closestation) # Remove unrequired file

colSums(is.na(hpd0405)) # NA values in OrigPrecip column

hpd0405$OrigPrecip<-ifelse(is.na(hpd0405$OrigPrecip),rowMeans(hpd0405[,c("ClosestPrecip", "Closest2ndPrecip",
                                                                         "Closest3rdPrecip","Closest4thPrecip",
                                                                         "Closest5thPrecip")], na.rm=TRUE),
                           hpd0405$OrigPrecip)

saveRDS(hpd0405, file = "hpd0405.rds") # Saving externally