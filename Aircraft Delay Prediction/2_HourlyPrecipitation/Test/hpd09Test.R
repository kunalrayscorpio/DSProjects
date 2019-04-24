rm(list=ls())

library(lubridate)
library(sqldf)
library(DMwR)

hpd200509<- read.table("200509hpd.txt", sep=",",header = T, dec = ".") # Read 09 File

# Changing Format as applicable

hpd200509$YearMonthDay<-ymd(hpd200509$YearMonthDay)
hpd200509$YearMonthDay<-as.factor(hpd200509$YearMonthDay)
hpd200509$WeatherStationID<-as.factor(hpd200509$WeatherStationID)

# Deriving time slots in weather data

hpd200509$TimeSlot<-ifelse(hpd200509$Time<200,'Midnight to 2AM',ifelse(hpd200509$Time<400,'2AM to 4AM',
                                                                       ifelse(hpd200509$Time<600,'4AM to 6AM',
                                                                              ifelse(hpd200509$Time<800,'6AM to 8AM',
                                                                                     ifelse(hpd200509$Time<1000,'8AM to 10AM',
                                                                                            ifelse(hpd200509$Time<1200,'10AM to Noon',
                                                                                                   ifelse(hpd200509$Time<1400,'Noon to 2PM',
                                                                                                          ifelse(hpd200509$Time<1600,'2PM to 4PM',
                                                                                                                 ifelse(hpd200509$Time<1800,'4PM to 6PM',
                                                                                                                        ifelse(hpd200509$Time<2000,'6PM to 8PM',
                                                                                                                               ifelse(hpd200509$Time<2200,'8PM to 10PM','10PM to Midnight')))))))))))

hpd200509$Time<-NULL # Dropping time column

# Aggregating Hourly Precipitation by Station, Date & Slot

hpd200509<-sqldf('select distinct a.WeatherStationID, a.YearMonthDay, a.TimeSlot, avg(a.HourlyPrecip) as AvgPrecip
                 from hpd200509 a group by a.WeatherStationID, a.YearMonthDay, a.TimeSlot')

# Merging with close station data

closestation <- readRDS("closestation.rds")

hpd0509<-merge(hpd200509,closestation,by.x="WeatherStationID",by.y="WeatherStationID")

# Creating Keys for future merging

hpd0509$Key0<-paste(hpd0509$WeatherStationID,hpd0509$YearMonthDay,hpd0509$TimeSlot)
hpd0509$Key1<-paste(hpd0509$ClosestWS,hpd0509$YearMonthDay,hpd0509$TimeSlot)
hpd0509$Key2<-paste(hpd0509$Closest2ndWS,hpd0509$YearMonthDay,hpd0509$TimeSlot)
hpd0509$Key3<-paste(hpd0509$Closest3rdWS,hpd0509$YearMonthDay,hpd0509$TimeSlot)
hpd0509$Key4<-paste(hpd0509$Closest4thWS,hpd0509$YearMonthDay,hpd0509$TimeSlot)
hpd0509$Key5<-paste(hpd0509$Closest5thWS,hpd0509$YearMonthDay,hpd0509$TimeSlot)

# Merging with Closest Weather Stations

rm(hpd200509) # Free up memory
temp<-hpd0509
names(hpd0509)[names(hpd0509) == "AvgPrecip"] = "OrigPrecip"
hpd0509<-sqldf('select a.*, b.AvgPrecip from hpd0509 a left join (select Key0, AvgPrecip from temp) b
               on a.Key1=b.Key0')
names(hpd0509)[names(hpd0509) == "AvgPrecip"] = "ClosestPrecip"
gc()
hpd0509<-sqldf('select a.*, b.AvgPrecip from hpd0509 a left join (select Key0, AvgPrecip from temp) b
               on a.Key2=b.Key0')
names(hpd0509)[names(hpd0509) == "AvgPrecip"] = "Closest2ndPrecip"
gc()
hpd0509<-sqldf('select a.*, b.AvgPrecip from hpd0509 a left join (select Key0, AvgPrecip from temp) b
               on a.Key3=b.Key0')
names(hpd0509)[names(hpd0509) == "AvgPrecip"] = "Closest3rdPrecip"
gc()
hpd0509<-sqldf('select a.*, b.AvgPrecip from hpd0509 a left join (select Key0, AvgPrecip from temp) b
               on a.Key4=b.Key0')
names(hpd0509)[names(hpd0509) == "AvgPrecip"] = "Closest4thPrecip"
gc()
hpd0509<-sqldf('select a.*, b.AvgPrecip from hpd0509 a left join (select Key0, AvgPrecip from temp) b
               on a.Key5=b.Key0')
names(hpd0509)[names(hpd0509) == "AvgPrecip"] = "Closest5thPrecip"
gc()

# Check for null values & impute using closest neighbours

rm(temp) # Remove temp file
rm(closestation) # Remove unrequired file

colSums(is.na(hpd0509)) # NA values in OrigPrecip column

hpd0509$OrigPrecip<-ifelse(is.na(hpd0509$OrigPrecip),rowMeans(hpd0509[,c("ClosestPrecip", "Closest2ndPrecip",
                                                                         "Closest3rdPrecip","Closest4thPrecip",
                                                                         "Closest5thPrecip")], na.rm=TRUE),
                           hpd0509$OrigPrecip)

saveRDS(hpd0509, file = "hpd0509Test.rds") # Saving externally