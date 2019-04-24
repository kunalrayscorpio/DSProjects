rm(list=ls())

library(lubridate)
library(sqldf)
library(DMwR)

hpd200503<- read.table("200503hpd.txt", sep=",",header = T, dec = ".") # Read 03 File

# Changing Format as applicable

hpd200503$YearMonthDay<-ymd(hpd200503$YearMonthDay)
hpd200503$YearMonthDay<-as.factor(hpd200503$YearMonthDay)
hpd200503$WeatherStationID<-as.factor(hpd200503$WeatherStationID)

# Deriving time slots in weather data

hpd200503$TimeSlot<-ifelse(hpd200503$Time<200,'Midnight to 2AM',ifelse(hpd200503$Time<400,'2AM to 4AM',
                                                                       ifelse(hpd200503$Time<600,'4AM to 6AM',
                                                                              ifelse(hpd200503$Time<800,'6AM to 8AM',
                                                                                     ifelse(hpd200503$Time<1000,'8AM to 10AM',
                                                                                            ifelse(hpd200503$Time<1200,'10AM to Noon',
                                                                                                   ifelse(hpd200503$Time<1400,'Noon to 2PM',
                                                                                                          ifelse(hpd200503$Time<1600,'2PM to 4PM',
                                                                                                                 ifelse(hpd200503$Time<1800,'4PM to 6PM',
                                                                                                                        ifelse(hpd200503$Time<2000,'6PM to 8PM',
                                                                                                                               ifelse(hpd200503$Time<2200,'8PM to 10PM','10PM to Midnight')))))))))))

hpd200503$Time<-NULL # Dropping time column

# Aggregating Hourly Precipitation by Station, Date & Slot

hpd200503<-sqldf('select distinct a.WeatherStationID, a.YearMonthDay, a.TimeSlot, avg(a.HourlyPrecip) as AvgPrecip
                 from hpd200503 a group by a.WeatherStationID, a.YearMonthDay, a.TimeSlot')

# Merging with close station data

closestation <- readRDS("closestation.rds")

hpd0503<-merge(hpd200503,closestation,by.x="WeatherStationID",by.y="WeatherStationID")

# Creating Keys for future merging

hpd0503$Key0<-paste(hpd0503$WeatherStationID,hpd0503$YearMonthDay,hpd0503$TimeSlot)
hpd0503$Key1<-paste(hpd0503$ClosestWS,hpd0503$YearMonthDay,hpd0503$TimeSlot)
hpd0503$Key2<-paste(hpd0503$Closest2ndWS,hpd0503$YearMonthDay,hpd0503$TimeSlot)
hpd0503$Key3<-paste(hpd0503$Closest3rdWS,hpd0503$YearMonthDay,hpd0503$TimeSlot)
hpd0503$Key4<-paste(hpd0503$Closest4thWS,hpd0503$YearMonthDay,hpd0503$TimeSlot)
hpd0503$Key5<-paste(hpd0503$Closest5thWS,hpd0503$YearMonthDay,hpd0503$TimeSlot)

# Merging with Closest Weather Stations

rm(hpd200503) # Free up memory
temp<-hpd0503
names(hpd0503)[names(hpd0503) == "AvgPrecip"] = "OrigPrecip"
hpd0503<-sqldf('select a.*, b.AvgPrecip from hpd0503 a left join (select Key0, AvgPrecip from temp) b
               on a.Key1=b.Key0')
names(hpd0503)[names(hpd0503) == "AvgPrecip"] = "ClosestPrecip"
gc()
hpd0503<-sqldf('select a.*, b.AvgPrecip from hpd0503 a left join (select Key0, AvgPrecip from temp) b
               on a.Key2=b.Key0')
names(hpd0503)[names(hpd0503) == "AvgPrecip"] = "Closest2ndPrecip"
gc()
hpd0503<-sqldf('select a.*, b.AvgPrecip from hpd0503 a left join (select Key0, AvgPrecip from temp) b
               on a.Key3=b.Key0')
names(hpd0503)[names(hpd0503) == "AvgPrecip"] = "Closest3rdPrecip"
gc()
hpd0503<-sqldf('select a.*, b.AvgPrecip from hpd0503 a left join (select Key0, AvgPrecip from temp) b
               on a.Key4=b.Key0')
names(hpd0503)[names(hpd0503) == "AvgPrecip"] = "Closest4thPrecip"
gc()
hpd0503<-sqldf('select a.*, b.AvgPrecip from hpd0503 a left join (select Key0, AvgPrecip from temp) b
               on a.Key5=b.Key0')
names(hpd0503)[names(hpd0503) == "AvgPrecip"] = "Closest5thPrecip"
gc()

# Check for null values & impute using closest neighbours

rm(temp) # Remove temp file
rm(closestation) # Remove unrequired file

colSums(is.na(hpd0503)) # NA values in OrigPrecip column

hpd0503$OrigPrecip<-ifelse(is.na(hpd0503$OrigPrecip),rowMeans(hpd0503[,c("ClosestPrecip", "Closest2ndPrecip",
                                                                         "Closest3rdPrecip","Closest4thPrecip",
                                                                         "Closest5thPrecip")], na.rm=TRUE),
                           hpd0503$OrigPrecip)

saveRDS(hpd0503, file = "hpd0503Test.rds") # Saving externally