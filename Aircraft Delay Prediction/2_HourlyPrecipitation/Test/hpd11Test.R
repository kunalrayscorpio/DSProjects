rm(list=ls())

library(lubridate)
library(sqldf)
library(DMwR)

hpd200511<- read.table("200511hpd.txt", sep=",",header = T, dec = ".") # Read 11 File

# Changing Format as applicable

hpd200511$YearMonthDay<-ymd(hpd200511$YearMonthDay)
hpd200511$YearMonthDay<-as.factor(hpd200511$YearMonthDay)
hpd200511$WeatherStationID<-as.factor(hpd200511$WeatherStationID)

# Deriving time slots in weather data

hpd200511$TimeSlot<-ifelse(hpd200511$Time<200,'Midnight to 2AM',ifelse(hpd200511$Time<400,'2AM to 4AM',
                                                                       ifelse(hpd200511$Time<600,'4AM to 6AM',
                                                                              ifelse(hpd200511$Time<800,'6AM to 8AM',
                                                                                     ifelse(hpd200511$Time<1000,'8AM to 10AM',
                                                                                            ifelse(hpd200511$Time<1200,'10AM to Noon',
                                                                                                   ifelse(hpd200511$Time<1400,'Noon to 2PM',
                                                                                                          ifelse(hpd200511$Time<1600,'2PM to 4PM',
                                                                                                                 ifelse(hpd200511$Time<1800,'4PM to 6PM',
                                                                                                                        ifelse(hpd200511$Time<2000,'6PM to 8PM',
                                                                                                                               ifelse(hpd200511$Time<2200,'8PM to 10PM','10PM to Midnight')))))))))))

hpd200511$Time<-NULL # Dropping time column

# Aggregating Hourly Precipitation by Station, Date & Slot

hpd200511<-sqldf('select distinct a.WeatherStationID, a.YearMonthDay, a.TimeSlot, avg(a.HourlyPrecip) as AvgPrecip
                 from hpd200511 a group by a.WeatherStationID, a.YearMonthDay, a.TimeSlot')

# Merging with close station data

closestation <- readRDS("closestation.rds")

hpd0511<-merge(hpd200511,closestation,by.x="WeatherStationID",by.y="WeatherStationID")

# Creating Keys for future merging

hpd0511$Key0<-paste(hpd0511$WeatherStationID,hpd0511$YearMonthDay,hpd0511$TimeSlot)
hpd0511$Key1<-paste(hpd0511$ClosestWS,hpd0511$YearMonthDay,hpd0511$TimeSlot)
hpd0511$Key2<-paste(hpd0511$Closest2ndWS,hpd0511$YearMonthDay,hpd0511$TimeSlot)
hpd0511$Key3<-paste(hpd0511$Closest3rdWS,hpd0511$YearMonthDay,hpd0511$TimeSlot)
hpd0511$Key4<-paste(hpd0511$Closest4thWS,hpd0511$YearMonthDay,hpd0511$TimeSlot)
hpd0511$Key5<-paste(hpd0511$Closest5thWS,hpd0511$YearMonthDay,hpd0511$TimeSlot)

# Merging with Closest Weather Stations

rm(hpd200511) # Free up memory
temp<-hpd0511
names(hpd0511)[names(hpd0511) == "AvgPrecip"] = "OrigPrecip"
hpd0511<-sqldf('select a.*, b.AvgPrecip from hpd0511 a left join (select Key0, AvgPrecip from temp) b
               on a.Key1=b.Key0')
names(hpd0511)[names(hpd0511) == "AvgPrecip"] = "ClosestPrecip"
gc()
hpd0511<-sqldf('select a.*, b.AvgPrecip from hpd0511 a left join (select Key0, AvgPrecip from temp) b
               on a.Key2=b.Key0')
names(hpd0511)[names(hpd0511) == "AvgPrecip"] = "Closest2ndPrecip"
gc()
hpd0511<-sqldf('select a.*, b.AvgPrecip from hpd0511 a left join (select Key0, AvgPrecip from temp) b
               on a.Key3=b.Key0')
names(hpd0511)[names(hpd0511) == "AvgPrecip"] = "Closest3rdPrecip"
gc()
hpd0511<-sqldf('select a.*, b.AvgPrecip from hpd0511 a left join (select Key0, AvgPrecip from temp) b
               on a.Key4=b.Key0')
names(hpd0511)[names(hpd0511) == "AvgPrecip"] = "Closest4thPrecip"
gc()
hpd0511<-sqldf('select a.*, b.AvgPrecip from hpd0511 a left join (select Key0, AvgPrecip from temp) b
               on a.Key5=b.Key0')
names(hpd0511)[names(hpd0511) == "AvgPrecip"] = "Closest5thPrecip"
gc()

# Check for null values & impute using closest neighbours

rm(temp) # Remove temp file
rm(closestation) # Remove unrequired file

colSums(is.na(hpd0511)) # NA values in OrigPrecip column

hpd0511$OrigPrecip<-ifelse(is.na(hpd0511$OrigPrecip),rowMeans(hpd0511[,c("ClosestPrecip", "Closest2ndPrecip",
                                                                         "Closest3rdPrecip","Closest4thPrecip",
                                                                         "Closest5thPrecip")], na.rm=TRUE),
                           hpd0511$OrigPrecip)

saveRDS(hpd0511, file = "hpd0511Test.rds") # Saving externally