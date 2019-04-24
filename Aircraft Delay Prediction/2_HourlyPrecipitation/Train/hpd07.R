rm(list=ls())

library(lubridate)
library(sqldf)
library(DMwR)

hpd200407<- read.table("200407hpd.txt", sep=",",header = T, dec = ".") # Read 01 File

# Changing Format as applicable

hpd200407$YearMonthDay<-ymd(hpd200407$YearMonthDay)
hpd200407$YearMonthDay<-as.factor(hpd200407$YearMonthDay)
hpd200407$WeatherStationID<-as.factor(hpd200407$WeatherStationID)

# Deriving time slots in weather data

hpd200407$TimeSlot<-ifelse(hpd200407$Time<200,'Midnight to 2AM',ifelse(hpd200407$Time<400,'2AM to 4AM',
                                                                       ifelse(hpd200407$Time<600,'4AM to 6AM',
                                                                              ifelse(hpd200407$Time<800,'6AM to 8AM',
                                                                                     ifelse(hpd200407$Time<1000,'8AM to 10AM',
                                                                                            ifelse(hpd200407$Time<1200,'10AM to Noon',
                                                                                                   ifelse(hpd200407$Time<1400,'Noon to 2PM',
                                                                                                          ifelse(hpd200407$Time<1600,'2PM to 4PM',
                                                                                                                 ifelse(hpd200407$Time<1800,'4PM to 6PM',
                                                                                                                        ifelse(hpd200407$Time<2000,'6PM to 8PM',
                                                                                                                               ifelse(hpd200407$Time<2200,'8PM to 10PM','10PM to Midnight')))))))))))

hpd200407$Time<-NULL # Dropping time column

# Aggregating Hourly Precipitation by Station, Date & Slot

hpd200407<-sqldf('select distinct a.WeatherStationID, a.YearMonthDay, a.TimeSlot, avg(a.HourlyPrecip) as AvgPrecip
                 from hpd200407 a group by a.WeatherStationID, a.YearMonthDay, a.TimeSlot')

# Merging with close station data

closestation <- readRDS("closestation.rds")

hpd0407<-merge(hpd200407,closestation,by.x="WeatherStationID",by.y="WeatherStationID")

# Creating Keys for future merging

hpd0407$Key0<-paste(hpd0407$WeatherStationID,hpd0407$YearMonthDay,hpd0407$TimeSlot)
hpd0407$Key1<-paste(hpd0407$ClosestWS,hpd0407$YearMonthDay,hpd0407$TimeSlot)
hpd0407$Key2<-paste(hpd0407$Closest2ndWS,hpd0407$YearMonthDay,hpd0407$TimeSlot)
hpd0407$Key3<-paste(hpd0407$Closest3rdWS,hpd0407$YearMonthDay,hpd0407$TimeSlot)
hpd0407$Key4<-paste(hpd0407$Closest4thWS,hpd0407$YearMonthDay,hpd0407$TimeSlot)
hpd0407$Key5<-paste(hpd0407$Closest5thWS,hpd0407$YearMonthDay,hpd0407$TimeSlot)

# Merging with Closest Weather Stations

rm(hpd200407) # Free up memory
temp<-hpd0407
names(hpd0407)[names(hpd0407) == "AvgPrecip"] = "OrigPrecip"
hpd0407<-sqldf('select a.*, b.AvgPrecip from hpd0407 a left join (select Key0, AvgPrecip from temp) b
               on a.Key1=b.Key0')
names(hpd0407)[names(hpd0407) == "AvgPrecip"] = "ClosestPrecip"
gc()
hpd0407<-sqldf('select a.*, b.AvgPrecip from hpd0407 a left join (select Key0, AvgPrecip from temp) b
               on a.Key2=b.Key0')
names(hpd0407)[names(hpd0407) == "AvgPrecip"] = "Closest2ndPrecip"
gc()
hpd0407<-sqldf('select a.*, b.AvgPrecip from hpd0407 a left join (select Key0, AvgPrecip from temp) b
               on a.Key3=b.Key0')
names(hpd0407)[names(hpd0407) == "AvgPrecip"] = "Closest3rdPrecip"
gc()
hpd0407<-sqldf('select a.*, b.AvgPrecip from hpd0407 a left join (select Key0, AvgPrecip from temp) b
               on a.Key4=b.Key0')
names(hpd0407)[names(hpd0407) == "AvgPrecip"] = "Closest4thPrecip"
gc()
hpd0407<-sqldf('select a.*, b.AvgPrecip from hpd0407 a left join (select Key0, AvgPrecip from temp) b
               on a.Key5=b.Key0')
names(hpd0407)[names(hpd0407) == "AvgPrecip"] = "Closest5thPrecip"
gc()

# Check for null values & impute using closest neighbours

rm(temp) # Remove temp file
rm(closestation) # Remove unrequired file

colSums(is.na(hpd0407)) # NA values in OrigPrecip column

hpd0407$OrigPrecip<-ifelse(is.na(hpd0407$OrigPrecip),rowMeans(hpd0407[,c("ClosestPrecip", "Closest2ndPrecip",
                                                                         "Closest3rdPrecip","Closest4thPrecip",
                                                                         "Closest5thPrecip")], na.rm=TRUE),
                           hpd0407$OrigPrecip)

saveRDS(hpd0407, file = "hpd0407.rds") # Saving externally