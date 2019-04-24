rm(list=ls())

library(lubridate)
library(sqldf)
library(DMwR)

hpd200411<- read.table("200411hpd.txt", sep=",",header = T, dec = ".") # Read 01 File

# Changing Format as applicable

hpd200411$YearMonthDay<-ymd(hpd200411$YearMonthDay)
hpd200411$YearMonthDay<-as.factor(hpd200411$YearMonthDay)
hpd200411$WeatherStationID<-as.factor(hpd200411$WeatherStationID)

# Deriving time slots in weather data

hpd200411$TimeSlot<-ifelse(hpd200411$Time<200,'Midnight to 2AM',ifelse(hpd200411$Time<400,'2AM to 4AM',
                                                                       ifelse(hpd200411$Time<600,'4AM to 6AM',
                                                                              ifelse(hpd200411$Time<800,'6AM to 8AM',
                                                                                     ifelse(hpd200411$Time<1000,'8AM to 10AM',
                                                                                            ifelse(hpd200411$Time<1200,'10AM to Noon',
                                                                                                   ifelse(hpd200411$Time<1400,'Noon to 2PM',
                                                                                                          ifelse(hpd200411$Time<1600,'2PM to 4PM',
                                                                                                                 ifelse(hpd200411$Time<1800,'4PM to 6PM',
                                                                                                                        ifelse(hpd200411$Time<2000,'6PM to 8PM',
                                                                                                                               ifelse(hpd200411$Time<2200,'8PM to 10PM','10PM to Midnight')))))))))))

hpd200411$Time<-NULL # Dropping time column

# Aggregating Hourly Precipitation by Station, Date & Slot

hpd200411<-sqldf('select distinct a.WeatherStationID, a.YearMonthDay, a.TimeSlot, avg(a.HourlyPrecip) as AvgPrecip
                 from hpd200411 a group by a.WeatherStationID, a.YearMonthDay, a.TimeSlot')

# Merging with close station data

closestation <- readRDS("closestation.rds")

hpd0411<-merge(hpd200411,closestation,by.x="WeatherStationID",by.y="WeatherStationID")

# Creating Keys for future merging

hpd0411$Key0<-paste(hpd0411$WeatherStationID,hpd0411$YearMonthDay,hpd0411$TimeSlot)
hpd0411$Key1<-paste(hpd0411$ClosestWS,hpd0411$YearMonthDay,hpd0411$TimeSlot)
hpd0411$Key2<-paste(hpd0411$Closest2ndWS,hpd0411$YearMonthDay,hpd0411$TimeSlot)
hpd0411$Key3<-paste(hpd0411$Closest3rdWS,hpd0411$YearMonthDay,hpd0411$TimeSlot)
hpd0411$Key4<-paste(hpd0411$Closest4thWS,hpd0411$YearMonthDay,hpd0411$TimeSlot)
hpd0411$Key5<-paste(hpd0411$Closest5thWS,hpd0411$YearMonthDay,hpd0411$TimeSlot)

# Merging with Closest Weather Stations

rm(hpd200411) # Free up memory
temp<-hpd0411
names(hpd0411)[names(hpd0411) == "AvgPrecip"] = "OrigPrecip"
hpd0411<-sqldf('select a.*, b.AvgPrecip from hpd0411 a left join (select Key0, AvgPrecip from temp) b
               on a.Key1=b.Key0')
names(hpd0411)[names(hpd0411) == "AvgPrecip"] = "ClosestPrecip"
gc()
hpd0411<-sqldf('select a.*, b.AvgPrecip from hpd0411 a left join (select Key0, AvgPrecip from temp) b
               on a.Key2=b.Key0')
names(hpd0411)[names(hpd0411) == "AvgPrecip"] = "Closest2ndPrecip"
gc()
hpd0411<-sqldf('select a.*, b.AvgPrecip from hpd0411 a left join (select Key0, AvgPrecip from temp) b
               on a.Key3=b.Key0')
names(hpd0411)[names(hpd0411) == "AvgPrecip"] = "Closest3rdPrecip"
gc()
hpd0411<-sqldf('select a.*, b.AvgPrecip from hpd0411 a left join (select Key0, AvgPrecip from temp) b
               on a.Key4=b.Key0')
names(hpd0411)[names(hpd0411) == "AvgPrecip"] = "Closest4thPrecip"
gc()
hpd0411<-sqldf('select a.*, b.AvgPrecip from hpd0411 a left join (select Key0, AvgPrecip from temp) b
               on a.Key5=b.Key0')
names(hpd0411)[names(hpd0411) == "AvgPrecip"] = "Closest5thPrecip"
gc()

# Check for null values & impute using closest neighbours

rm(temp) # Remove temp file
rm(closestation) # Remove unrequired file

colSums(is.na(hpd0411)) # NA values in OrigPrecip column

hpd0411$OrigPrecip<-ifelse(is.na(hpd0411$OrigPrecip),rowMeans(hpd0411[,c("ClosestPrecip", "Closest2ndPrecip",
                                                                         "Closest3rdPrecip","Closest4thPrecip",
                                                                         "Closest5thPrecip")], na.rm=TRUE),
                           hpd0411$OrigPrecip)

saveRDS(hpd0411, file = "hpd0411.rds") # Saving externally