rm(list=ls())

library(lubridate)
library(sqldf)
library(DMwR)

hpd200401<- read.table("200401hpd.txt", sep=",",header = T, dec = ".") # Read 01 File

# Changing Format as applicable

hpd200401$YearMonthDay<-ymd(hpd200401$YearMonthDay)
hpd200401$YearMonthDay<-as.factor(hpd200401$YearMonthDay)
hpd200401$WeatherStationID<-as.factor(hpd200401$WeatherStationID)

# Deriving time slots in weather data

hpd200401$TimeSlot<-ifelse(hpd200401$Time<200,'Midnight to 2AM',ifelse(hpd200401$Time<400,'2AM to 4AM',
                                                               ifelse(hpd200401$Time<600,'4AM to 6AM',
                                                                      ifelse(hpd200401$Time<800,'6AM to 8AM',
                                                                             ifelse(hpd200401$Time<1000,'8AM to 10AM',
                                                                                    ifelse(hpd200401$Time<1200,'10AM to Noon',
                                                                                           ifelse(hpd200401$Time<1400,'Noon to 2PM',
                                                                                                  ifelse(hpd200401$Time<1600,'2PM to 4PM',
                                                                                                         ifelse(hpd200401$Time<1800,'4PM to 6PM',
                                                                                                                ifelse(hpd200401$Time<2000,'6PM to 8PM',
                                                                                                                       ifelse(hpd200401$Time<2200,'8PM to 10PM','10PM to Midnight')))))))))))

hpd200401$Time<-NULL # Dropping time column

# Aggregating Hourly Precipitation by Station, Date & Slot

hpd200401<-sqldf('select distinct a.WeatherStationID, a.YearMonthDay, a.TimeSlot, avg(a.HourlyPrecip) as AvgPrecip
                 from hpd200401 a group by a.WeatherStationID, a.YearMonthDay, a.TimeSlot')

# Merging with close station data

closestation <- readRDS("closestation.rds")

hpd0401<-merge(hpd200401,closestation,by.x="WeatherStationID",by.y="WeatherStationID")

# Creating Keys for future merging

hpd0401$Key0<-paste(hpd0401$WeatherStationID,hpd0401$YearMonthDay,hpd0401$TimeSlot)
hpd0401$Key1<-paste(hpd0401$ClosestWS,hpd0401$YearMonthDay,hpd0401$TimeSlot)
hpd0401$Key2<-paste(hpd0401$Closest2ndWS,hpd0401$YearMonthDay,hpd0401$TimeSlot)
hpd0401$Key3<-paste(hpd0401$Closest3rdWS,hpd0401$YearMonthDay,hpd0401$TimeSlot)
hpd0401$Key4<-paste(hpd0401$Closest4thWS,hpd0401$YearMonthDay,hpd0401$TimeSlot)
hpd0401$Key5<-paste(hpd0401$Closest5thWS,hpd0401$YearMonthDay,hpd0401$TimeSlot)

# Merging with Closest Weather Stations

rm(hpd200401) # Free up memory
temp<-hpd0401
names(hpd0401)[names(hpd0401) == "AvgPrecip"] = "OrigPrecip"
hpd0401<-sqldf('select a.*, b.AvgPrecip from hpd0401 a left join (select Key0, AvgPrecip from temp) b
           on a.Key1=b.Key0')
names(hpd0401)[names(hpd0401) == "AvgPrecip"] = "ClosestPrecip"
gc()
hpd0401<-sqldf('select a.*, b.AvgPrecip from hpd0401 a left join (select Key0, AvgPrecip from temp) b
           on a.Key2=b.Key0')
names(hpd0401)[names(hpd0401) == "AvgPrecip"] = "Closest2ndPrecip"
gc()
hpd0401<-sqldf('select a.*, b.AvgPrecip from hpd0401 a left join (select Key0, AvgPrecip from temp) b
           on a.Key3=b.Key0')
names(hpd0401)[names(hpd0401) == "AvgPrecip"] = "Closest3rdPrecip"
gc()
hpd0401<-sqldf('select a.*, b.AvgPrecip from hpd0401 a left join (select Key0, AvgPrecip from temp) b
           on a.Key4=b.Key0')
names(hpd0401)[names(hpd0401) == "AvgPrecip"] = "Closest4thPrecip"
gc()
hpd0401<-sqldf('select a.*, b.AvgPrecip from hpd0401 a left join (select Key0, AvgPrecip from temp) b
           on a.Key5=b.Key0')
names(hpd0401)[names(hpd0401) == "AvgPrecip"] = "Closest5thPrecip"
gc()

# Check for null values & impute using closest neighbours

rm(temp) # Remove temp file
rm(closestation) # Remove unrequired file

colSums(is.na(hpd0401)) # NA values in OrigPrecip column

hpd0401$OrigPrecip<-ifelse(is.na(hpd0401$OrigPrecip),rowMeans(hpd0401[,c("ClosestPrecip", "Closest2ndPrecip",
                                                                "Closest3rdPrecip","Closest4thPrecip",
                                                                "Closest5thPrecip")], na.rm=TRUE),
                        hpd0401$OrigPrecip)

saveRDS(hpd0401, file = "hpd0401.rds") # Saving externally