rm(list=ls())

library(lubridate)
library(sqldf)
library(DMwR)

hpd200409<- read.table("200409hpd.txt", sep=",",header = T, dec = ".") # Read 01 File

# Changing Format as applicable

hpd200409$YearMonthDay<-ymd(hpd200409$YearMonthDay)
hpd200409$YearMonthDay<-as.factor(hpd200409$YearMonthDay)
hpd200409$WeatherStationID<-as.factor(hpd200409$WeatherStationID)

# Deriving time slots in weather data

hpd200409$TimeSlot<-ifelse(hpd200409$Time<200,'Midnight to 2AM',ifelse(hpd200409$Time<400,'2AM to 4AM',
                                                                       ifelse(hpd200409$Time<600,'4AM to 6AM',
                                                                              ifelse(hpd200409$Time<800,'6AM to 8AM',
                                                                                     ifelse(hpd200409$Time<1000,'8AM to 10AM',
                                                                                            ifelse(hpd200409$Time<1200,'10AM to Noon',
                                                                                                   ifelse(hpd200409$Time<1400,'Noon to 2PM',
                                                                                                          ifelse(hpd200409$Time<1600,'2PM to 4PM',
                                                                                                                 ifelse(hpd200409$Time<1800,'4PM to 6PM',
                                                                                                                        ifelse(hpd200409$Time<2000,'6PM to 8PM',
                                                                                                                               ifelse(hpd200409$Time<2200,'8PM to 10PM','10PM to Midnight')))))))))))

hpd200409$Time<-NULL # Dropping time column

# Aggregating Hourly Precipitation by Station, Date & Slot

hpd200409<-sqldf('select distinct a.WeatherStationID, a.YearMonthDay, a.TimeSlot, avg(a.HourlyPrecip) as AvgPrecip
                 from hpd200409 a group by a.WeatherStationID, a.YearMonthDay, a.TimeSlot')

# Merging with close station data

closestation <- readRDS("closestation.rds")

hpd0409<-merge(hpd200409,closestation,by.x="WeatherStationID",by.y="WeatherStationID")

# Creating Keys for future merging

hpd0409$Key0<-paste(hpd0409$WeatherStationID,hpd0409$YearMonthDay,hpd0409$TimeSlot)
hpd0409$Key1<-paste(hpd0409$ClosestWS,hpd0409$YearMonthDay,hpd0409$TimeSlot)
hpd0409$Key2<-paste(hpd0409$Closest2ndWS,hpd0409$YearMonthDay,hpd0409$TimeSlot)
hpd0409$Key3<-paste(hpd0409$Closest3rdWS,hpd0409$YearMonthDay,hpd0409$TimeSlot)
hpd0409$Key4<-paste(hpd0409$Closest4thWS,hpd0409$YearMonthDay,hpd0409$TimeSlot)
hpd0409$Key5<-paste(hpd0409$Closest5thWS,hpd0409$YearMonthDay,hpd0409$TimeSlot)

# Merging with Closest Weather Stations

rm(hpd200409) # Free up memory
temp<-hpd0409
names(hpd0409)[names(hpd0409) == "AvgPrecip"] = "OrigPrecip"
hpd0409<-sqldf('select a.*, b.AvgPrecip from hpd0409 a left join (select Key0, AvgPrecip from temp) b
               on a.Key1=b.Key0')
names(hpd0409)[names(hpd0409) == "AvgPrecip"] = "ClosestPrecip"
gc()
hpd0409<-sqldf('select a.*, b.AvgPrecip from hpd0409 a left join (select Key0, AvgPrecip from temp) b
               on a.Key2=b.Key0')
names(hpd0409)[names(hpd0409) == "AvgPrecip"] = "Closest2ndPrecip"
gc()
hpd0409<-sqldf('select a.*, b.AvgPrecip from hpd0409 a left join (select Key0, AvgPrecip from temp) b
               on a.Key3=b.Key0')
names(hpd0409)[names(hpd0409) == "AvgPrecip"] = "Closest3rdPrecip"
gc()
hpd0409<-sqldf('select a.*, b.AvgPrecip from hpd0409 a left join (select Key0, AvgPrecip from temp) b
               on a.Key4=b.Key0')
names(hpd0409)[names(hpd0409) == "AvgPrecip"] = "Closest4thPrecip"
gc()
hpd0409<-sqldf('select a.*, b.AvgPrecip from hpd0409 a left join (select Key0, AvgPrecip from temp) b
               on a.Key5=b.Key0')
names(hpd0409)[names(hpd0409) == "AvgPrecip"] = "Closest5thPrecip"
gc()

# Check for null values & impute using closest neighbours

rm(temp) # Remove temp file
rm(closestation) # Remove unrequired file

colSums(is.na(hpd0409)) # NA values in OrigPrecip column

hpd0409$OrigPrecip<-ifelse(is.na(hpd0409$OrigPrecip),rowMeans(hpd0409[,c("ClosestPrecip", "Closest2ndPrecip",
                                                                         "Closest3rdPrecip","Closest4thPrecip",
                                                                         "Closest5thPrecip")], na.rm=TRUE),
                           hpd0409$OrigPrecip)

saveRDS(hpd0409, file = "hpd0409.rds") # Saving externally