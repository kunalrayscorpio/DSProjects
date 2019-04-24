rm(list=ls())

library(lubridate)
library(sqldf)
library(DMwR)

hpd200403<- read.table("200403hpd.txt", sep=",",header = T, dec = ".") # Read 01 File

# Changing Format as applicable

hpd200403$YearMonthDay<-ymd(hpd200403$YearMonthDay)
hpd200403$YearMonthDay<-as.factor(hpd200403$YearMonthDay)
hpd200403$WeatherStationID<-as.factor(hpd200403$WeatherStationID)

# Deriving time slots in weather data

hpd200403$TimeSlot<-ifelse(hpd200403$Time<200,'Midnight to 2AM',ifelse(hpd200403$Time<400,'2AM to 4AM',
                                                                       ifelse(hpd200403$Time<600,'4AM to 6AM',
                                                                              ifelse(hpd200403$Time<800,'6AM to 8AM',
                                                                                     ifelse(hpd200403$Time<1000,'8AM to 10AM',
                                                                                            ifelse(hpd200403$Time<1200,'10AM to Noon',
                                                                                                   ifelse(hpd200403$Time<1400,'Noon to 2PM',
                                                                                                          ifelse(hpd200403$Time<1600,'2PM to 4PM',
                                                                                                                 ifelse(hpd200403$Time<1800,'4PM to 6PM',
                                                                                                                        ifelse(hpd200403$Time<2000,'6PM to 8PM',
                                                                                                                               ifelse(hpd200403$Time<2200,'8PM to 10PM','10PM to Midnight')))))))))))

hpd200403$Time<-NULL # Dropping time column

# Aggregating Hourly Precipitation by Station, Date & Slot

hpd200403<-sqldf('select distinct a.WeatherStationID, a.YearMonthDay, a.TimeSlot, avg(a.HourlyPrecip) as AvgPrecip
                 from hpd200403 a group by a.WeatherStationID, a.YearMonthDay, a.TimeSlot')

# Merging with close station data

closestation <- readRDS("closestation.rds")

hpd0403<-merge(hpd200403,closestation,by.x="WeatherStationID",by.y="WeatherStationID")

# Creating Keys for future merging

hpd0403$Key0<-paste(hpd0403$WeatherStationID,hpd0403$YearMonthDay,hpd0403$TimeSlot)
hpd0403$Key1<-paste(hpd0403$ClosestWS,hpd0403$YearMonthDay,hpd0403$TimeSlot)
hpd0403$Key2<-paste(hpd0403$Closest2ndWS,hpd0403$YearMonthDay,hpd0403$TimeSlot)
hpd0403$Key3<-paste(hpd0403$Closest3rdWS,hpd0403$YearMonthDay,hpd0403$TimeSlot)
hpd0403$Key4<-paste(hpd0403$Closest4thWS,hpd0403$YearMonthDay,hpd0403$TimeSlot)
hpd0403$Key5<-paste(hpd0403$Closest5thWS,hpd0403$YearMonthDay,hpd0403$TimeSlot)

# Merging with Closest Weather Stations

rm(hpd200403) # Free up memory
temp<-hpd0403
names(hpd0403)[names(hpd0403) == "AvgPrecip"] = "OrigPrecip"
hpd0403<-sqldf('select a.*, b.AvgPrecip from hpd0403 a left join (select Key0, AvgPrecip from temp) b
               on a.Key1=b.Key0')
names(hpd0403)[names(hpd0403) == "AvgPrecip"] = "ClosestPrecip"
gc()
hpd0403<-sqldf('select a.*, b.AvgPrecip from hpd0403 a left join (select Key0, AvgPrecip from temp) b
               on a.Key2=b.Key0')
names(hpd0403)[names(hpd0403) == "AvgPrecip"] = "Closest2ndPrecip"
gc()
hpd0403<-sqldf('select a.*, b.AvgPrecip from hpd0403 a left join (select Key0, AvgPrecip from temp) b
           on a.Key3=b.Key0')
names(hpd0403)[names(hpd0403) == "AvgPrecip"] = "Closest3rdPrecip"
gc()
hpd0403<-sqldf('select a.*, b.AvgPrecip from hpd0403 a left join (select Key0, AvgPrecip from temp) b
           on a.Key4=b.Key0')
names(hpd0403)[names(hpd0403) == "AvgPrecip"] = "Closest4thPrecip"
gc()
hpd0403<-sqldf('select a.*, b.AvgPrecip from hpd0403 a left join (select Key0, AvgPrecip from temp) b
           on a.Key5=b.Key0')
names(hpd0403)[names(hpd0403) == "AvgPrecip"] = "Closest5thPrecip"
gc()

# Check for null values & impute using closest neighbours

rm(temp) # Remove temp file
rm(closestation) # Remove unrequired file

colSums(is.na(hpd0403)) # NA values in OrigPrecip column

hpd0403$OrigPrecip<-ifelse(is.na(hpd0403$OrigPrecip),rowMeans(hpd0403[,c("ClosestPrecip", "Closest2ndPrecip",
                                                                         "Closest3rdPrecip","Closest4thPrecip",
                                                                         "Closest5thPrecip")], na.rm=TRUE),
                           hpd0403$OrigPrecip)

saveRDS(hpd0403, file = "hpd0403.rds") # Saving externally