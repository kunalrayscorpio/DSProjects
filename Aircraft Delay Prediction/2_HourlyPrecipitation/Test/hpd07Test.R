rm(list=ls())

library(lubridate)
library(sqldf)
library(DMwR)

hpd200507<- read.table("200507hpd.txt", sep=",",header = T, dec = ".") # Read 07 File

# Changing Format as applicable

hpd200507$YearMonthDay<-ymd(hpd200507$YearMonthDay)
hpd200507$YearMonthDay<-as.factor(hpd200507$YearMonthDay)
hpd200507$WeatherStationID<-as.factor(hpd200507$WeatherStationID)

# Deriving time slots in weather data

hpd200507$TimeSlot<-ifelse(hpd200507$Time<200,'Midnight to 2AM',ifelse(hpd200507$Time<400,'2AM to 4AM',
                                                                       ifelse(hpd200507$Time<600,'4AM to 6AM',
                                                                              ifelse(hpd200507$Time<800,'6AM to 8AM',
                                                                                     ifelse(hpd200507$Time<1000,'8AM to 10AM',
                                                                                            ifelse(hpd200507$Time<1200,'10AM to Noon',
                                                                                                   ifelse(hpd200507$Time<1400,'Noon to 2PM',
                                                                                                          ifelse(hpd200507$Time<1600,'2PM to 4PM',
                                                                                                                 ifelse(hpd200507$Time<1800,'4PM to 6PM',
                                                                                                                        ifelse(hpd200507$Time<2000,'6PM to 8PM',
                                                                                                                               ifelse(hpd200507$Time<2200,'8PM to 10PM','10PM to Midnight')))))))))))

hpd200507$Time<-NULL # Dropping time column

# Aggregating Hourly Precipitation by Station, Date & Slot

hpd200507<-sqldf('select distinct a.WeatherStationID, a.YearMonthDay, a.TimeSlot, avg(a.HourlyPrecip) as AvgPrecip
                 from hpd200507 a group by a.WeatherStationID, a.YearMonthDay, a.TimeSlot')

# Merging with close station data

closestation <- readRDS("closestation.rds")

hpd0507<-merge(hpd200507,closestation,by.x="WeatherStationID",by.y="WeatherStationID")

# Creating Keys for future merging

hpd0507$Key0<-paste(hpd0507$WeatherStationID,hpd0507$YearMonthDay,hpd0507$TimeSlot)
hpd0507$Key1<-paste(hpd0507$ClosestWS,hpd0507$YearMonthDay,hpd0507$TimeSlot)
hpd0507$Key2<-paste(hpd0507$Closest2ndWS,hpd0507$YearMonthDay,hpd0507$TimeSlot)
hpd0507$Key3<-paste(hpd0507$Closest3rdWS,hpd0507$YearMonthDay,hpd0507$TimeSlot)
hpd0507$Key4<-paste(hpd0507$Closest4thWS,hpd0507$YearMonthDay,hpd0507$TimeSlot)
hpd0507$Key5<-paste(hpd0507$Closest5thWS,hpd0507$YearMonthDay,hpd0507$TimeSlot)

# Merging with Closest Weather Stations

rm(hpd200507) # Free up memory
temp<-hpd0507
names(hpd0507)[names(hpd0507) == "AvgPrecip"] = "OrigPrecip"
hpd0507<-sqldf('select a.*, b.AvgPrecip from hpd0507 a left join (select Key0, AvgPrecip from temp) b
               on a.Key1=b.Key0')
names(hpd0507)[names(hpd0507) == "AvgPrecip"] = "ClosestPrecip"
gc()
hpd0507<-sqldf('select a.*, b.AvgPrecip from hpd0507 a left join (select Key0, AvgPrecip from temp) b
               on a.Key2=b.Key0')
names(hpd0507)[names(hpd0507) == "AvgPrecip"] = "Closest2ndPrecip"
gc()
hpd0507<-sqldf('select a.*, b.AvgPrecip from hpd0507 a left join (select Key0, AvgPrecip from temp) b
               on a.Key3=b.Key0')
names(hpd0507)[names(hpd0507) == "AvgPrecip"] = "Closest3rdPrecip"
gc()
hpd0507<-sqldf('select a.*, b.AvgPrecip from hpd0507 a left join (select Key0, AvgPrecip from temp) b
               on a.Key4=b.Key0')
names(hpd0507)[names(hpd0507) == "AvgPrecip"] = "Closest4thPrecip"
gc()
hpd0507<-sqldf('select a.*, b.AvgPrecip from hpd0507 a left join (select Key0, AvgPrecip from temp) b
               on a.Key5=b.Key0')
names(hpd0507)[names(hpd0507) == "AvgPrecip"] = "Closest5thPrecip"
gc()

# Check for null values & impute using closest neighbours

rm(temp) # Remove temp file
rm(closestation) # Remove unrequired file

colSums(is.na(hpd0507)) # NA values in OrigPrecip column

hpd0507$OrigPrecip<-ifelse(is.na(hpd0507$OrigPrecip),rowMeans(hpd0507[,c("ClosestPrecip", "Closest2ndPrecip",
                                                                         "Closest3rdPrecip","Closest4thPrecip",
                                                                         "Closest5thPrecip")], na.rm=TRUE),
                           hpd0507$OrigPrecip)

saveRDS(hpd0507, file = "hpd0507Test.rds") # Saving externally