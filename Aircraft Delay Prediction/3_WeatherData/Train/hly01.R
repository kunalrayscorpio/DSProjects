rm(list=ls())

library(lubridate)
library(sqldf)
library(DMwR)

hly200401<- read.table("200401hourly.txt", sep=",",header = T, dec = ".") # Read 01 File
str(hly200401)

# Removing units from visibility & changing to numeric
hly200401$Visibility<-as.character(hly200401$Visibility)
hly200401$Visibility <- gsub("SM","",hly200401$Visibility,fixed = TRUE)
hly200401$Visibility<-as.numeric(hly200401$Visibility)


# Splitting Sky Condition & Deriving the lowest ceiling height with Broken or Overcast
hly200401$SkyConditions<-as.character(hly200401$SkyConditions)
library(data.table)
setDT(hly200401)[, paste0("SC", 1:3) := tstrsplit(SkyConditions, " ")]
library(stringr)
hly200401$SC4<-ifelse(str_sub(hly200401$SC1,1,3)%in%c('BKN','OVC') | str_sub(hly200401$SC1,1,2) == 'VV',
                      as.integer(str_sub(hly200401$SC1,-2-1))*100,12000)
hly200401$SC5<-ifelse(str_sub(hly200401$SC2,1,3)%in%c('BKN','OVC') | str_sub(hly200401$SC2,1,2) == 'VV',
                      as.integer(str_sub(hly200401$SC2,-2-1))*100,12000)
hly200401$SC6<-ifelse(str_sub(hly200401$SC3,1,3)%in%c('BKN','OVC') | str_sub(hly200401$SC3,1,2) == 'VV',
                      as.integer(str_sub(hly200401$SC3,-2-1))*100,12000)

hly200401$SC4<-ifelse(is.na(hly200401$SC4),12000,hly200401$SC4)
hly200401$SC5<-ifelse(is.na(hly200401$SC5),12000,hly200401$SC5)
hly200401$SC6<-ifelse(is.na(hly200401$SC6),12000,hly200401$SC6)

str(hly200401)

hly200401$SC7<-apply(hly200401[,16:18], 1, min)

hly200401$SkyConditions<-hly200401$SC7
hly200401$SC1<-NULL
hly200401$SC2<-NULL
hly200401$SC3<-NULL
hly200401$SC4<-NULL
hly200401$SC5<-NULL
hly200401$SC6<-NULL
hly200401$SC7<-NULL

# Changing Format as applicable

hly200401$YearMonthDay<-ymd(hly200401$YearMonthDay)
hly200401$YearMonthDay<-as.factor(hly200401$YearMonthDay)
hly200401$WeatherStationID<-as.factor(hly200401$WeatherStationID)

# Deriving time slots in weather data

hly200401$TimeSlot<-ifelse(hly200401$Time<200,'Midnight to 2AM',ifelse(hly200401$Time<400,'2AM to 4AM',
                                                                       ifelse(hly200401$Time<600,'4AM to 6AM',
                                                                              ifelse(hly200401$Time<800,'6AM to 8AM',
                                                                                     ifelse(hly200401$Time<1000,'8AM to 10AM',
                                                                                            ifelse(hly200401$Time<1200,'10AM to Noon',
                                                                                                   ifelse(hly200401$Time<1400,'Noon to 2PM',
                                                                                                          ifelse(hly200401$Time<1600,'2PM to 4PM',
                                                                                                                 ifelse(hly200401$Time<1800,'4PM to 6PM',
                                                                                                                        ifelse(hly200401$Time<2000,'6PM to 8PM',
                                                                                                                               ifelse(hly200401$Time<2200,'8PM to 10PM','10PM to Midnight')))))))))))

hly200401$Time<-NULL # Dropping time column

# Aggregating Data by Station, Date & Slot

hly200401<-sqldf('select distinct a.WeatherStationID, a.YearMonthDay, a.TimeSlot, avg(a.SkyConditions) as AvgSkyCond,
                 avg(a.Visibility) as AvgVis, avg(a.DBT) as AvgDBT, avg(a.DewPointTemp) as AvgDewPtTemp,
                 avg(a.RelativeHumidityPercent) as AvgRelHumPerc, avg(a.WindSpeed) as AvgWindSp,
                 avg(a.WindDirection) as AvgWindDir, avg(a.WindGustValue) as AvgWindGustVal,
                 avg(a.StationPressure) as AvgStnPres from hly200401 a group by
                 a.WeatherStationID, a.YearMonthDay, a.TimeSlot')

# Merging with close station data

closestation <- readRDS("closestation.rds")

hly0401<-merge(hly200401,closestation,by.x="WeatherStationID",by.y="WeatherStationID")

# Creating Keys for future merging

hly0401$Key0<-paste(hly0401$WeatherStationID,hly0401$YearMonthDay,hly0401$TimeSlot)
hly0401$Key1<-paste(hly0401$ClosestWS,hly0401$YearMonthDay,hly0401$TimeSlot)
hly0401$Key2<-paste(hly0401$Closest2ndWS,hly0401$YearMonthDay,hly0401$TimeSlot)
hly0401$Key3<-paste(hly0401$Closest3rdWS,hly0401$YearMonthDay,hly0401$TimeSlot)
hly0401$Key4<-paste(hly0401$Closest4thWS,hly0401$YearMonthDay,hly0401$TimeSlot)
hly0401$Key5<-paste(hly0401$Closest5thWS,hly0401$YearMonthDay,hly0401$TimeSlot)

# Merging with Closest Weather Stations

rm(hly200401) # Free up memory
temp<-hly0401

names(hly0401)[names(hly0401) == "AvgSkyCond"] = "OrigSkyCond"
names(hly0401)[names(hly0401) == "AvgVis"] = "OrigVis"
names(hly0401)[names(hly0401) == "AvgDBT"] = "OrigDBT"
names(hly0401)[names(hly0401) == "AvgDewPtTemp"] = "OrigDewPtTemp"
names(hly0401)[names(hly0401) == "AvgRelHumPerc"] = "OrigRelHumPerc"
names(hly0401)[names(hly0401) == "AvgWindSp"] = "OrigWindSp"
names(hly0401)[names(hly0401) == "AvgWindDir"] = "OrigWindDir"
names(hly0401)[names(hly0401) == "AvgWindGustVal"] = "OrigWindGustVal"
names(hly0401)[names(hly0401) == "AvgStnPres"] = "OrigStnPres"

hly0401<-sqldf('select a.*, b.AvgSkyCond, b.AvgVis, b.AvgDBT, b.AvgDewPtTemp, b.AvgRelHumPerc,
               b.AvgWindSp, b.AvgWindDir, b.AvgWindGustVal, b.AvgStnPres from hly0401 a left join
               (select Key0, AvgSkyCond, AvgVis, AvgDBT, AvgDewPtTemp, AvgRelHumPerc, AvgWindSp,
               AvgWindDir, AvgWindGustVal, AvgStnPres from temp) b on a.Key1 = b.Key0')

names(hly0401)[names(hly0401) == "AvgSkyCond"] = "ClosestSkyCond"
names(hly0401)[names(hly0401) == "AvgVis"] = "ClosestVis"
names(hly0401)[names(hly0401) == "AvgDBT"] = "ClosestDBT"
names(hly0401)[names(hly0401) == "AvgDewPtTemp"] = "ClosestDewPtTemp"
names(hly0401)[names(hly0401) == "AvgRelHumPerc"] = "ClosestRelHumPerc"
names(hly0401)[names(hly0401) == "AvgWindSp"] = "ClosestWindSp"
names(hly0401)[names(hly0401) == "AvgWindDir"] = "ClosestWindDir"
names(hly0401)[names(hly0401) == "AvgWindGustVal"] = "ClosestWindGustVal"
names(hly0401)[names(hly0401) == "AvgStnPres"] = "ClosestStnPres"

gc()

hly0401<-sqldf('select a.*, b.AvgSkyCond, b.AvgVis, b.AvgDBT, b.AvgDewPtTemp, b.AvgRelHumPerc,
               b.AvgWindSp, b.AvgWindDir, b.AvgWindGustVal, b.AvgStnPres from hly0401 a left join
               (select Key0, AvgSkyCond, AvgVis, AvgDBT, AvgDewPtTemp, AvgRelHumPerc, AvgWindSp,
               AvgWindDir, AvgWindGustVal, AvgStnPres from temp) b on a.Key2 = b.Key0')

names(hly0401)[names(hly0401) == "AvgSkyCond"] = "Closest2ndSkyCond"
names(hly0401)[names(hly0401) == "AvgVis"] = "Closest2ndVis"
names(hly0401)[names(hly0401) == "AvgDBT"] = "Closest2ndDBT"
names(hly0401)[names(hly0401) == "AvgDewPtTemp"] = "Closest2ndDewPtTemp"
names(hly0401)[names(hly0401) == "AvgRelHumPerc"] = "Closest2ndRelHumPerc"
names(hly0401)[names(hly0401) == "AvgWindSp"] = "Closest2ndWindSp"
names(hly0401)[names(hly0401) == "AvgWindDir"] = "Closest2ndWindDir"
names(hly0401)[names(hly0401) == "AvgWindGustVal"] = "Closest2ndWindGustVal"
names(hly0401)[names(hly0401) == "AvgStnPres"] = "Closest2ndStnPres"

gc()

hly0401<-sqldf('select a.*, b.AvgSkyCond, b.AvgVis, b.AvgDBT, b.AvgDewPtTemp, b.AvgRelHumPerc,
               b.AvgWindSp, b.AvgWindDir, b.AvgWindGustVal, b.AvgStnPres from hly0401 a left join
               (select Key0, AvgSkyCond, AvgVis, AvgDBT, AvgDewPtTemp, AvgRelHumPerc, AvgWindSp,
               AvgWindDir, AvgWindGustVal, AvgStnPres from temp) b on a.Key3 = b.Key0')

names(hly0401)[names(hly0401) == "AvgSkyCond"] = "Closest3rdSkyCond"
names(hly0401)[names(hly0401) == "AvgVis"] = "Closest3rdVis"
names(hly0401)[names(hly0401) == "AvgDBT"] = "Closest3rdDBT"
names(hly0401)[names(hly0401) == "AvgDewPtTemp"] = "Closest3rdDewPtTemp"
names(hly0401)[names(hly0401) == "AvgRelHumPerc"] = "Closest3rdRelHumPerc"
names(hly0401)[names(hly0401) == "AvgWindSp"] = "Closest3rdWindSp"
names(hly0401)[names(hly0401) == "AvgWindDir"] = "Closest3rdWindDir"
names(hly0401)[names(hly0401) == "AvgWindGustVal"] = "Closest3rdWindGustVal"
names(hly0401)[names(hly0401) == "AvgStnPres"] = "Closest3rdStnPres"

gc()

hly0401<-sqldf('select a.*, b.AvgSkyCond, b.AvgVis, b.AvgDBT, b.AvgDewPtTemp, b.AvgRelHumPerc,
               b.AvgWindSp, b.AvgWindDir, b.AvgWindGustVal, b.AvgStnPres from hly0401 a left join
               (select Key0, AvgSkyCond, AvgVis, AvgDBT, AvgDewPtTemp, AvgRelHumPerc, AvgWindSp,
               AvgWindDir, AvgWindGustVal, AvgStnPres from temp) b on a.Key4 = b.Key0')

names(hly0401)[names(hly0401) == "AvgSkyCond"] = "Closest4thSkyCond"
names(hly0401)[names(hly0401) == "AvgVis"] = "Closest4thVis"
names(hly0401)[names(hly0401) == "AvgDBT"] = "Closest4thDBT"
names(hly0401)[names(hly0401) == "AvgDewPtTemp"] = "Closest4thDewPtTemp"
names(hly0401)[names(hly0401) == "AvgRelHumPerc"] = "Closest4thRelHumPerc"
names(hly0401)[names(hly0401) == "AvgWindSp"] = "Closest4thWindSp"
names(hly0401)[names(hly0401) == "AvgWindDir"] = "Closest4thWindDir"
names(hly0401)[names(hly0401) == "AvgWindGustVal"] = "Closest4thWindGustVal"
names(hly0401)[names(hly0401) == "AvgStnPres"] = "Closest4thStnPres"

gc()

hly0401<-sqldf('select a.*, b.AvgSkyCond, b.AvgVis, b.AvgDBT, b.AvgDewPtTemp, b.AvgRelHumPerc,
               b.AvgWindSp, b.AvgWindDir, b.AvgWindGustVal, b.AvgStnPres from hly0401 a left join
               (select Key0, AvgSkyCond, AvgVis, AvgDBT, AvgDewPtTemp, AvgRelHumPerc, AvgWindSp,
               AvgWindDir, AvgWindGustVal, AvgStnPres from temp) b on a.Key5 = b.Key0')

names(hly0401)[names(hly0401) == "AvgSkyCond"] = "Closest5thSkyCond"
names(hly0401)[names(hly0401) == "AvgVis"] = "Closest5thVis"
names(hly0401)[names(hly0401) == "AvgDBT"] = "Closest5thDBT"
names(hly0401)[names(hly0401) == "AvgDewPtTemp"] = "Closest5thDewPtTemp"
names(hly0401)[names(hly0401) == "AvgRelHumPerc"] = "Closest5thRelHumPerc"
names(hly0401)[names(hly0401) == "AvgWindSp"] = "Closest5thWindSp"
names(hly0401)[names(hly0401) == "AvgWindDir"] = "Closest5thWindDir"
names(hly0401)[names(hly0401) == "AvgWindGustVal"] = "Closest5thWindGustVal"
names(hly0401)[names(hly0401) == "AvgStnPres"] = "Closest5thStnPres"

gc()

# Check for null values & impute using closest neighbours

rm(temp) # Remove temp file
rm(closestation) # Remove unrequired file

colSums(is.na(hly0401)) # NA values in OrigPrecip column

hly0401$OrigSkyCond<-ifelse(is.na(hly0401$OrigSkyCond),rowMeans(hly0401[,c("ClosestSkyCond", "Closest2ndSkyCond",
                                                                         "Closest3rdSkyCond","Closest4thSkyCond",
                                                                         "Closest5thSkyCond")], na.rm=TRUE),
                           hly0401$OrigSkyCond)

hly0401$OrigVis<-ifelse(is.na(hly0401$OrigVis),rowMeans(hly0401[,c("ClosestVis", "Closest2ndVis",
                                                                           "Closest3rdVis","Closest4thVis",
                                                                           "Closest5thVis")], na.rm=TRUE),
                            hly0401$OrigVis)

hly0401$OrigDBT<-ifelse(is.na(hly0401$OrigDBT),rowMeans(hly0401[,c("ClosestDBT", "Closest2ndDBT",
                                                                   "Closest3rdDBT","Closest4thDBT",
                                                                   "Closest5thDBT")], na.rm=TRUE),
                        hly0401$OrigDBT)

hly0401$OrigDewPtTemp<-ifelse(is.na(hly0401$OrigDewPtTemp),rowMeans(hly0401[,c("ClosestDewPtTemp", "Closest2ndDewPtTemp",
                                                                   "Closest3rdDewPtTemp","Closest4thDewPtTemp",
                                                                   "Closest5thDewPtTemp")], na.rm=TRUE),
                        hly0401$OrigDewPtTemp)

hly0401$OrigRelHumPerc<-ifelse(is.na(hly0401$OrigRelHumPerc),rowMeans(hly0401[,c("ClosestRelHumPerc", "Closest2ndRelHumPerc",
                                                                               "Closest3rdRelHumPerc","Closest4thRelHumPerc",
                                                                               "Closest5thRelHumPerc")], na.rm=TRUE),
                              hly0401$OrigRelHumPerc)

hly0401$OrigWindSp<-ifelse(is.na(hly0401$OrigWindSp),rowMeans(hly0401[,c("ClosestWindSp", "Closest2ndWindSp",
                                                                                 "Closest3rdWindSp","Closest4thWindSp",
                                                                                 "Closest5thWindSp")], na.rm=TRUE),
                               hly0401$OrigWindSp)

hly0401$OrigWindDir<-ifelse(is.na(hly0401$OrigWindDir),rowMeans(hly0401[,c("ClosestWindDir", "Closest2ndWindDir",
                                                                         "Closest3rdWindDir","Closest4thWindDir",
                                                                         "Closest5thWindDir")], na.rm=TRUE),
                           hly0401$OrigWindDir)

hly0401$OrigWindGustVal<-ifelse(is.na(hly0401$OrigWindGustVal),rowMeans(hly0401[,c("ClosestWindGustVal", "Closest2ndWindGustVal",
                                                                           "Closest3rdWindGustVal","Closest4thWindGustVal",
                                                                           "Closest5thWindGustVal")], na.rm=TRUE),
                            hly0401$OrigWindGustVal)

hly0401$OrigStnPres<-ifelse(is.na(hly0401$OrigStnPres),rowMeans(hly0401[,c("ClosestStnPres", "Closest2ndStnPres",
                                                                                   "Closest3rdStnPres","Closest4thStnPres",
                                                                                   "Closest5thStnPres")], na.rm=TRUE),
                                hly0401$OrigStnPres)

saveRDS(hly0401, file = "hly0401.rds") # Saving externally