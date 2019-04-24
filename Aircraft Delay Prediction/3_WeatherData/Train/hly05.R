rm(list=ls())

library(lubridate)
library(sqldf)
library(DMwR)

hly200405<- read.table("200405hourly.txt", sep=",",header = T, dec = ".") # Read 01 File
str(hly200405)

# Removing units from visibility & changing to numeric
hly200405$Visibility<-as.character(hly200405$Visibility)
hly200405$Visibility <- gsub("SM","",hly200405$Visibility,fixed = TRUE)
hly200405$Visibility<-as.numeric(hly200405$Visibility)


# Splitting Sky Condition & Deriving the lowest ceiling height with Broken or Overcast
hly200405$SkyConditions<-as.character(hly200405$SkyConditions)
library(data.table)
setDT(hly200405)[, paste0("SC", 1:3) := tstrsplit(SkyConditions, " ")]
library(stringr)
hly200405$SC4<-ifelse(str_sub(hly200405$SC1,1,3)%in%c('BKN','OVC') | str_sub(hly200405$SC1,1,2) == 'VV',
                      as.integer(str_sub(hly200405$SC1,-2-1))*100,12000)
hly200405$SC5<-ifelse(str_sub(hly200405$SC2,1,3)%in%c('BKN','OVC') | str_sub(hly200405$SC2,1,2) == 'VV',
                      as.integer(str_sub(hly200405$SC2,-2-1))*100,12000)
hly200405$SC6<-ifelse(str_sub(hly200405$SC3,1,3)%in%c('BKN','OVC') | str_sub(hly200405$SC3,1,2) == 'VV',
                      as.integer(str_sub(hly200405$SC3,-2-1))*100,12000)

hly200405$SC4<-ifelse(is.na(hly200405$SC4),12000,hly200405$SC4)
hly200405$SC5<-ifelse(is.na(hly200405$SC5),12000,hly200405$SC5)
hly200405$SC6<-ifelse(is.na(hly200405$SC6),12000,hly200405$SC6)

str(hly200405)

hly200405$SC7<-apply(hly200405[,16:18], 1, min)

hly200405$SkyConditions<-hly200405$SC7
hly200405$SC1<-NULL
hly200405$SC2<-NULL
hly200405$SC3<-NULL
hly200405$SC4<-NULL
hly200405$SC5<-NULL
hly200405$SC6<-NULL
hly200405$SC7<-NULL

# Changing Format as applicable

hly200405$YearMonthDay<-ymd(hly200405$YearMonthDay)
hly200405$YearMonthDay<-as.factor(hly200405$YearMonthDay)
hly200405$WeatherStationID<-as.factor(hly200405$WeatherStationID)

# Deriving time slots in weather data

hly200405$TimeSlot<-ifelse(hly200405$Time<200,'Midnight to 2AM',ifelse(hly200405$Time<400,'2AM to 4AM',
                                                                       ifelse(hly200405$Time<600,'4AM to 6AM',
                                                                              ifelse(hly200405$Time<800,'6AM to 8AM',
                                                                                     ifelse(hly200405$Time<1000,'8AM to 10AM',
                                                                                            ifelse(hly200405$Time<1200,'10AM to Noon',
                                                                                                   ifelse(hly200405$Time<1400,'Noon to 2PM',
                                                                                                          ifelse(hly200405$Time<1600,'2PM to 4PM',
                                                                                                                 ifelse(hly200405$Time<1800,'4PM to 6PM',
                                                                                                                        ifelse(hly200405$Time<2000,'6PM to 8PM',
                                                                                                                               ifelse(hly200405$Time<2200,'8PM to 10PM','10PM to Midnight')))))))))))

hly200405$Time<-NULL # Dropping time column

# Aggregating Hourly Precipitation by Station, Date & Slot

hly200405<-sqldf('select distinct a.WeatherStationID, a.YearMonthDay, a.TimeSlot, avg(a.SkyConditions) as AvgSkyCond,
                 avg(a.Visibility) as AvgVis, avg(a.DBT) as AvgDBT, avg(a.DewPointTemp) as AvgDewPtTemp,
                 avg(a.RelativeHumidityPercent) as AvgRelHumPerc, avg(a.WindSpeed) as AvgWindSp,
                 avg(a.WindDirection) as AvgWindDir, avg(a.WindGustValue) as AvgWindGustVal,
                 avg(a.StationPressure) as AvgStnPres from hly200405 a group by
                 a.WeatherStationID, a.YearMonthDay, a.TimeSlot')

# Merging with close station data

closestation <- readRDS("closestation.rds")

hly0405<-merge(hly200405,closestation,by.x="WeatherStationID",by.y="WeatherStationID")

# Creating Keys for future merging

hly0405$Key0<-paste(hly0405$WeatherStationID,hly0405$YearMonthDay,hly0405$TimeSlot)
hly0405$Key1<-paste(hly0405$ClosestWS,hly0405$YearMonthDay,hly0405$TimeSlot)
hly0405$Key2<-paste(hly0405$Closest2ndWS,hly0405$YearMonthDay,hly0405$TimeSlot)
hly0405$Key3<-paste(hly0405$Closest3rdWS,hly0405$YearMonthDay,hly0405$TimeSlot)
hly0405$Key4<-paste(hly0405$Closest4thWS,hly0405$YearMonthDay,hly0405$TimeSlot)
hly0405$Key5<-paste(hly0405$Closest5thWS,hly0405$YearMonthDay,hly0405$TimeSlot)

# Merging with Closest Weather Stations

rm(hly200405) # Free up memory
temp<-hly0405

names(hly0405)[names(hly0405) == "AvgSkyCond"] = "OrigSkyCond"
names(hly0405)[names(hly0405) == "AvgVis"] = "OrigVis"
names(hly0405)[names(hly0405) == "AvgDBT"] = "OrigDBT"
names(hly0405)[names(hly0405) == "AvgDewPtTemp"] = "OrigDewPtTemp"
names(hly0405)[names(hly0405) == "AvgRelHumPerc"] = "OrigRelHumPerc"
names(hly0405)[names(hly0405) == "AvgWindSp"] = "OrigWindSp"
names(hly0405)[names(hly0405) == "AvgWindDir"] = "OrigWindDir"
names(hly0405)[names(hly0405) == "AvgWindGustVal"] = "OrigWindGustVal"
names(hly0405)[names(hly0405) == "AvgStnPres"] = "OrigStnPres"

hly0405<-sqldf('select a.*, b.AvgSkyCond, b.AvgVis, b.AvgDBT, b.AvgDewPtTemp, b.AvgRelHumPerc,
               b.AvgWindSp, b.AvgWindDir, b.AvgWindGustVal, b.AvgStnPres from hly0405 a left join
               (select Key0, AvgSkyCond, AvgVis, AvgDBT, AvgDewPtTemp, AvgRelHumPerc, AvgWindSp,
               AvgWindDir, AvgWindGustVal, AvgStnPres from temp) b on a.Key1 = b.Key0')

names(hly0405)[names(hly0405) == "AvgSkyCond"] = "ClosestSkyCond"
names(hly0405)[names(hly0405) == "AvgVis"] = "ClosestVis"
names(hly0405)[names(hly0405) == "AvgDBT"] = "ClosestDBT"
names(hly0405)[names(hly0405) == "AvgDewPtTemp"] = "ClosestDewPtTemp"
names(hly0405)[names(hly0405) == "AvgRelHumPerc"] = "ClosestRelHumPerc"
names(hly0405)[names(hly0405) == "AvgWindSp"] = "ClosestWindSp"
names(hly0405)[names(hly0405) == "AvgWindDir"] = "ClosestWindDir"
names(hly0405)[names(hly0405) == "AvgWindGustVal"] = "ClosestWindGustVal"
names(hly0405)[names(hly0405) == "AvgStnPres"] = "ClosestStnPres"

gc()

hly0405<-sqldf('select a.*, b.AvgSkyCond, b.AvgVis, b.AvgDBT, b.AvgDewPtTemp, b.AvgRelHumPerc,
               b.AvgWindSp, b.AvgWindDir, b.AvgWindGustVal, b.AvgStnPres from hly0405 a left join
               (select Key0, AvgSkyCond, AvgVis, AvgDBT, AvgDewPtTemp, AvgRelHumPerc, AvgWindSp,
               AvgWindDir, AvgWindGustVal, AvgStnPres from temp) b on a.Key2 = b.Key0')

names(hly0405)[names(hly0405) == "AvgSkyCond"] = "Closest2ndSkyCond"
names(hly0405)[names(hly0405) == "AvgVis"] = "Closest2ndVis"
names(hly0405)[names(hly0405) == "AvgDBT"] = "Closest2ndDBT"
names(hly0405)[names(hly0405) == "AvgDewPtTemp"] = "Closest2ndDewPtTemp"
names(hly0405)[names(hly0405) == "AvgRelHumPerc"] = "Closest2ndRelHumPerc"
names(hly0405)[names(hly0405) == "AvgWindSp"] = "Closest2ndWindSp"
names(hly0405)[names(hly0405) == "AvgWindDir"] = "Closest2ndWindDir"
names(hly0405)[names(hly0405) == "AvgWindGustVal"] = "Closest2ndWindGustVal"
names(hly0405)[names(hly0405) == "AvgStnPres"] = "Closest2ndStnPres"

gc()

hly0405<-sqldf('select a.*, b.AvgSkyCond, b.AvgVis, b.AvgDBT, b.AvgDewPtTemp, b.AvgRelHumPerc,
               b.AvgWindSp, b.AvgWindDir, b.AvgWindGustVal, b.AvgStnPres from hly0405 a left join
               (select Key0, AvgSkyCond, AvgVis, AvgDBT, AvgDewPtTemp, AvgRelHumPerc, AvgWindSp,
               AvgWindDir, AvgWindGustVal, AvgStnPres from temp) b on a.Key3 = b.Key0')

names(hly0405)[names(hly0405) == "AvgSkyCond"] = "Closest3rdSkyCond"
names(hly0405)[names(hly0405) == "AvgVis"] = "Closest3rdVis"
names(hly0405)[names(hly0405) == "AvgDBT"] = "Closest3rdDBT"
names(hly0405)[names(hly0405) == "AvgDewPtTemp"] = "Closest3rdDewPtTemp"
names(hly0405)[names(hly0405) == "AvgRelHumPerc"] = "Closest3rdRelHumPerc"
names(hly0405)[names(hly0405) == "AvgWindSp"] = "Closest3rdWindSp"
names(hly0405)[names(hly0405) == "AvgWindDir"] = "Closest3rdWindDir"
names(hly0405)[names(hly0405) == "AvgWindGustVal"] = "Closest3rdWindGustVal"
names(hly0405)[names(hly0405) == "AvgStnPres"] = "Closest3rdStnPres"

gc()

hly0405<-sqldf('select a.*, b.AvgSkyCond, b.AvgVis, b.AvgDBT, b.AvgDewPtTemp, b.AvgRelHumPerc,
               b.AvgWindSp, b.AvgWindDir, b.AvgWindGustVal, b.AvgStnPres from hly0405 a left join
               (select Key0, AvgSkyCond, AvgVis, AvgDBT, AvgDewPtTemp, AvgRelHumPerc, AvgWindSp,
               AvgWindDir, AvgWindGustVal, AvgStnPres from temp) b on a.Key4 = b.Key0')

names(hly0405)[names(hly0405) == "AvgSkyCond"] = "Closest4thSkyCond"
names(hly0405)[names(hly0405) == "AvgVis"] = "Closest4thVis"
names(hly0405)[names(hly0405) == "AvgDBT"] = "Closest4thDBT"
names(hly0405)[names(hly0405) == "AvgDewPtTemp"] = "Closest4thDewPtTemp"
names(hly0405)[names(hly0405) == "AvgRelHumPerc"] = "Closest4thRelHumPerc"
names(hly0405)[names(hly0405) == "AvgWindSp"] = "Closest4thWindSp"
names(hly0405)[names(hly0405) == "AvgWindDir"] = "Closest4thWindDir"
names(hly0405)[names(hly0405) == "AvgWindGustVal"] = "Closest4thWindGustVal"
names(hly0405)[names(hly0405) == "AvgStnPres"] = "Closest4thStnPres"

gc()

hly0405<-sqldf('select a.*, b.AvgSkyCond, b.AvgVis, b.AvgDBT, b.AvgDewPtTemp, b.AvgRelHumPerc,
               b.AvgWindSp, b.AvgWindDir, b.AvgWindGustVal, b.AvgStnPres from hly0405 a left join
               (select Key0, AvgSkyCond, AvgVis, AvgDBT, AvgDewPtTemp, AvgRelHumPerc, AvgWindSp,
               AvgWindDir, AvgWindGustVal, AvgStnPres from temp) b on a.Key5 = b.Key0')

names(hly0405)[names(hly0405) == "AvgSkyCond"] = "Closest5thSkyCond"
names(hly0405)[names(hly0405) == "AvgVis"] = "Closest5thVis"
names(hly0405)[names(hly0405) == "AvgDBT"] = "Closest5thDBT"
names(hly0405)[names(hly0405) == "AvgDewPtTemp"] = "Closest5thDewPtTemp"
names(hly0405)[names(hly0405) == "AvgRelHumPerc"] = "Closest5thRelHumPerc"
names(hly0405)[names(hly0405) == "AvgWindSp"] = "Closest5thWindSp"
names(hly0405)[names(hly0405) == "AvgWindDir"] = "Closest5thWindDir"
names(hly0405)[names(hly0405) == "AvgWindGustVal"] = "Closest5thWindGustVal"
names(hly0405)[names(hly0405) == "AvgStnPres"] = "Closest5thStnPres"

gc()

# Check for null values & impute using closest neighbours

rm(temp) # Remove temp file
rm(closestation) # Remove unrequired file

colSums(is.na(hly0405)) # NA values in OrigPrecip column

hly0405$OrigSkyCond<-ifelse(is.na(hly0405$OrigSkyCond),rowMeans(hly0405[,c("ClosestSkyCond", "Closest2ndSkyCond",
                                                                           "Closest3rdSkyCond","Closest4thSkyCond",
                                                                           "Closest5thSkyCond")], na.rm=TRUE),
                            hly0405$OrigSkyCond)

hly0405$OrigVis<-ifelse(is.na(hly0405$OrigVis),rowMeans(hly0405[,c("ClosestVis", "Closest2ndVis",
                                                                   "Closest3rdVis","Closest4thVis",
                                                                   "Closest5thVis")], na.rm=TRUE),
                        hly0405$OrigVis)

hly0405$OrigDBT<-ifelse(is.na(hly0405$OrigDBT),rowMeans(hly0405[,c("ClosestDBT", "Closest2ndDBT",
                                                                   "Closest3rdDBT","Closest4thDBT",
                                                                   "Closest5thDBT")], na.rm=TRUE),
                        hly0405$OrigDBT)

hly0405$OrigDewPtTemp<-ifelse(is.na(hly0405$OrigDewPtTemp),rowMeans(hly0405[,c("ClosestDewPtTemp", "Closest2ndDewPtTemp",
                                                                               "Closest3rdDewPtTemp","Closest4thDewPtTemp",
                                                                               "Closest5thDewPtTemp")], na.rm=TRUE),
                              hly0405$OrigDewPtTemp)

hly0405$OrigRelHumPerc<-ifelse(is.na(hly0405$OrigRelHumPerc),rowMeans(hly0405[,c("ClosestRelHumPerc", "Closest2ndRelHumPerc",
                                                                                 "Closest3rdRelHumPerc","Closest4thRelHumPerc",
                                                                                 "Closest5thRelHumPerc")], na.rm=TRUE),
                               hly0405$OrigRelHumPerc)

hly0405$OrigWindSp<-ifelse(is.na(hly0405$OrigWindSp),rowMeans(hly0405[,c("ClosestWindSp", "Closest2ndWindSp",
                                                                         "Closest3rdWindSp","Closest4thWindSp",
                                                                         "Closest5thWindSp")], na.rm=TRUE),
                           hly0405$OrigWindSp)

hly0405$OrigWindDir<-ifelse(is.na(hly0405$OrigWindDir),rowMeans(hly0405[,c("ClosestWindDir", "Closest2ndWindDir",
                                                                           "Closest3rdWindDir","Closest4thWindDir",
                                                                           "Closest5thWindDir")], na.rm=TRUE),
                            hly0405$OrigWindDir)

hly0405$OrigWindGustVal<-ifelse(is.na(hly0405$OrigWindGustVal),rowMeans(hly0405[,c("ClosestWindGustVal", "Closest2ndWindGustVal",
                                                                                   "Closest3rdWindGustVal","Closest4thWindGustVal",
                                                                                   "Closest5thWindGustVal")], na.rm=TRUE),
                                hly0405$OrigWindGustVal)

hly0405$OrigStnPres<-ifelse(is.na(hly0405$OrigStnPres),rowMeans(hly0405[,c("ClosestStnPres", "Closest2ndStnPres",
                                                                           "Closest3rdStnPres","Closest4thStnPres",
                                                                           "Closest5thStnPres")], na.rm=TRUE),
                            hly0405$OrigStnPres)

saveRDS(hly0405, file = "hly0405.rds") # Saving externally