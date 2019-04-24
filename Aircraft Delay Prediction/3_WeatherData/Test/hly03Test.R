rm(list=ls())

library(lubridate)
library(sqldf)
library(DMwR)

hly200503<- read.table("200503hourly.txt", sep=",",header = T, dec = ".") # Read 03 File
str(hly200503)

# Removing units from visibility & changing to numeric
hly200503$Visibility<-as.character(hly200503$Visibility)
hly200503$Visibility <- gsub("SM","",hly200503$Visibility,fixed = TRUE)
hly200503$Visibility<-as.numeric(hly200503$Visibility)


# Splitting Sky Condition & Deriving the lowest ceiling height with Broken or Overcast
hly200503$SkyConditions<-as.character(hly200503$SkyConditions)
library(data.table)
setDT(hly200503)[, paste0("SC", 1:3) := tstrsplit(SkyConditions, " ")]
library(stringr)
hly200503$SC4<-ifelse(str_sub(hly200503$SC1,1,3)%in%c('BKN','OVC') | str_sub(hly200503$SC1,1,2) == 'VV',
                      as.integer(str_sub(hly200503$SC1,-2-1))*100,12000)
hly200503$SC5<-ifelse(str_sub(hly200503$SC2,1,3)%in%c('BKN','OVC') | str_sub(hly200503$SC2,1,2) == 'VV',
                      as.integer(str_sub(hly200503$SC2,-2-1))*100,12000)
hly200503$SC6<-ifelse(str_sub(hly200503$SC3,1,3)%in%c('BKN','OVC') | str_sub(hly200503$SC3,1,2) == 'VV',
                      as.integer(str_sub(hly200503$SC3,-2-1))*100,12000)

hly200503$SC4<-ifelse(is.na(hly200503$SC4),12000,hly200503$SC4)
hly200503$SC5<-ifelse(is.na(hly200503$SC5),12000,hly200503$SC5)
hly200503$SC6<-ifelse(is.na(hly200503$SC6),12000,hly200503$SC6)

str(hly200503)

hly200503$SC7<-apply(hly200503[,16:18], 1, min)

hly200503$SkyConditions<-hly200503$SC7
hly200503$SC1<-NULL
hly200503$SC2<-NULL
hly200503$SC3<-NULL
hly200503$SC4<-NULL
hly200503$SC5<-NULL
hly200503$SC6<-NULL
hly200503$SC7<-NULL

# Changing Format as applicable

hly200503$YearMonthDay<-ymd(hly200503$YearMonthDay)
hly200503$YearMonthDay<-as.factor(hly200503$YearMonthDay)
hly200503$WeatherStationID<-as.factor(hly200503$WeatherStationID)

# Deriving time slots in weather data

hly200503$TimeSlot<-ifelse(hly200503$Time<200,'Midnight to 2AM',ifelse(hly200503$Time<400,'2AM to 4AM',
                                                                       ifelse(hly200503$Time<600,'4AM to 6AM',
                                                                              ifelse(hly200503$Time<800,'6AM to 8AM',
                                                                                     ifelse(hly200503$Time<1000,'8AM to 10AM',
                                                                                            ifelse(hly200503$Time<1200,'10AM to Noon',
                                                                                                   ifelse(hly200503$Time<1400,'Noon to 2PM',
                                                                                                          ifelse(hly200503$Time<1600,'2PM to 4PM',
                                                                                                                 ifelse(hly200503$Time<1800,'4PM to 6PM',
                                                                                                                        ifelse(hly200503$Time<2000,'6PM to 8PM',
                                                                                                                               ifelse(hly200503$Time<2200,'8PM to 10PM','10PM to Midnight')))))))))))

hly200503$Time<-NULL # Dropping time column

# Aggregating Hourly Precipitation by Station, Date & Slot

hly200503<-sqldf('select distinct a.WeatherStationID, a.YearMonthDay, a.TimeSlot, avg(a.SkyConditions) as AvgSkyCond,
                 avg(a.Visibility) as AvgVis, avg(a.DBT) as AvgDBT, avg(a.DewPointTemp) as AvgDewPtTemp,
                 avg(a.RelativeHumidityPercent) as AvgRelHumPerc, avg(a.WindSpeed) as AvgWindSp,
                 avg(a.WindDirection) as AvgWindDir, avg(a.WindGustValue) as AvgWindGustVal,
                 avg(a.StationPressure) as AvgStnPres from hly200503 a group by
                 a.WeatherStationID, a.YearMonthDay, a.TimeSlot')

# Merging with close station data

closestation <- readRDS("closestation.rds")

hly0503<-merge(hly200503,closestation,by.x="WeatherStationID",by.y="WeatherStationID")

# Creating Keys for future merging

hly0503$Key0<-paste(hly0503$WeatherStationID,hly0503$YearMonthDay,hly0503$TimeSlot)
hly0503$Key1<-paste(hly0503$ClosestWS,hly0503$YearMonthDay,hly0503$TimeSlot)
hly0503$Key2<-paste(hly0503$Closest2ndWS,hly0503$YearMonthDay,hly0503$TimeSlot)
hly0503$Key3<-paste(hly0503$Closest3rdWS,hly0503$YearMonthDay,hly0503$TimeSlot)
hly0503$Key4<-paste(hly0503$Closest4thWS,hly0503$YearMonthDay,hly0503$TimeSlot)
hly0503$Key5<-paste(hly0503$Closest5thWS,hly0503$YearMonthDay,hly0503$TimeSlot)

# Merging with Closest Weather Stations

rm(hly200503) # Free up memory
temp<-hly0503

names(hly0503)[names(hly0503) == "AvgSkyCond"] = "OrigSkyCond"
names(hly0503)[names(hly0503) == "AvgVis"] = "OrigVis"
names(hly0503)[names(hly0503) == "AvgDBT"] = "OrigDBT"
names(hly0503)[names(hly0503) == "AvgDewPtTemp"] = "OrigDewPtTemp"
names(hly0503)[names(hly0503) == "AvgRelHumPerc"] = "OrigRelHumPerc"
names(hly0503)[names(hly0503) == "AvgWindSp"] = "OrigWindSp"
names(hly0503)[names(hly0503) == "AvgWindDir"] = "OrigWindDir"
names(hly0503)[names(hly0503) == "AvgWindGustVal"] = "OrigWindGustVal"
names(hly0503)[names(hly0503) == "AvgStnPres"] = "OrigStnPres"

hly0503<-sqldf('select a.*, b.AvgSkyCond, b.AvgVis, b.AvgDBT, b.AvgDewPtTemp, b.AvgRelHumPerc,
               b.AvgWindSp, b.AvgWindDir, b.AvgWindGustVal, b.AvgStnPres from hly0503 a left join
               (select Key0, AvgSkyCond, AvgVis, AvgDBT, AvgDewPtTemp, AvgRelHumPerc, AvgWindSp,
               AvgWindDir, AvgWindGustVal, AvgStnPres from temp) b on a.Key1 = b.Key0')

names(hly0503)[names(hly0503) == "AvgSkyCond"] = "ClosestSkyCond"
names(hly0503)[names(hly0503) == "AvgVis"] = "ClosestVis"
names(hly0503)[names(hly0503) == "AvgDBT"] = "ClosestDBT"
names(hly0503)[names(hly0503) == "AvgDewPtTemp"] = "ClosestDewPtTemp"
names(hly0503)[names(hly0503) == "AvgRelHumPerc"] = "ClosestRelHumPerc"
names(hly0503)[names(hly0503) == "AvgWindSp"] = "ClosestWindSp"
names(hly0503)[names(hly0503) == "AvgWindDir"] = "ClosestWindDir"
names(hly0503)[names(hly0503) == "AvgWindGustVal"] = "ClosestWindGustVal"
names(hly0503)[names(hly0503) == "AvgStnPres"] = "ClosestStnPres"

gc()

hly0503<-sqldf('select a.*, b.AvgSkyCond, b.AvgVis, b.AvgDBT, b.AvgDewPtTemp, b.AvgRelHumPerc,
               b.AvgWindSp, b.AvgWindDir, b.AvgWindGustVal, b.AvgStnPres from hly0503 a left join
               (select Key0, AvgSkyCond, AvgVis, AvgDBT, AvgDewPtTemp, AvgRelHumPerc, AvgWindSp,
               AvgWindDir, AvgWindGustVal, AvgStnPres from temp) b on a.Key2 = b.Key0')

names(hly0503)[names(hly0503) == "AvgSkyCond"] = "Closest2ndSkyCond"
names(hly0503)[names(hly0503) == "AvgVis"] = "Closest2ndVis"
names(hly0503)[names(hly0503) == "AvgDBT"] = "Closest2ndDBT"
names(hly0503)[names(hly0503) == "AvgDewPtTemp"] = "Closest2ndDewPtTemp"
names(hly0503)[names(hly0503) == "AvgRelHumPerc"] = "Closest2ndRelHumPerc"
names(hly0503)[names(hly0503) == "AvgWindSp"] = "Closest2ndWindSp"
names(hly0503)[names(hly0503) == "AvgWindDir"] = "Closest2ndWindDir"
names(hly0503)[names(hly0503) == "AvgWindGustVal"] = "Closest2ndWindGustVal"
names(hly0503)[names(hly0503) == "AvgStnPres"] = "Closest2ndStnPres"

gc()

hly0503<-sqldf('select a.*, b.AvgSkyCond, b.AvgVis, b.AvgDBT, b.AvgDewPtTemp, b.AvgRelHumPerc,
               b.AvgWindSp, b.AvgWindDir, b.AvgWindGustVal, b.AvgStnPres from hly0503 a left join
               (select Key0, AvgSkyCond, AvgVis, AvgDBT, AvgDewPtTemp, AvgRelHumPerc, AvgWindSp,
               AvgWindDir, AvgWindGustVal, AvgStnPres from temp) b on a.Key3 = b.Key0')

names(hly0503)[names(hly0503) == "AvgSkyCond"] = "Closest3rdSkyCond"
names(hly0503)[names(hly0503) == "AvgVis"] = "Closest3rdVis"
names(hly0503)[names(hly0503) == "AvgDBT"] = "Closest3rdDBT"
names(hly0503)[names(hly0503) == "AvgDewPtTemp"] = "Closest3rdDewPtTemp"
names(hly0503)[names(hly0503) == "AvgRelHumPerc"] = "Closest3rdRelHumPerc"
names(hly0503)[names(hly0503) == "AvgWindSp"] = "Closest3rdWindSp"
names(hly0503)[names(hly0503) == "AvgWindDir"] = "Closest3rdWindDir"
names(hly0503)[names(hly0503) == "AvgWindGustVal"] = "Closest3rdWindGustVal"
names(hly0503)[names(hly0503) == "AvgStnPres"] = "Closest3rdStnPres"

gc()

hly0503<-sqldf('select a.*, b.AvgSkyCond, b.AvgVis, b.AvgDBT, b.AvgDewPtTemp, b.AvgRelHumPerc,
               b.AvgWindSp, b.AvgWindDir, b.AvgWindGustVal, b.AvgStnPres from hly0503 a left join
               (select Key0, AvgSkyCond, AvgVis, AvgDBT, AvgDewPtTemp, AvgRelHumPerc, AvgWindSp,
               AvgWindDir, AvgWindGustVal, AvgStnPres from temp) b on a.Key4 = b.Key0')

names(hly0503)[names(hly0503) == "AvgSkyCond"] = "Closest4thSkyCond"
names(hly0503)[names(hly0503) == "AvgVis"] = "Closest4thVis"
names(hly0503)[names(hly0503) == "AvgDBT"] = "Closest4thDBT"
names(hly0503)[names(hly0503) == "AvgDewPtTemp"] = "Closest4thDewPtTemp"
names(hly0503)[names(hly0503) == "AvgRelHumPerc"] = "Closest4thRelHumPerc"
names(hly0503)[names(hly0503) == "AvgWindSp"] = "Closest4thWindSp"
names(hly0503)[names(hly0503) == "AvgWindDir"] = "Closest4thWindDir"
names(hly0503)[names(hly0503) == "AvgWindGustVal"] = "Closest4thWindGustVal"
names(hly0503)[names(hly0503) == "AvgStnPres"] = "Closest4thStnPres"

gc()

hly0503<-sqldf('select a.*, b.AvgSkyCond, b.AvgVis, b.AvgDBT, b.AvgDewPtTemp, b.AvgRelHumPerc,
               b.AvgWindSp, b.AvgWindDir, b.AvgWindGustVal, b.AvgStnPres from hly0503 a left join
               (select Key0, AvgSkyCond, AvgVis, AvgDBT, AvgDewPtTemp, AvgRelHumPerc, AvgWindSp,
               AvgWindDir, AvgWindGustVal, AvgStnPres from temp) b on a.Key5 = b.Key0')

names(hly0503)[names(hly0503) == "AvgSkyCond"] = "Closest5thSkyCond"
names(hly0503)[names(hly0503) == "AvgVis"] = "Closest5thVis"
names(hly0503)[names(hly0503) == "AvgDBT"] = "Closest5thDBT"
names(hly0503)[names(hly0503) == "AvgDewPtTemp"] = "Closest5thDewPtTemp"
names(hly0503)[names(hly0503) == "AvgRelHumPerc"] = "Closest5thRelHumPerc"
names(hly0503)[names(hly0503) == "AvgWindSp"] = "Closest5thWindSp"
names(hly0503)[names(hly0503) == "AvgWindDir"] = "Closest5thWindDir"
names(hly0503)[names(hly0503) == "AvgWindGustVal"] = "Closest5thWindGustVal"
names(hly0503)[names(hly0503) == "AvgStnPres"] = "Closest5thStnPres"

gc()

# Check for null values & impute using closest neighbours

rm(temp) # Remove temp file
rm(closestation) # Remove unrequired file

colSums(is.na(hly0503)) # NA values in OrigPrecip column

hly0503$OrigSkyCond<-ifelse(is.na(hly0503$OrigSkyCond),rowMeans(hly0503[,c("ClosestSkyCond", "Closest2ndSkyCond",
                                                                           "Closest3rdSkyCond","Closest4thSkyCond",
                                                                           "Closest5thSkyCond")], na.rm=TRUE),
                            hly0503$OrigSkyCond)

hly0503$OrigVis<-ifelse(is.na(hly0503$OrigVis),rowMeans(hly0503[,c("ClosestVis", "Closest2ndVis",
                                                                   "Closest3rdVis","Closest4thVis",
                                                                   "Closest5thVis")], na.rm=TRUE),
                        hly0503$OrigVis)

hly0503$OrigDBT<-ifelse(is.na(hly0503$OrigDBT),rowMeans(hly0503[,c("ClosestDBT", "Closest2ndDBT",
                                                                   "Closest3rdDBT","Closest4thDBT",
                                                                   "Closest5thDBT")], na.rm=TRUE),
                        hly0503$OrigDBT)

hly0503$OrigDewPtTemp<-ifelse(is.na(hly0503$OrigDewPtTemp),rowMeans(hly0503[,c("ClosestDewPtTemp", "Closest2ndDewPtTemp",
                                                                               "Closest3rdDewPtTemp","Closest4thDewPtTemp",
                                                                               "Closest5thDewPtTemp")], na.rm=TRUE),
                              hly0503$OrigDewPtTemp)

hly0503$OrigRelHumPerc<-ifelse(is.na(hly0503$OrigRelHumPerc),rowMeans(hly0503[,c("ClosestRelHumPerc", "Closest2ndRelHumPerc",
                                                                                 "Closest3rdRelHumPerc","Closest4thRelHumPerc",
                                                                                 "Closest5thRelHumPerc")], na.rm=TRUE),
                               hly0503$OrigRelHumPerc)

hly0503$OrigWindSp<-ifelse(is.na(hly0503$OrigWindSp),rowMeans(hly0503[,c("ClosestWindSp", "Closest2ndWindSp",
                                                                         "Closest3rdWindSp","Closest4thWindSp",
                                                                         "Closest5thWindSp")], na.rm=TRUE),
                           hly0503$OrigWindSp)

hly0503$OrigWindDir<-ifelse(is.na(hly0503$OrigWindDir),rowMeans(hly0503[,c("ClosestWindDir", "Closest2ndWindDir",
                                                                           "Closest3rdWindDir","Closest4thWindDir",
                                                                           "Closest5thWindDir")], na.rm=TRUE),
                            hly0503$OrigWindDir)

hly0503$OrigWindGustVal<-ifelse(is.na(hly0503$OrigWindGustVal),rowMeans(hly0503[,c("ClosestWindGustVal", "Closest2ndWindGustVal",
                                                                                   "Closest3rdWindGustVal","Closest4thWindGustVal",
                                                                                   "Closest5thWindGustVal")], na.rm=TRUE),
                                hly0503$OrigWindGustVal)

hly0503$OrigStnPres<-ifelse(is.na(hly0503$OrigStnPres),rowMeans(hly0503[,c("ClosestStnPres", "Closest2ndStnPres",
                                                                           "Closest3rdStnPres","Closest4thStnPres",
                                                                           "Closest5thStnPres")], na.rm=TRUE),
                            hly0503$OrigStnPres)

saveRDS(hly0503, file = "hly0503Test.rds") # Saving externally