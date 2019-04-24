rm(list=ls())

library(lubridate)
library(sqldf)
library(DMwR)

hly200509<- read.table("200509hourly.txt", sep=",",header = T, dec = ".") # Read 09 File
str(hly200509)

# Removing units from visibility & changing to numeric
hly200509$Visibility<-as.character(hly200509$Visibility)
hly200509$Visibility <- gsub("SM","",hly200509$Visibility,fixed = TRUE)
hly200509$Visibility<-as.numeric(hly200509$Visibility)


# Splitting Sky Condition & Deriving the lowest ceiling height with Broken or Overcast
hly200509$SkyConditions<-as.character(hly200509$SkyConditions)
library(data.table)
setDT(hly200509)[, paste0("SC", 1:3) := tstrsplit(SkyConditions, " ")]
library(stringr)
hly200509$SC4<-ifelse(str_sub(hly200509$SC1,1,3)%in%c('BKN','OVC') | str_sub(hly200509$SC1,1,2) == 'VV',
                      as.integer(str_sub(hly200509$SC1,-2-1))*100,12000)
hly200509$SC5<-ifelse(str_sub(hly200509$SC2,1,3)%in%c('BKN','OVC') | str_sub(hly200509$SC2,1,2) == 'VV',
                      as.integer(str_sub(hly200509$SC2,-2-1))*100,12000)
hly200509$SC6<-ifelse(str_sub(hly200509$SC3,1,3)%in%c('BKN','OVC') | str_sub(hly200509$SC3,1,2) == 'VV',
                      as.integer(str_sub(hly200509$SC3,-2-1))*100,12000)

hly200509$SC4<-ifelse(is.na(hly200509$SC4),12000,hly200509$SC4)
hly200509$SC5<-ifelse(is.na(hly200509$SC5),12000,hly200509$SC5)
hly200509$SC6<-ifelse(is.na(hly200509$SC6),12000,hly200509$SC6)

str(hly200509)

hly200509$SC7<-apply(hly200509[,16:18], 1, min)

hly200509$SkyConditions<-hly200509$SC7
hly200509$SC1<-NULL
hly200509$SC2<-NULL
hly200509$SC3<-NULL
hly200509$SC4<-NULL
hly200509$SC5<-NULL
hly200509$SC6<-NULL
hly200509$SC7<-NULL

# Changing Format as applicable

hly200509$YearMonthDay<-ymd(hly200509$YearMonthDay)
hly200509$YearMonthDay<-as.factor(hly200509$YearMonthDay)
hly200509$WeatherStationID<-as.factor(hly200509$WeatherStationID)

# Deriving time slots in weather data

hly200509$TimeSlot<-ifelse(hly200509$Time<200,'Midnight to 2AM',ifelse(hly200509$Time<400,'2AM to 4AM',
                                                                       ifelse(hly200509$Time<600,'4AM to 6AM',
                                                                              ifelse(hly200509$Time<800,'6AM to 8AM',
                                                                                     ifelse(hly200509$Time<1000,'8AM to 10AM',
                                                                                            ifelse(hly200509$Time<1200,'10AM to Noon',
                                                                                                   ifelse(hly200509$Time<1400,'Noon to 2PM',
                                                                                                          ifelse(hly200509$Time<1600,'2PM to 4PM',
                                                                                                                 ifelse(hly200509$Time<1800,'4PM to 6PM',
                                                                                                                        ifelse(hly200509$Time<2000,'6PM to 8PM',
                                                                                                                               ifelse(hly200509$Time<2200,'8PM to 10PM','10PM to Midnight')))))))))))

hly200509$Time<-NULL # Dropping time column

# Aggregating Hourly Precipitation by Station, Date & Slot

hly200509<-sqldf('select distinct a.WeatherStationID, a.YearMonthDay, a.TimeSlot, avg(a.SkyConditions) as AvgSkyCond,
                 avg(a.Visibility) as AvgVis, avg(a.DBT) as AvgDBT, avg(a.DewPointTemp) as AvgDewPtTemp,
                 avg(a.RelativeHumidityPercent) as AvgRelHumPerc, avg(a.WindSpeed) as AvgWindSp,
                 avg(a.WindDirection) as AvgWindDir, avg(a.WindGustValue) as AvgWindGustVal,
                 avg(a.StationPressure) as AvgStnPres from hly200509 a group by
                 a.WeatherStationID, a.YearMonthDay, a.TimeSlot')

# Merging with close station data

closestation <- readRDS("closestation.rds")

hly0509<-merge(hly200509,closestation,by.x="WeatherStationID",by.y="WeatherStationID")

# Creating Keys for future merging

hly0509$Key0<-paste(hly0509$WeatherStationID,hly0509$YearMonthDay,hly0509$TimeSlot)
hly0509$Key1<-paste(hly0509$ClosestWS,hly0509$YearMonthDay,hly0509$TimeSlot)
hly0509$Key2<-paste(hly0509$Closest2ndWS,hly0509$YearMonthDay,hly0509$TimeSlot)
hly0509$Key3<-paste(hly0509$Closest3rdWS,hly0509$YearMonthDay,hly0509$TimeSlot)
hly0509$Key4<-paste(hly0509$Closest4thWS,hly0509$YearMonthDay,hly0509$TimeSlot)
hly0509$Key5<-paste(hly0509$Closest5thWS,hly0509$YearMonthDay,hly0509$TimeSlot)

# Merging with Closest Weather Stations

rm(hly200509) # Free up memory
temp<-hly0509

names(hly0509)[names(hly0509) == "AvgSkyCond"] = "OrigSkyCond"
names(hly0509)[names(hly0509) == "AvgVis"] = "OrigVis"
names(hly0509)[names(hly0509) == "AvgDBT"] = "OrigDBT"
names(hly0509)[names(hly0509) == "AvgDewPtTemp"] = "OrigDewPtTemp"
names(hly0509)[names(hly0509) == "AvgRelHumPerc"] = "OrigRelHumPerc"
names(hly0509)[names(hly0509) == "AvgWindSp"] = "OrigWindSp"
names(hly0509)[names(hly0509) == "AvgWindDir"] = "OrigWindDir"
names(hly0509)[names(hly0509) == "AvgWindGustVal"] = "OrigWindGustVal"
names(hly0509)[names(hly0509) == "AvgStnPres"] = "OrigStnPres"

hly0509<-sqldf('select a.*, b.AvgSkyCond, b.AvgVis, b.AvgDBT, b.AvgDewPtTemp, b.AvgRelHumPerc,
               b.AvgWindSp, b.AvgWindDir, b.AvgWindGustVal, b.AvgStnPres from hly0509 a left join
               (select Key0, AvgSkyCond, AvgVis, AvgDBT, AvgDewPtTemp, AvgRelHumPerc, AvgWindSp,
               AvgWindDir, AvgWindGustVal, AvgStnPres from temp) b on a.Key1 = b.Key0')

names(hly0509)[names(hly0509) == "AvgSkyCond"] = "ClosestSkyCond"
names(hly0509)[names(hly0509) == "AvgVis"] = "ClosestVis"
names(hly0509)[names(hly0509) == "AvgDBT"] = "ClosestDBT"
names(hly0509)[names(hly0509) == "AvgDewPtTemp"] = "ClosestDewPtTemp"
names(hly0509)[names(hly0509) == "AvgRelHumPerc"] = "ClosestRelHumPerc"
names(hly0509)[names(hly0509) == "AvgWindSp"] = "ClosestWindSp"
names(hly0509)[names(hly0509) == "AvgWindDir"] = "ClosestWindDir"
names(hly0509)[names(hly0509) == "AvgWindGustVal"] = "ClosestWindGustVal"
names(hly0509)[names(hly0509) == "AvgStnPres"] = "ClosestStnPres"

gc()

hly0509<-sqldf('select a.*, b.AvgSkyCond, b.AvgVis, b.AvgDBT, b.AvgDewPtTemp, b.AvgRelHumPerc,
               b.AvgWindSp, b.AvgWindDir, b.AvgWindGustVal, b.AvgStnPres from hly0509 a left join
               (select Key0, AvgSkyCond, AvgVis, AvgDBT, AvgDewPtTemp, AvgRelHumPerc, AvgWindSp,
               AvgWindDir, AvgWindGustVal, AvgStnPres from temp) b on a.Key2 = b.Key0')

names(hly0509)[names(hly0509) == "AvgSkyCond"] = "Closest2ndSkyCond"
names(hly0509)[names(hly0509) == "AvgVis"] = "Closest2ndVis"
names(hly0509)[names(hly0509) == "AvgDBT"] = "Closest2ndDBT"
names(hly0509)[names(hly0509) == "AvgDewPtTemp"] = "Closest2ndDewPtTemp"
names(hly0509)[names(hly0509) == "AvgRelHumPerc"] = "Closest2ndRelHumPerc"
names(hly0509)[names(hly0509) == "AvgWindSp"] = "Closest2ndWindSp"
names(hly0509)[names(hly0509) == "AvgWindDir"] = "Closest2ndWindDir"
names(hly0509)[names(hly0509) == "AvgWindGustVal"] = "Closest2ndWindGustVal"
names(hly0509)[names(hly0509) == "AvgStnPres"] = "Closest2ndStnPres"

gc()

hly0509<-sqldf('select a.*, b.AvgSkyCond, b.AvgVis, b.AvgDBT, b.AvgDewPtTemp, b.AvgRelHumPerc,
               b.AvgWindSp, b.AvgWindDir, b.AvgWindGustVal, b.AvgStnPres from hly0509 a left join
               (select Key0, AvgSkyCond, AvgVis, AvgDBT, AvgDewPtTemp, AvgRelHumPerc, AvgWindSp,
               AvgWindDir, AvgWindGustVal, AvgStnPres from temp) b on a.Key3 = b.Key0')

names(hly0509)[names(hly0509) == "AvgSkyCond"] = "Closest3rdSkyCond"
names(hly0509)[names(hly0509) == "AvgVis"] = "Closest3rdVis"
names(hly0509)[names(hly0509) == "AvgDBT"] = "Closest3rdDBT"
names(hly0509)[names(hly0509) == "AvgDewPtTemp"] = "Closest3rdDewPtTemp"
names(hly0509)[names(hly0509) == "AvgRelHumPerc"] = "Closest3rdRelHumPerc"
names(hly0509)[names(hly0509) == "AvgWindSp"] = "Closest3rdWindSp"
names(hly0509)[names(hly0509) == "AvgWindDir"] = "Closest3rdWindDir"
names(hly0509)[names(hly0509) == "AvgWindGustVal"] = "Closest3rdWindGustVal"
names(hly0509)[names(hly0509) == "AvgStnPres"] = "Closest3rdStnPres"

gc()

hly0509<-sqldf('select a.*, b.AvgSkyCond, b.AvgVis, b.AvgDBT, b.AvgDewPtTemp, b.AvgRelHumPerc,
               b.AvgWindSp, b.AvgWindDir, b.AvgWindGustVal, b.AvgStnPres from hly0509 a left join
               (select Key0, AvgSkyCond, AvgVis, AvgDBT, AvgDewPtTemp, AvgRelHumPerc, AvgWindSp,
               AvgWindDir, AvgWindGustVal, AvgStnPres from temp) b on a.Key4 = b.Key0')

names(hly0509)[names(hly0509) == "AvgSkyCond"] = "Closest4thSkyCond"
names(hly0509)[names(hly0509) == "AvgVis"] = "Closest4thVis"
names(hly0509)[names(hly0509) == "AvgDBT"] = "Closest4thDBT"
names(hly0509)[names(hly0509) == "AvgDewPtTemp"] = "Closest4thDewPtTemp"
names(hly0509)[names(hly0509) == "AvgRelHumPerc"] = "Closest4thRelHumPerc"
names(hly0509)[names(hly0509) == "AvgWindSp"] = "Closest4thWindSp"
names(hly0509)[names(hly0509) == "AvgWindDir"] = "Closest4thWindDir"
names(hly0509)[names(hly0509) == "AvgWindGustVal"] = "Closest4thWindGustVal"
names(hly0509)[names(hly0509) == "AvgStnPres"] = "Closest4thStnPres"

gc()

hly0509<-sqldf('select a.*, b.AvgSkyCond, b.AvgVis, b.AvgDBT, b.AvgDewPtTemp, b.AvgRelHumPerc,
               b.AvgWindSp, b.AvgWindDir, b.AvgWindGustVal, b.AvgStnPres from hly0509 a left join
               (select Key0, AvgSkyCond, AvgVis, AvgDBT, AvgDewPtTemp, AvgRelHumPerc, AvgWindSp,
               AvgWindDir, AvgWindGustVal, AvgStnPres from temp) b on a.Key5 = b.Key0')

names(hly0509)[names(hly0509) == "AvgSkyCond"] = "Closest5thSkyCond"
names(hly0509)[names(hly0509) == "AvgVis"] = "Closest5thVis"
names(hly0509)[names(hly0509) == "AvgDBT"] = "Closest5thDBT"
names(hly0509)[names(hly0509) == "AvgDewPtTemp"] = "Closest5thDewPtTemp"
names(hly0509)[names(hly0509) == "AvgRelHumPerc"] = "Closest5thRelHumPerc"
names(hly0509)[names(hly0509) == "AvgWindSp"] = "Closest5thWindSp"
names(hly0509)[names(hly0509) == "AvgWindDir"] = "Closest5thWindDir"
names(hly0509)[names(hly0509) == "AvgWindGustVal"] = "Closest5thWindGustVal"
names(hly0509)[names(hly0509) == "AvgStnPres"] = "Closest5thStnPres"

gc()

# Check for null values & impute using closest neighbours

rm(temp) # Remove temp file
rm(closestation) # Remove unrequired file

colSums(is.na(hly0509)) # NA values in OrigPrecip column

hly0509$OrigSkyCond<-ifelse(is.na(hly0509$OrigSkyCond),rowMeans(hly0509[,c("ClosestSkyCond", "Closest2ndSkyCond",
                                                                           "Closest3rdSkyCond","Closest4thSkyCond",
                                                                           "Closest5thSkyCond")], na.rm=TRUE),
                            hly0509$OrigSkyCond)

hly0509$OrigVis<-ifelse(is.na(hly0509$OrigVis),rowMeans(hly0509[,c("ClosestVis", "Closest2ndVis",
                                                                   "Closest3rdVis","Closest4thVis",
                                                                   "Closest5thVis")], na.rm=TRUE),
                        hly0509$OrigVis)

hly0509$OrigDBT<-ifelse(is.na(hly0509$OrigDBT),rowMeans(hly0509[,c("ClosestDBT", "Closest2ndDBT",
                                                                   "Closest3rdDBT","Closest4thDBT",
                                                                   "Closest5thDBT")], na.rm=TRUE),
                        hly0509$OrigDBT)

hly0509$OrigDewPtTemp<-ifelse(is.na(hly0509$OrigDewPtTemp),rowMeans(hly0509[,c("ClosestDewPtTemp", "Closest2ndDewPtTemp",
                                                                               "Closest3rdDewPtTemp","Closest4thDewPtTemp",
                                                                               "Closest5thDewPtTemp")], na.rm=TRUE),
                              hly0509$OrigDewPtTemp)

hly0509$OrigRelHumPerc<-ifelse(is.na(hly0509$OrigRelHumPerc),rowMeans(hly0509[,c("ClosestRelHumPerc", "Closest2ndRelHumPerc",
                                                                                 "Closest3rdRelHumPerc","Closest4thRelHumPerc",
                                                                                 "Closest5thRelHumPerc")], na.rm=TRUE),
                               hly0509$OrigRelHumPerc)

hly0509$OrigWindSp<-ifelse(is.na(hly0509$OrigWindSp),rowMeans(hly0509[,c("ClosestWindSp", "Closest2ndWindSp",
                                                                         "Closest3rdWindSp","Closest4thWindSp",
                                                                         "Closest5thWindSp")], na.rm=TRUE),
                           hly0509$OrigWindSp)

hly0509$OrigWindDir<-ifelse(is.na(hly0509$OrigWindDir),rowMeans(hly0509[,c("ClosestWindDir", "Closest2ndWindDir",
                                                                           "Closest3rdWindDir","Closest4thWindDir",
                                                                           "Closest5thWindDir")], na.rm=TRUE),
                            hly0509$OrigWindDir)

hly0509$OrigWindGustVal<-ifelse(is.na(hly0509$OrigWindGustVal),rowMeans(hly0509[,c("ClosestWindGustVal", "Closest2ndWindGustVal",
                                                                                   "Closest3rdWindGustVal","Closest4thWindGustVal",
                                                                                   "Closest5thWindGustVal")], na.rm=TRUE),
                                hly0509$OrigWindGustVal)

hly0509$OrigStnPres<-ifelse(is.na(hly0509$OrigStnPres),rowMeans(hly0509[,c("ClosestStnPres", "Closest2ndStnPres",
                                                                           "Closest3rdStnPres","Closest4thStnPres",
                                                                           "Closest5thStnPres")], na.rm=TRUE),
                            hly0509$OrigStnPres)

saveRDS(hly0509, file = "hly0509Test.rds") # Saving externally