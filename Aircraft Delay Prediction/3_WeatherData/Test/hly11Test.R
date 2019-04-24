rm(list=ls())

library(lubridate)
library(sqldf)
library(DMwR)

hly200511<- read.table("200511hourly.txt", sep=",",header = T, dec = ".") # Read 11 File
str(hly200511)

# Removing units from visibility & changing to numeric
hly200511$Visibility<-as.character(hly200511$Visibility)
hly200511$Visibility <- gsub("SM","",hly200511$Visibility,fixed = TRUE)
hly200511$Visibility<-as.numeric(hly200511$Visibility)


# Splitting Sky Condition & Deriving the lowest ceiling height with Broken or Overcast
hly200511$SkyConditions<-as.character(hly200511$SkyConditions)
library(data.table)
setDT(hly200511)[, paste0("SC", 1:3) := tstrsplit(SkyConditions, " ")]
library(stringr)
hly200511$SC4<-ifelse(str_sub(hly200511$SC1,1,3)%in%c('BKN','OVC') | str_sub(hly200511$SC1,1,2) == 'VV',
                      as.integer(str_sub(hly200511$SC1,-2-1))*100,12000)
hly200511$SC5<-ifelse(str_sub(hly200511$SC2,1,3)%in%c('BKN','OVC') | str_sub(hly200511$SC2,1,2) == 'VV',
                      as.integer(str_sub(hly200511$SC2,-2-1))*100,12000)
hly200511$SC6<-ifelse(str_sub(hly200511$SC3,1,3)%in%c('BKN','OVC') | str_sub(hly200511$SC3,1,2) == 'VV',
                      as.integer(str_sub(hly200511$SC3,-2-1))*100,12000)

hly200511$SC4<-ifelse(is.na(hly200511$SC4),12000,hly200511$SC4)
hly200511$SC5<-ifelse(is.na(hly200511$SC5),12000,hly200511$SC5)
hly200511$SC6<-ifelse(is.na(hly200511$SC6),12000,hly200511$SC6)

str(hly200511)

hly200511$SC7<-apply(hly200511[,16:18], 1, min)

hly200511$SkyConditions<-hly200511$SC7
hly200511$SC1<-NULL
hly200511$SC2<-NULL
hly200511$SC3<-NULL
hly200511$SC4<-NULL
hly200511$SC5<-NULL
hly200511$SC6<-NULL
hly200511$SC7<-NULL

# Changing Format as applicable

hly200511$YearMonthDay<-ymd(hly200511$YearMonthDay)
hly200511$YearMonthDay<-as.factor(hly200511$YearMonthDay)
hly200511$WeatherStationID<-as.factor(hly200511$WeatherStationID)

# Deriving time slots in weather data

hly200511$TimeSlot<-ifelse(hly200511$Time<200,'Midnight to 2AM',ifelse(hly200511$Time<400,'2AM to 4AM',
                                                                       ifelse(hly200511$Time<600,'4AM to 6AM',
                                                                              ifelse(hly200511$Time<800,'6AM to 8AM',
                                                                                     ifelse(hly200511$Time<1000,'8AM to 10AM',
                                                                                            ifelse(hly200511$Time<1200,'10AM to Noon',
                                                                                                   ifelse(hly200511$Time<1400,'Noon to 2PM',
                                                                                                          ifelse(hly200511$Time<1600,'2PM to 4PM',
                                                                                                                 ifelse(hly200511$Time<1800,'4PM to 6PM',
                                                                                                                        ifelse(hly200511$Time<2000,'6PM to 8PM',
                                                                                                                               ifelse(hly200511$Time<2200,'8PM to 10PM','10PM to Midnight')))))))))))

hly200511$Time<-NULL # Dropping time column

# Aggregating Hourly Precipitation by Station, Date & Slot

hly200511<-sqldf('select distinct a.WeatherStationID, a.YearMonthDay, a.TimeSlot, avg(a.SkyConditions) as AvgSkyCond,
                 avg(a.Visibility) as AvgVis, avg(a.DBT) as AvgDBT, avg(a.DewPointTemp) as AvgDewPtTemp,
                 avg(a.RelativeHumidityPercent) as AvgRelHumPerc, avg(a.WindSpeed) as AvgWindSp,
                 avg(a.WindDirection) as AvgWindDir, avg(a.WindGustValue) as AvgWindGustVal,
                 avg(a.StationPressure) as AvgStnPres from hly200511 a group by
                 a.WeatherStationID, a.YearMonthDay, a.TimeSlot')

# Merging with close station data

closestation <- readRDS("closestation.rds")

hly0511<-merge(hly200511,closestation,by.x="WeatherStationID",by.y="WeatherStationID")

# Creating Keys for future merging

hly0511$Key0<-paste(hly0511$WeatherStationID,hly0511$YearMonthDay,hly0511$TimeSlot)
hly0511$Key1<-paste(hly0511$ClosestWS,hly0511$YearMonthDay,hly0511$TimeSlot)
hly0511$Key2<-paste(hly0511$Closest2ndWS,hly0511$YearMonthDay,hly0511$TimeSlot)
hly0511$Key3<-paste(hly0511$Closest3rdWS,hly0511$YearMonthDay,hly0511$TimeSlot)
hly0511$Key4<-paste(hly0511$Closest4thWS,hly0511$YearMonthDay,hly0511$TimeSlot)
hly0511$Key5<-paste(hly0511$Closest5thWS,hly0511$YearMonthDay,hly0511$TimeSlot)

# Merging with Closest Weather Stations

rm(hly200511) # Free up memory
temp<-hly0511

names(hly0511)[names(hly0511) == "AvgSkyCond"] = "OrigSkyCond"
names(hly0511)[names(hly0511) == "AvgVis"] = "OrigVis"
names(hly0511)[names(hly0511) == "AvgDBT"] = "OrigDBT"
names(hly0511)[names(hly0511) == "AvgDewPtTemp"] = "OrigDewPtTemp"
names(hly0511)[names(hly0511) == "AvgRelHumPerc"] = "OrigRelHumPerc"
names(hly0511)[names(hly0511) == "AvgWindSp"] = "OrigWindSp"
names(hly0511)[names(hly0511) == "AvgWindDir"] = "OrigWindDir"
names(hly0511)[names(hly0511) == "AvgWindGustVal"] = "OrigWindGustVal"
names(hly0511)[names(hly0511) == "AvgStnPres"] = "OrigStnPres"

hly0511<-sqldf('select a.*, b.AvgSkyCond, b.AvgVis, b.AvgDBT, b.AvgDewPtTemp, b.AvgRelHumPerc,
               b.AvgWindSp, b.AvgWindDir, b.AvgWindGustVal, b.AvgStnPres from hly0511 a left join
               (select Key0, AvgSkyCond, AvgVis, AvgDBT, AvgDewPtTemp, AvgRelHumPerc, AvgWindSp,
               AvgWindDir, AvgWindGustVal, AvgStnPres from temp) b on a.Key1 = b.Key0')

names(hly0511)[names(hly0511) == "AvgSkyCond"] = "ClosestSkyCond"
names(hly0511)[names(hly0511) == "AvgVis"] = "ClosestVis"
names(hly0511)[names(hly0511) == "AvgDBT"] = "ClosestDBT"
names(hly0511)[names(hly0511) == "AvgDewPtTemp"] = "ClosestDewPtTemp"
names(hly0511)[names(hly0511) == "AvgRelHumPerc"] = "ClosestRelHumPerc"
names(hly0511)[names(hly0511) == "AvgWindSp"] = "ClosestWindSp"
names(hly0511)[names(hly0511) == "AvgWindDir"] = "ClosestWindDir"
names(hly0511)[names(hly0511) == "AvgWindGustVal"] = "ClosestWindGustVal"
names(hly0511)[names(hly0511) == "AvgStnPres"] = "ClosestStnPres"

gc()

hly0511<-sqldf('select a.*, b.AvgSkyCond, b.AvgVis, b.AvgDBT, b.AvgDewPtTemp, b.AvgRelHumPerc,
               b.AvgWindSp, b.AvgWindDir, b.AvgWindGustVal, b.AvgStnPres from hly0511 a left join
               (select Key0, AvgSkyCond, AvgVis, AvgDBT, AvgDewPtTemp, AvgRelHumPerc, AvgWindSp,
               AvgWindDir, AvgWindGustVal, AvgStnPres from temp) b on a.Key2 = b.Key0')

names(hly0511)[names(hly0511) == "AvgSkyCond"] = "Closest2ndSkyCond"
names(hly0511)[names(hly0511) == "AvgVis"] = "Closest2ndVis"
names(hly0511)[names(hly0511) == "AvgDBT"] = "Closest2ndDBT"
names(hly0511)[names(hly0511) == "AvgDewPtTemp"] = "Closest2ndDewPtTemp"
names(hly0511)[names(hly0511) == "AvgRelHumPerc"] = "Closest2ndRelHumPerc"
names(hly0511)[names(hly0511) == "AvgWindSp"] = "Closest2ndWindSp"
names(hly0511)[names(hly0511) == "AvgWindDir"] = "Closest2ndWindDir"
names(hly0511)[names(hly0511) == "AvgWindGustVal"] = "Closest2ndWindGustVal"
names(hly0511)[names(hly0511) == "AvgStnPres"] = "Closest2ndStnPres"

gc()

hly0511<-sqldf('select a.*, b.AvgSkyCond, b.AvgVis, b.AvgDBT, b.AvgDewPtTemp, b.AvgRelHumPerc,
               b.AvgWindSp, b.AvgWindDir, b.AvgWindGustVal, b.AvgStnPres from hly0511 a left join
               (select Key0, AvgSkyCond, AvgVis, AvgDBT, AvgDewPtTemp, AvgRelHumPerc, AvgWindSp,
               AvgWindDir, AvgWindGustVal, AvgStnPres from temp) b on a.Key3 = b.Key0')

names(hly0511)[names(hly0511) == "AvgSkyCond"] = "Closest3rdSkyCond"
names(hly0511)[names(hly0511) == "AvgVis"] = "Closest3rdVis"
names(hly0511)[names(hly0511) == "AvgDBT"] = "Closest3rdDBT"
names(hly0511)[names(hly0511) == "AvgDewPtTemp"] = "Closest3rdDewPtTemp"
names(hly0511)[names(hly0511) == "AvgRelHumPerc"] = "Closest3rdRelHumPerc"
names(hly0511)[names(hly0511) == "AvgWindSp"] = "Closest3rdWindSp"
names(hly0511)[names(hly0511) == "AvgWindDir"] = "Closest3rdWindDir"
names(hly0511)[names(hly0511) == "AvgWindGustVal"] = "Closest3rdWindGustVal"
names(hly0511)[names(hly0511) == "AvgStnPres"] = "Closest3rdStnPres"

gc()

hly0511<-sqldf('select a.*, b.AvgSkyCond, b.AvgVis, b.AvgDBT, b.AvgDewPtTemp, b.AvgRelHumPerc,
               b.AvgWindSp, b.AvgWindDir, b.AvgWindGustVal, b.AvgStnPres from hly0511 a left join
               (select Key0, AvgSkyCond, AvgVis, AvgDBT, AvgDewPtTemp, AvgRelHumPerc, AvgWindSp,
               AvgWindDir, AvgWindGustVal, AvgStnPres from temp) b on a.Key4 = b.Key0')

names(hly0511)[names(hly0511) == "AvgSkyCond"] = "Closest4thSkyCond"
names(hly0511)[names(hly0511) == "AvgVis"] = "Closest4thVis"
names(hly0511)[names(hly0511) == "AvgDBT"] = "Closest4thDBT"
names(hly0511)[names(hly0511) == "AvgDewPtTemp"] = "Closest4thDewPtTemp"
names(hly0511)[names(hly0511) == "AvgRelHumPerc"] = "Closest4thRelHumPerc"
names(hly0511)[names(hly0511) == "AvgWindSp"] = "Closest4thWindSp"
names(hly0511)[names(hly0511) == "AvgWindDir"] = "Closest4thWindDir"
names(hly0511)[names(hly0511) == "AvgWindGustVal"] = "Closest4thWindGustVal"
names(hly0511)[names(hly0511) == "AvgStnPres"] = "Closest4thStnPres"

gc()

hly0511<-sqldf('select a.*, b.AvgSkyCond, b.AvgVis, b.AvgDBT, b.AvgDewPtTemp, b.AvgRelHumPerc,
               b.AvgWindSp, b.AvgWindDir, b.AvgWindGustVal, b.AvgStnPres from hly0511 a left join
               (select Key0, AvgSkyCond, AvgVis, AvgDBT, AvgDewPtTemp, AvgRelHumPerc, AvgWindSp,
               AvgWindDir, AvgWindGustVal, AvgStnPres from temp) b on a.Key5 = b.Key0')

names(hly0511)[names(hly0511) == "AvgSkyCond"] = "Closest5thSkyCond"
names(hly0511)[names(hly0511) == "AvgVis"] = "Closest5thVis"
names(hly0511)[names(hly0511) == "AvgDBT"] = "Closest5thDBT"
names(hly0511)[names(hly0511) == "AvgDewPtTemp"] = "Closest5thDewPtTemp"
names(hly0511)[names(hly0511) == "AvgRelHumPerc"] = "Closest5thRelHumPerc"
names(hly0511)[names(hly0511) == "AvgWindSp"] = "Closest5thWindSp"
names(hly0511)[names(hly0511) == "AvgWindDir"] = "Closest5thWindDir"
names(hly0511)[names(hly0511) == "AvgWindGustVal"] = "Closest5thWindGustVal"
names(hly0511)[names(hly0511) == "AvgStnPres"] = "Closest5thStnPres"

gc()

# Check for null values & impute using closest neighbours

rm(temp) # Remove temp file
rm(closestation) # Remove unrequired file

colSums(is.na(hly0511)) # NA values in OrigPrecip column

hly0511$OrigSkyCond<-ifelse(is.na(hly0511$OrigSkyCond),rowMeans(hly0511[,c("ClosestSkyCond", "Closest2ndSkyCond",
                                                                           "Closest3rdSkyCond","Closest4thSkyCond",
                                                                           "Closest5thSkyCond")], na.rm=TRUE),
                            hly0511$OrigSkyCond)

hly0511$OrigVis<-ifelse(is.na(hly0511$OrigVis),rowMeans(hly0511[,c("ClosestVis", "Closest2ndVis",
                                                                   "Closest3rdVis","Closest4thVis",
                                                                   "Closest5thVis")], na.rm=TRUE),
                        hly0511$OrigVis)

hly0511$OrigDBT<-ifelse(is.na(hly0511$OrigDBT),rowMeans(hly0511[,c("ClosestDBT", "Closest2ndDBT",
                                                                   "Closest3rdDBT","Closest4thDBT",
                                                                   "Closest5thDBT")], na.rm=TRUE),
                        hly0511$OrigDBT)

hly0511$OrigDewPtTemp<-ifelse(is.na(hly0511$OrigDewPtTemp),rowMeans(hly0511[,c("ClosestDewPtTemp", "Closest2ndDewPtTemp",
                                                                               "Closest3rdDewPtTemp","Closest4thDewPtTemp",
                                                                               "Closest5thDewPtTemp")], na.rm=TRUE),
                              hly0511$OrigDewPtTemp)

hly0511$OrigRelHumPerc<-ifelse(is.na(hly0511$OrigRelHumPerc),rowMeans(hly0511[,c("ClosestRelHumPerc", "Closest2ndRelHumPerc",
                                                                                 "Closest3rdRelHumPerc","Closest4thRelHumPerc",
                                                                                 "Closest5thRelHumPerc")], na.rm=TRUE),
                               hly0511$OrigRelHumPerc)

hly0511$OrigWindSp<-ifelse(is.na(hly0511$OrigWindSp),rowMeans(hly0511[,c("ClosestWindSp", "Closest2ndWindSp",
                                                                         "Closest3rdWindSp","Closest4thWindSp",
                                                                         "Closest5thWindSp")], na.rm=TRUE),
                           hly0511$OrigWindSp)

hly0511$OrigWindDir<-ifelse(is.na(hly0511$OrigWindDir),rowMeans(hly0511[,c("ClosestWindDir", "Closest2ndWindDir",
                                                                           "Closest3rdWindDir","Closest4thWindDir",
                                                                           "Closest5thWindDir")], na.rm=TRUE),
                            hly0511$OrigWindDir)

hly0511$OrigWindGustVal<-ifelse(is.na(hly0511$OrigWindGustVal),rowMeans(hly0511[,c("ClosestWindGustVal", "Closest2ndWindGustVal",
                                                                                   "Closest3rdWindGustVal","Closest4thWindGustVal",
                                                                                   "Closest5thWindGustVal")], na.rm=TRUE),
                                hly0511$OrigWindGustVal)

hly0511$OrigStnPres<-ifelse(is.na(hly0511$OrigStnPres),rowMeans(hly0511[,c("ClosestStnPres", "Closest2ndStnPres",
                                                                           "Closest3rdStnPres","Closest4thStnPres",
                                                                           "Closest5thStnPres")], na.rm=TRUE),
                            hly0511$OrigStnPres)

saveRDS(hly0511, file = "hly0511Test.rds") # Saving externally