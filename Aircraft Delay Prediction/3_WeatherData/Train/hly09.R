rm(list=ls())

library(lubridate)
library(sqldf)
library(DMwR)

hly200409<- read.table("200409hourly.txt", sep=",",header = T, dec = ".") # Read 01 File
str(hly200409)

# Removing units from visibility & changing to numeric
hly200409$Visibility<-as.character(hly200409$Visibility)
hly200409$Visibility <- gsub("SM","",hly200409$Visibility,fixed = TRUE)
hly200409$Visibility<-as.numeric(hly200409$Visibility)


# Splitting Sky Condition & Deriving the lowest ceiling height with Broken or Overcast
hly200409$SkyConditions<-as.character(hly200409$SkyConditions)
library(data.table)
setDT(hly200409)[, paste0("SC", 1:3) := tstrsplit(SkyConditions, " ")]
library(stringr)
hly200409$SC4<-ifelse(str_sub(hly200409$SC1,1,3)%in%c('BKN','OVC') | str_sub(hly200409$SC1,1,2) == 'VV',
                      as.integer(str_sub(hly200409$SC1,-2-1))*100,12000)
hly200409$SC5<-ifelse(str_sub(hly200409$SC2,1,3)%in%c('BKN','OVC') | str_sub(hly200409$SC2,1,2) == 'VV',
                      as.integer(str_sub(hly200409$SC2,-2-1))*100,12000)
hly200409$SC6<-ifelse(str_sub(hly200409$SC3,1,3)%in%c('BKN','OVC') | str_sub(hly200409$SC3,1,2) == 'VV',
                      as.integer(str_sub(hly200409$SC3,-2-1))*100,12000)

hly200409$SC4<-ifelse(is.na(hly200409$SC4),12000,hly200409$SC4)
hly200409$SC5<-ifelse(is.na(hly200409$SC5),12000,hly200409$SC5)
hly200409$SC6<-ifelse(is.na(hly200409$SC6),12000,hly200409$SC6)

str(hly200409)

hly200409$SC7<-apply(hly200409[,16:18], 1, min)

hly200409$SkyConditions<-hly200409$SC7
hly200409$SC1<-NULL
hly200409$SC2<-NULL
hly200409$SC3<-NULL
hly200409$SC4<-NULL
hly200409$SC5<-NULL
hly200409$SC6<-NULL
hly200409$SC7<-NULL

# Changing Format as applicable

hly200409$YearMonthDay<-ymd(hly200409$YearMonthDay)
hly200409$YearMonthDay<-as.factor(hly200409$YearMonthDay)
hly200409$WeatherStationID<-as.factor(hly200409$WeatherStationID)

# Deriving time slots in weather data

hly200409$TimeSlot<-ifelse(hly200409$Time<200,'Midnight to 2AM',ifelse(hly200409$Time<400,'2AM to 4AM',
                                                                       ifelse(hly200409$Time<600,'4AM to 6AM',
                                                                              ifelse(hly200409$Time<800,'6AM to 8AM',
                                                                                     ifelse(hly200409$Time<1000,'8AM to 10AM',
                                                                                            ifelse(hly200409$Time<1200,'10AM to Noon',
                                                                                                   ifelse(hly200409$Time<1400,'Noon to 2PM',
                                                                                                          ifelse(hly200409$Time<1600,'2PM to 4PM',
                                                                                                                 ifelse(hly200409$Time<1800,'4PM to 6PM',
                                                                                                                        ifelse(hly200409$Time<2000,'6PM to 8PM',
                                                                                                                               ifelse(hly200409$Time<2200,'8PM to 10PM','10PM to Midnight')))))))))))

hly200409$Time<-NULL # Dropping time column

# Aggregating Hourly Precipitation by Station, Date & Slot

hly200409<-sqldf('select distinct a.WeatherStationID, a.YearMonthDay, a.TimeSlot, avg(a.SkyConditions) as AvgSkyCond,
                 avg(a.Visibility) as AvgVis, avg(a.DBT) as AvgDBT, avg(a.DewPointTemp) as AvgDewPtTemp,
                 avg(a.RelativeHumidityPercent) as AvgRelHumPerc, avg(a.WindSpeed) as AvgWindSp,
                 avg(a.WindDirection) as AvgWindDir, avg(a.WindGustValue) as AvgWindGustVal,
                 avg(a.StationPressure) as AvgStnPres from hly200409 a group by
                 a.WeatherStationID, a.YearMonthDay, a.TimeSlot')

# Merging with close station data

closestation <- readRDS("closestation.rds")

hly0409<-merge(hly200409,closestation,by.x="WeatherStationID",by.y="WeatherStationID")

# Creating Keys for future merging

hly0409$Key0<-paste(hly0409$WeatherStationID,hly0409$YearMonthDay,hly0409$TimeSlot)
hly0409$Key1<-paste(hly0409$ClosestWS,hly0409$YearMonthDay,hly0409$TimeSlot)
hly0409$Key2<-paste(hly0409$Closest2ndWS,hly0409$YearMonthDay,hly0409$TimeSlot)
hly0409$Key3<-paste(hly0409$Closest3rdWS,hly0409$YearMonthDay,hly0409$TimeSlot)
hly0409$Key4<-paste(hly0409$Closest4thWS,hly0409$YearMonthDay,hly0409$TimeSlot)
hly0409$Key5<-paste(hly0409$Closest5thWS,hly0409$YearMonthDay,hly0409$TimeSlot)

# Merging with Closest Weather Stations

rm(hly200409) # Free up memory
temp<-hly0409

names(hly0409)[names(hly0409) == "AvgSkyCond"] = "OrigSkyCond"
names(hly0409)[names(hly0409) == "AvgVis"] = "OrigVis"
names(hly0409)[names(hly0409) == "AvgDBT"] = "OrigDBT"
names(hly0409)[names(hly0409) == "AvgDewPtTemp"] = "OrigDewPtTemp"
names(hly0409)[names(hly0409) == "AvgRelHumPerc"] = "OrigRelHumPerc"
names(hly0409)[names(hly0409) == "AvgWindSp"] = "OrigWindSp"
names(hly0409)[names(hly0409) == "AvgWindDir"] = "OrigWindDir"
names(hly0409)[names(hly0409) == "AvgWindGustVal"] = "OrigWindGustVal"
names(hly0409)[names(hly0409) == "AvgStnPres"] = "OrigStnPres"

hly0409<-sqldf('select a.*, b.AvgSkyCond, b.AvgVis, b.AvgDBT, b.AvgDewPtTemp, b.AvgRelHumPerc,
               b.AvgWindSp, b.AvgWindDir, b.AvgWindGustVal, b.AvgStnPres from hly0409 a left join
               (select Key0, AvgSkyCond, AvgVis, AvgDBT, AvgDewPtTemp, AvgRelHumPerc, AvgWindSp,
               AvgWindDir, AvgWindGustVal, AvgStnPres from temp) b on a.Key1 = b.Key0')

names(hly0409)[names(hly0409) == "AvgSkyCond"] = "ClosestSkyCond"
names(hly0409)[names(hly0409) == "AvgVis"] = "ClosestVis"
names(hly0409)[names(hly0409) == "AvgDBT"] = "ClosestDBT"
names(hly0409)[names(hly0409) == "AvgDewPtTemp"] = "ClosestDewPtTemp"
names(hly0409)[names(hly0409) == "AvgRelHumPerc"] = "ClosestRelHumPerc"
names(hly0409)[names(hly0409) == "AvgWindSp"] = "ClosestWindSp"
names(hly0409)[names(hly0409) == "AvgWindDir"] = "ClosestWindDir"
names(hly0409)[names(hly0409) == "AvgWindGustVal"] = "ClosestWindGustVal"
names(hly0409)[names(hly0409) == "AvgStnPres"] = "ClosestStnPres"

gc()

hly0409<-sqldf('select a.*, b.AvgSkyCond, b.AvgVis, b.AvgDBT, b.AvgDewPtTemp, b.AvgRelHumPerc,
               b.AvgWindSp, b.AvgWindDir, b.AvgWindGustVal, b.AvgStnPres from hly0409 a left join
               (select Key0, AvgSkyCond, AvgVis, AvgDBT, AvgDewPtTemp, AvgRelHumPerc, AvgWindSp,
               AvgWindDir, AvgWindGustVal, AvgStnPres from temp) b on a.Key2 = b.Key0')

names(hly0409)[names(hly0409) == "AvgSkyCond"] = "Closest2ndSkyCond"
names(hly0409)[names(hly0409) == "AvgVis"] = "Closest2ndVis"
names(hly0409)[names(hly0409) == "AvgDBT"] = "Closest2ndDBT"
names(hly0409)[names(hly0409) == "AvgDewPtTemp"] = "Closest2ndDewPtTemp"
names(hly0409)[names(hly0409) == "AvgRelHumPerc"] = "Closest2ndRelHumPerc"
names(hly0409)[names(hly0409) == "AvgWindSp"] = "Closest2ndWindSp"
names(hly0409)[names(hly0409) == "AvgWindDir"] = "Closest2ndWindDir"
names(hly0409)[names(hly0409) == "AvgWindGustVal"] = "Closest2ndWindGustVal"
names(hly0409)[names(hly0409) == "AvgStnPres"] = "Closest2ndStnPres"

gc()

hly0409<-sqldf('select a.*, b.AvgSkyCond, b.AvgVis, b.AvgDBT, b.AvgDewPtTemp, b.AvgRelHumPerc,
               b.AvgWindSp, b.AvgWindDir, b.AvgWindGustVal, b.AvgStnPres from hly0409 a left join
               (select Key0, AvgSkyCond, AvgVis, AvgDBT, AvgDewPtTemp, AvgRelHumPerc, AvgWindSp,
               AvgWindDir, AvgWindGustVal, AvgStnPres from temp) b on a.Key3 = b.Key0')

names(hly0409)[names(hly0409) == "AvgSkyCond"] = "Closest3rdSkyCond"
names(hly0409)[names(hly0409) == "AvgVis"] = "Closest3rdVis"
names(hly0409)[names(hly0409) == "AvgDBT"] = "Closest3rdDBT"
names(hly0409)[names(hly0409) == "AvgDewPtTemp"] = "Closest3rdDewPtTemp"
names(hly0409)[names(hly0409) == "AvgRelHumPerc"] = "Closest3rdRelHumPerc"
names(hly0409)[names(hly0409) == "AvgWindSp"] = "Closest3rdWindSp"
names(hly0409)[names(hly0409) == "AvgWindDir"] = "Closest3rdWindDir"
names(hly0409)[names(hly0409) == "AvgWindGustVal"] = "Closest3rdWindGustVal"
names(hly0409)[names(hly0409) == "AvgStnPres"] = "Closest3rdStnPres"

gc()

hly0409<-sqldf('select a.*, b.AvgSkyCond, b.AvgVis, b.AvgDBT, b.AvgDewPtTemp, b.AvgRelHumPerc,
               b.AvgWindSp, b.AvgWindDir, b.AvgWindGustVal, b.AvgStnPres from hly0409 a left join
               (select Key0, AvgSkyCond, AvgVis, AvgDBT, AvgDewPtTemp, AvgRelHumPerc, AvgWindSp,
               AvgWindDir, AvgWindGustVal, AvgStnPres from temp) b on a.Key4 = b.Key0')

names(hly0409)[names(hly0409) == "AvgSkyCond"] = "Closest4thSkyCond"
names(hly0409)[names(hly0409) == "AvgVis"] = "Closest4thVis"
names(hly0409)[names(hly0409) == "AvgDBT"] = "Closest4thDBT"
names(hly0409)[names(hly0409) == "AvgDewPtTemp"] = "Closest4thDewPtTemp"
names(hly0409)[names(hly0409) == "AvgRelHumPerc"] = "Closest4thRelHumPerc"
names(hly0409)[names(hly0409) == "AvgWindSp"] = "Closest4thWindSp"
names(hly0409)[names(hly0409) == "AvgWindDir"] = "Closest4thWindDir"
names(hly0409)[names(hly0409) == "AvgWindGustVal"] = "Closest4thWindGustVal"
names(hly0409)[names(hly0409) == "AvgStnPres"] = "Closest4thStnPres"

gc()

hly0409<-sqldf('select a.*, b.AvgSkyCond, b.AvgVis, b.AvgDBT, b.AvgDewPtTemp, b.AvgRelHumPerc,
               b.AvgWindSp, b.AvgWindDir, b.AvgWindGustVal, b.AvgStnPres from hly0409 a left join
               (select Key0, AvgSkyCond, AvgVis, AvgDBT, AvgDewPtTemp, AvgRelHumPerc, AvgWindSp,
               AvgWindDir, AvgWindGustVal, AvgStnPres from temp) b on a.Key5 = b.Key0')

names(hly0409)[names(hly0409) == "AvgSkyCond"] = "Closest5thSkyCond"
names(hly0409)[names(hly0409) == "AvgVis"] = "Closest5thVis"
names(hly0409)[names(hly0409) == "AvgDBT"] = "Closest5thDBT"
names(hly0409)[names(hly0409) == "AvgDewPtTemp"] = "Closest5thDewPtTemp"
names(hly0409)[names(hly0409) == "AvgRelHumPerc"] = "Closest5thRelHumPerc"
names(hly0409)[names(hly0409) == "AvgWindSp"] = "Closest5thWindSp"
names(hly0409)[names(hly0409) == "AvgWindDir"] = "Closest5thWindDir"
names(hly0409)[names(hly0409) == "AvgWindGustVal"] = "Closest5thWindGustVal"
names(hly0409)[names(hly0409) == "AvgStnPres"] = "Closest5thStnPres"

gc()

# Check for null values & impute using closest neighbours

rm(temp) # Remove temp file
rm(closestation) # Remove unrequired file

colSums(is.na(hly0409)) # NA values in OrigPrecip column

hly0409$OrigSkyCond<-ifelse(is.na(hly0409$OrigSkyCond),rowMeans(hly0409[,c("ClosestSkyCond", "Closest2ndSkyCond",
                                                                           "Closest3rdSkyCond","Closest4thSkyCond",
                                                                           "Closest5thSkyCond")], na.rm=TRUE),
                            hly0409$OrigSkyCond)

hly0409$OrigVis<-ifelse(is.na(hly0409$OrigVis),rowMeans(hly0409[,c("ClosestVis", "Closest2ndVis",
                                                                   "Closest3rdVis","Closest4thVis",
                                                                   "Closest5thVis")], na.rm=TRUE),
                        hly0409$OrigVis)

hly0409$OrigDBT<-ifelse(is.na(hly0409$OrigDBT),rowMeans(hly0409[,c("ClosestDBT", "Closest2ndDBT",
                                                                   "Closest3rdDBT","Closest4thDBT",
                                                                   "Closest5thDBT")], na.rm=TRUE),
                        hly0409$OrigDBT)

hly0409$OrigDewPtTemp<-ifelse(is.na(hly0409$OrigDewPtTemp),rowMeans(hly0409[,c("ClosestDewPtTemp", "Closest2ndDewPtTemp",
                                                                               "Closest3rdDewPtTemp","Closest4thDewPtTemp",
                                                                               "Closest5thDewPtTemp")], na.rm=TRUE),
                              hly0409$OrigDewPtTemp)

hly0409$OrigRelHumPerc<-ifelse(is.na(hly0409$OrigRelHumPerc),rowMeans(hly0409[,c("ClosestRelHumPerc", "Closest2ndRelHumPerc",
                                                                                 "Closest3rdRelHumPerc","Closest4thRelHumPerc",
                                                                                 "Closest5thRelHumPerc")], na.rm=TRUE),
                               hly0409$OrigRelHumPerc)

hly0409$OrigWindSp<-ifelse(is.na(hly0409$OrigWindSp),rowMeans(hly0409[,c("ClosestWindSp", "Closest2ndWindSp",
                                                                         "Closest3rdWindSp","Closest4thWindSp",
                                                                         "Closest5thWindSp")], na.rm=TRUE),
                           hly0409$OrigWindSp)

hly0409$OrigWindDir<-ifelse(is.na(hly0409$OrigWindDir),rowMeans(hly0409[,c("ClosestWindDir", "Closest2ndWindDir",
                                                                           "Closest3rdWindDir","Closest4thWindDir",
                                                                           "Closest5thWindDir")], na.rm=TRUE),
                            hly0409$OrigWindDir)

hly0409$OrigWindGustVal<-ifelse(is.na(hly0409$OrigWindGustVal),rowMeans(hly0409[,c("ClosestWindGustVal", "Closest2ndWindGustVal",
                                                                                   "Closest3rdWindGustVal","Closest4thWindGustVal",
                                                                                   "Closest5thWindGustVal")], na.rm=TRUE),
                                hly0409$OrigWindGustVal)

hly0409$OrigStnPres<-ifelse(is.na(hly0409$OrigStnPres),rowMeans(hly0409[,c("ClosestStnPres", "Closest2ndStnPres",
                                                                           "Closest3rdStnPres","Closest4thStnPres",
                                                                           "Closest5thStnPres")], na.rm=TRUE),
                            hly0409$OrigStnPres)

saveRDS(hly0409, file = "hly0409.rds") # Saving externally