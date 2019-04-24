rm(list=ls())

library(lubridate)
library(sqldf)
library(DMwR)

hly200407<- read.table("200407hourly.txt", sep=",",header = T, dec = ".") # Read 01 File
str(hly200407)

# Removing units from visibility & changing to numeric
hly200407$Visibility<-as.character(hly200407$Visibility)
hly200407$Visibility <- gsub("SM","",hly200407$Visibility,fixed = TRUE)
hly200407$Visibility<-as.numeric(hly200407$Visibility)


# Splitting Sky Condition & Deriving the lowest ceiling height with Broken or Overcast
hly200407$SkyConditions<-as.character(hly200407$SkyConditions)
library(data.table)
setDT(hly200407)[, paste0("SC", 1:3) := tstrsplit(SkyConditions, " ")]
library(stringr)
hly200407$SC4<-ifelse(str_sub(hly200407$SC1,1,3)%in%c('BKN','OVC') | str_sub(hly200407$SC1,1,2) == 'VV',
                      as.integer(str_sub(hly200407$SC1,-2-1))*100,12000)
hly200407$SC5<-ifelse(str_sub(hly200407$SC2,1,3)%in%c('BKN','OVC') | str_sub(hly200407$SC2,1,2) == 'VV',
                      as.integer(str_sub(hly200407$SC2,-2-1))*100,12000)
hly200407$SC6<-ifelse(str_sub(hly200407$SC3,1,3)%in%c('BKN','OVC') | str_sub(hly200407$SC3,1,2) == 'VV',
                      as.integer(str_sub(hly200407$SC3,-2-1))*100,12000)

hly200407$SC4<-ifelse(is.na(hly200407$SC4),12000,hly200407$SC4)
hly200407$SC5<-ifelse(is.na(hly200407$SC5),12000,hly200407$SC5)
hly200407$SC6<-ifelse(is.na(hly200407$SC6),12000,hly200407$SC6)

str(hly200407)

hly200407$SC7<-apply(hly200407[,16:18], 1, min)

hly200407$SkyConditions<-hly200407$SC7
hly200407$SC1<-NULL
hly200407$SC2<-NULL
hly200407$SC3<-NULL
hly200407$SC4<-NULL
hly200407$SC5<-NULL
hly200407$SC6<-NULL
hly200407$SC7<-NULL

# Changing Format as applicable

hly200407$YearMonthDay<-ymd(hly200407$YearMonthDay)
hly200407$YearMonthDay<-as.factor(hly200407$YearMonthDay)
hly200407$WeatherStationID<-as.factor(hly200407$WeatherStationID)

# Deriving time slots in weather data

hly200407$TimeSlot<-ifelse(hly200407$Time<200,'Midnight to 2AM',ifelse(hly200407$Time<400,'2AM to 4AM',
                                                                       ifelse(hly200407$Time<600,'4AM to 6AM',
                                                                              ifelse(hly200407$Time<800,'6AM to 8AM',
                                                                                     ifelse(hly200407$Time<1000,'8AM to 10AM',
                                                                                            ifelse(hly200407$Time<1200,'10AM to Noon',
                                                                                                   ifelse(hly200407$Time<1400,'Noon to 2PM',
                                                                                                          ifelse(hly200407$Time<1600,'2PM to 4PM',
                                                                                                                 ifelse(hly200407$Time<1800,'4PM to 6PM',
                                                                                                                        ifelse(hly200407$Time<2000,'6PM to 8PM',
                                                                                                                               ifelse(hly200407$Time<2200,'8PM to 10PM','10PM to Midnight')))))))))))

hly200407$Time<-NULL # Dropping time column

# Aggregating Hourly Precipitation by Station, Date & Slot

hly200407<-sqldf('select distinct a.WeatherStationID, a.YearMonthDay, a.TimeSlot, avg(a.SkyConditions) as AvgSkyCond,
                 avg(a.Visibility) as AvgVis, avg(a.DBT) as AvgDBT, avg(a.DewPointTemp) as AvgDewPtTemp,
                 avg(a.RelativeHumidityPercent) as AvgRelHumPerc, avg(a.WindSpeed) as AvgWindSp,
                 avg(a.WindDirection) as AvgWindDir, avg(a.WindGustValue) as AvgWindGustVal,
                 avg(a.StationPressure) as AvgStnPres from hly200407 a group by
                 a.WeatherStationID, a.YearMonthDay, a.TimeSlot')

# Merging with close station data

closestation <- readRDS("closestation.rds")

hly0407<-merge(hly200407,closestation,by.x="WeatherStationID",by.y="WeatherStationID")

# Creating Keys for future merging

hly0407$Key0<-paste(hly0407$WeatherStationID,hly0407$YearMonthDay,hly0407$TimeSlot)
hly0407$Key1<-paste(hly0407$ClosestWS,hly0407$YearMonthDay,hly0407$TimeSlot)
hly0407$Key2<-paste(hly0407$Closest2ndWS,hly0407$YearMonthDay,hly0407$TimeSlot)
hly0407$Key3<-paste(hly0407$Closest3rdWS,hly0407$YearMonthDay,hly0407$TimeSlot)
hly0407$Key4<-paste(hly0407$Closest4thWS,hly0407$YearMonthDay,hly0407$TimeSlot)
hly0407$Key5<-paste(hly0407$Closest5thWS,hly0407$YearMonthDay,hly0407$TimeSlot)

# Merging with Closest Weather Stations

rm(hly200407) # Free up memory
temp<-hly0407

names(hly0407)[names(hly0407) == "AvgSkyCond"] = "OrigSkyCond"
names(hly0407)[names(hly0407) == "AvgVis"] = "OrigVis"
names(hly0407)[names(hly0407) == "AvgDBT"] = "OrigDBT"
names(hly0407)[names(hly0407) == "AvgDewPtTemp"] = "OrigDewPtTemp"
names(hly0407)[names(hly0407) == "AvgRelHumPerc"] = "OrigRelHumPerc"
names(hly0407)[names(hly0407) == "AvgWindSp"] = "OrigWindSp"
names(hly0407)[names(hly0407) == "AvgWindDir"] = "OrigWindDir"
names(hly0407)[names(hly0407) == "AvgWindGustVal"] = "OrigWindGustVal"
names(hly0407)[names(hly0407) == "AvgStnPres"] = "OrigStnPres"

hly0407<-sqldf('select a.*, b.AvgSkyCond, b.AvgVis, b.AvgDBT, b.AvgDewPtTemp, b.AvgRelHumPerc,
               b.AvgWindSp, b.AvgWindDir, b.AvgWindGustVal, b.AvgStnPres from hly0407 a left join
               (select Key0, AvgSkyCond, AvgVis, AvgDBT, AvgDewPtTemp, AvgRelHumPerc, AvgWindSp,
               AvgWindDir, AvgWindGustVal, AvgStnPres from temp) b on a.Key1 = b.Key0')

names(hly0407)[names(hly0407) == "AvgSkyCond"] = "ClosestSkyCond"
names(hly0407)[names(hly0407) == "AvgVis"] = "ClosestVis"
names(hly0407)[names(hly0407) == "AvgDBT"] = "ClosestDBT"
names(hly0407)[names(hly0407) == "AvgDewPtTemp"] = "ClosestDewPtTemp"
names(hly0407)[names(hly0407) == "AvgRelHumPerc"] = "ClosestRelHumPerc"
names(hly0407)[names(hly0407) == "AvgWindSp"] = "ClosestWindSp"
names(hly0407)[names(hly0407) == "AvgWindDir"] = "ClosestWindDir"
names(hly0407)[names(hly0407) == "AvgWindGustVal"] = "ClosestWindGustVal"
names(hly0407)[names(hly0407) == "AvgStnPres"] = "ClosestStnPres"

gc()

hly0407<-sqldf('select a.*, b.AvgSkyCond, b.AvgVis, b.AvgDBT, b.AvgDewPtTemp, b.AvgRelHumPerc,
               b.AvgWindSp, b.AvgWindDir, b.AvgWindGustVal, b.AvgStnPres from hly0407 a left join
               (select Key0, AvgSkyCond, AvgVis, AvgDBT, AvgDewPtTemp, AvgRelHumPerc, AvgWindSp,
               AvgWindDir, AvgWindGustVal, AvgStnPres from temp) b on a.Key2 = b.Key0')

names(hly0407)[names(hly0407) == "AvgSkyCond"] = "Closest2ndSkyCond"
names(hly0407)[names(hly0407) == "AvgVis"] = "Closest2ndVis"
names(hly0407)[names(hly0407) == "AvgDBT"] = "Closest2ndDBT"
names(hly0407)[names(hly0407) == "AvgDewPtTemp"] = "Closest2ndDewPtTemp"
names(hly0407)[names(hly0407) == "AvgRelHumPerc"] = "Closest2ndRelHumPerc"
names(hly0407)[names(hly0407) == "AvgWindSp"] = "Closest2ndWindSp"
names(hly0407)[names(hly0407) == "AvgWindDir"] = "Closest2ndWindDir"
names(hly0407)[names(hly0407) == "AvgWindGustVal"] = "Closest2ndWindGustVal"
names(hly0407)[names(hly0407) == "AvgStnPres"] = "Closest2ndStnPres"

gc()

hly0407<-sqldf('select a.*, b.AvgSkyCond, b.AvgVis, b.AvgDBT, b.AvgDewPtTemp, b.AvgRelHumPerc,
               b.AvgWindSp, b.AvgWindDir, b.AvgWindGustVal, b.AvgStnPres from hly0407 a left join
               (select Key0, AvgSkyCond, AvgVis, AvgDBT, AvgDewPtTemp, AvgRelHumPerc, AvgWindSp,
               AvgWindDir, AvgWindGustVal, AvgStnPres from temp) b on a.Key3 = b.Key0')

names(hly0407)[names(hly0407) == "AvgSkyCond"] = "Closest3rdSkyCond"
names(hly0407)[names(hly0407) == "AvgVis"] = "Closest3rdVis"
names(hly0407)[names(hly0407) == "AvgDBT"] = "Closest3rdDBT"
names(hly0407)[names(hly0407) == "AvgDewPtTemp"] = "Closest3rdDewPtTemp"
names(hly0407)[names(hly0407) == "AvgRelHumPerc"] = "Closest3rdRelHumPerc"
names(hly0407)[names(hly0407) == "AvgWindSp"] = "Closest3rdWindSp"
names(hly0407)[names(hly0407) == "AvgWindDir"] = "Closest3rdWindDir"
names(hly0407)[names(hly0407) == "AvgWindGustVal"] = "Closest3rdWindGustVal"
names(hly0407)[names(hly0407) == "AvgStnPres"] = "Closest3rdStnPres"

gc()

hly0407<-sqldf('select a.*, b.AvgSkyCond, b.AvgVis, b.AvgDBT, b.AvgDewPtTemp, b.AvgRelHumPerc,
               b.AvgWindSp, b.AvgWindDir, b.AvgWindGustVal, b.AvgStnPres from hly0407 a left join
               (select Key0, AvgSkyCond, AvgVis, AvgDBT, AvgDewPtTemp, AvgRelHumPerc, AvgWindSp,
               AvgWindDir, AvgWindGustVal, AvgStnPres from temp) b on a.Key4 = b.Key0')

names(hly0407)[names(hly0407) == "AvgSkyCond"] = "Closest4thSkyCond"
names(hly0407)[names(hly0407) == "AvgVis"] = "Closest4thVis"
names(hly0407)[names(hly0407) == "AvgDBT"] = "Closest4thDBT"
names(hly0407)[names(hly0407) == "AvgDewPtTemp"] = "Closest4thDewPtTemp"
names(hly0407)[names(hly0407) == "AvgRelHumPerc"] = "Closest4thRelHumPerc"
names(hly0407)[names(hly0407) == "AvgWindSp"] = "Closest4thWindSp"
names(hly0407)[names(hly0407) == "AvgWindDir"] = "Closest4thWindDir"
names(hly0407)[names(hly0407) == "AvgWindGustVal"] = "Closest4thWindGustVal"
names(hly0407)[names(hly0407) == "AvgStnPres"] = "Closest4thStnPres"

gc()

hly0407<-sqldf('select a.*, b.AvgSkyCond, b.AvgVis, b.AvgDBT, b.AvgDewPtTemp, b.AvgRelHumPerc,
               b.AvgWindSp, b.AvgWindDir, b.AvgWindGustVal, b.AvgStnPres from hly0407 a left join
               (select Key0, AvgSkyCond, AvgVis, AvgDBT, AvgDewPtTemp, AvgRelHumPerc, AvgWindSp,
               AvgWindDir, AvgWindGustVal, AvgStnPres from temp) b on a.Key5 = b.Key0')

names(hly0407)[names(hly0407) == "AvgSkyCond"] = "Closest5thSkyCond"
names(hly0407)[names(hly0407) == "AvgVis"] = "Closest5thVis"
names(hly0407)[names(hly0407) == "AvgDBT"] = "Closest5thDBT"
names(hly0407)[names(hly0407) == "AvgDewPtTemp"] = "Closest5thDewPtTemp"
names(hly0407)[names(hly0407) == "AvgRelHumPerc"] = "Closest5thRelHumPerc"
names(hly0407)[names(hly0407) == "AvgWindSp"] = "Closest5thWindSp"
names(hly0407)[names(hly0407) == "AvgWindDir"] = "Closest5thWindDir"
names(hly0407)[names(hly0407) == "AvgWindGustVal"] = "Closest5thWindGustVal"
names(hly0407)[names(hly0407) == "AvgStnPres"] = "Closest5thStnPres"

gc()

# Check for null values & impute using closest neighbours

rm(temp) # Remove temp file
rm(closestation) # Remove unrequired file

colSums(is.na(hly0407)) # NA values in OrigPrecip column

hly0407$OrigSkyCond<-ifelse(is.na(hly0407$OrigSkyCond),rowMeans(hly0407[,c("ClosestSkyCond", "Closest2ndSkyCond",
                                                                           "Closest3rdSkyCond","Closest4thSkyCond",
                                                                           "Closest5thSkyCond")], na.rm=TRUE),
                            hly0407$OrigSkyCond)

hly0407$OrigVis<-ifelse(is.na(hly0407$OrigVis),rowMeans(hly0407[,c("ClosestVis", "Closest2ndVis",
                                                                   "Closest3rdVis","Closest4thVis",
                                                                   "Closest5thVis")], na.rm=TRUE),
                        hly0407$OrigVis)

hly0407$OrigDBT<-ifelse(is.na(hly0407$OrigDBT),rowMeans(hly0407[,c("ClosestDBT", "Closest2ndDBT",
                                                                   "Closest3rdDBT","Closest4thDBT",
                                                                   "Closest5thDBT")], na.rm=TRUE),
                        hly0407$OrigDBT)

hly0407$OrigDewPtTemp<-ifelse(is.na(hly0407$OrigDewPtTemp),rowMeans(hly0407[,c("ClosestDewPtTemp", "Closest2ndDewPtTemp",
                                                                               "Closest3rdDewPtTemp","Closest4thDewPtTemp",
                                                                               "Closest5thDewPtTemp")], na.rm=TRUE),
                              hly0407$OrigDewPtTemp)

hly0407$OrigRelHumPerc<-ifelse(is.na(hly0407$OrigRelHumPerc),rowMeans(hly0407[,c("ClosestRelHumPerc", "Closest2ndRelHumPerc",
                                                                                 "Closest3rdRelHumPerc","Closest4thRelHumPerc",
                                                                                 "Closest5thRelHumPerc")], na.rm=TRUE),
                               hly0407$OrigRelHumPerc)

hly0407$OrigWindSp<-ifelse(is.na(hly0407$OrigWindSp),rowMeans(hly0407[,c("ClosestWindSp", "Closest2ndWindSp",
                                                                         "Closest3rdWindSp","Closest4thWindSp",
                                                                         "Closest5thWindSp")], na.rm=TRUE),
                           hly0407$OrigWindSp)

hly0407$OrigWindDir<-ifelse(is.na(hly0407$OrigWindDir),rowMeans(hly0407[,c("ClosestWindDir", "Closest2ndWindDir",
                                                                           "Closest3rdWindDir","Closest4thWindDir",
                                                                           "Closest5thWindDir")], na.rm=TRUE),
                            hly0407$OrigWindDir)

hly0407$OrigWindGustVal<-ifelse(is.na(hly0407$OrigWindGustVal),rowMeans(hly0407[,c("ClosestWindGustVal", "Closest2ndWindGustVal",
                                                                                   "Closest3rdWindGustVal","Closest4thWindGustVal",
                                                                                   "Closest5thWindGustVal")], na.rm=TRUE),
                                hly0407$OrigWindGustVal)

hly0407$OrigStnPres<-ifelse(is.na(hly0407$OrigStnPres),rowMeans(hly0407[,c("ClosestStnPres", "Closest2ndStnPres",
                                                                           "Closest3rdStnPres","Closest4thStnPres",
                                                                           "Closest5thStnPres")], na.rm=TRUE),
                            hly0407$OrigStnPres)

saveRDS(hly0407, file = "hly0407.rds") # Saving externally