rm(list=ls())

library(lubridate)
library(sqldf)
library(DMwR)

hly200403<- read.table("200403hourly.txt", sep=",",header = T, dec = ".") # Read 01 File
str(hly200403)

# Removing units from visibility & changing to numeric
hly200403$Visibility<-as.character(hly200403$Visibility)
hly200403$Visibility <- gsub("SM","",hly200403$Visibility,fixed = TRUE)
hly200403$Visibility<-as.numeric(hly200403$Visibility)


# Splitting Sky Condition & Deriving the lowest ceiling height with Broken or Overcast
hly200403$SkyConditions<-as.character(hly200403$SkyConditions)
library(data.table)
setDT(hly200403)[, paste0("SC", 1:3) := tstrsplit(SkyConditions, " ")]
library(stringr)
hly200403$SC4<-ifelse(str_sub(hly200403$SC1,1,3)%in%c('BKN','OVC') | str_sub(hly200403$SC1,1,2) == 'VV',
                      as.integer(str_sub(hly200403$SC1,-2-1))*100,12000)
hly200403$SC5<-ifelse(str_sub(hly200403$SC2,1,3)%in%c('BKN','OVC') | str_sub(hly200403$SC2,1,2) == 'VV',
                      as.integer(str_sub(hly200403$SC2,-2-1))*100,12000)
hly200403$SC6<-ifelse(str_sub(hly200403$SC3,1,3)%in%c('BKN','OVC') | str_sub(hly200403$SC3,1,2) == 'VV',
                      as.integer(str_sub(hly200403$SC3,-2-1))*100,12000)

hly200403$SC4<-ifelse(is.na(hly200403$SC4),12000,hly200403$SC4)
hly200403$SC5<-ifelse(is.na(hly200403$SC5),12000,hly200403$SC5)
hly200403$SC6<-ifelse(is.na(hly200403$SC6),12000,hly200403$SC6)

str(hly200403)

hly200403$SC7<-apply(hly200403[,16:18], 1, min)

hly200403$SkyConditions<-hly200403$SC7
hly200403$SC1<-NULL
hly200403$SC2<-NULL
hly200403$SC3<-NULL
hly200403$SC4<-NULL
hly200403$SC5<-NULL
hly200403$SC6<-NULL
hly200403$SC7<-NULL

# Changing Format as applicable

hly200403$YearMonthDay<-ymd(hly200403$YearMonthDay)
hly200403$YearMonthDay<-as.factor(hly200403$YearMonthDay)
hly200403$WeatherStationID<-as.factor(hly200403$WeatherStationID)

# Deriving time slots in weather data

hly200403$TimeSlot<-ifelse(hly200403$Time<200,'Midnight to 2AM',ifelse(hly200403$Time<400,'2AM to 4AM',
                                                                       ifelse(hly200403$Time<600,'4AM to 6AM',
                                                                              ifelse(hly200403$Time<800,'6AM to 8AM',
                                                                                     ifelse(hly200403$Time<1000,'8AM to 10AM',
                                                                                            ifelse(hly200403$Time<1200,'10AM to Noon',
                                                                                                   ifelse(hly200403$Time<1400,'Noon to 2PM',
                                                                                                          ifelse(hly200403$Time<1600,'2PM to 4PM',
                                                                                                                 ifelse(hly200403$Time<1800,'4PM to 6PM',
                                                                                                                        ifelse(hly200403$Time<2000,'6PM to 8PM',
                                                                                                                               ifelse(hly200403$Time<2200,'8PM to 10PM','10PM to Midnight')))))))))))

hly200403$Time<-NULL # Dropping time column

# Aggregating Hourly Precipitation by Station, Date & Slot

hly200403<-sqldf('select distinct a.WeatherStationID, a.YearMonthDay, a.TimeSlot, avg(a.SkyConditions) as AvgSkyCond,
                 avg(a.Visibility) as AvgVis, avg(a.DBT) as AvgDBT, avg(a.DewPointTemp) as AvgDewPtTemp,
                 avg(a.RelativeHumidityPercent) as AvgRelHumPerc, avg(a.WindSpeed) as AvgWindSp,
                 avg(a.WindDirection) as AvgWindDir, avg(a.WindGustValue) as AvgWindGustVal,
                 avg(a.StationPressure) as AvgStnPres from hly200403 a group by
                 a.WeatherStationID, a.YearMonthDay, a.TimeSlot')

# Merging with close station data

closestation <- readRDS("closestation.rds")

hly0403<-merge(hly200403,closestation,by.x="WeatherStationID",by.y="WeatherStationID")

# Creating Keys for future merging

hly0403$Key0<-paste(hly0403$WeatherStationID,hly0403$YearMonthDay,hly0403$TimeSlot)
hly0403$Key1<-paste(hly0403$ClosestWS,hly0403$YearMonthDay,hly0403$TimeSlot)
hly0403$Key2<-paste(hly0403$Closest2ndWS,hly0403$YearMonthDay,hly0403$TimeSlot)
hly0403$Key3<-paste(hly0403$Closest3rdWS,hly0403$YearMonthDay,hly0403$TimeSlot)
hly0403$Key4<-paste(hly0403$Closest4thWS,hly0403$YearMonthDay,hly0403$TimeSlot)
hly0403$Key5<-paste(hly0403$Closest5thWS,hly0403$YearMonthDay,hly0403$TimeSlot)

# Merging with Closest Weather Stations

rm(hly200403) # Free up memory
temp<-hly0403

names(hly0403)[names(hly0403) == "AvgSkyCond"] = "OrigSkyCond"
names(hly0403)[names(hly0403) == "AvgVis"] = "OrigVis"
names(hly0403)[names(hly0403) == "AvgDBT"] = "OrigDBT"
names(hly0403)[names(hly0403) == "AvgDewPtTemp"] = "OrigDewPtTemp"
names(hly0403)[names(hly0403) == "AvgRelHumPerc"] = "OrigRelHumPerc"
names(hly0403)[names(hly0403) == "AvgWindSp"] = "OrigWindSp"
names(hly0403)[names(hly0403) == "AvgWindDir"] = "OrigWindDir"
names(hly0403)[names(hly0403) == "AvgWindGustVal"] = "OrigWindGustVal"
names(hly0403)[names(hly0403) == "AvgStnPres"] = "OrigStnPres"

hly0403<-sqldf('select a.*, b.AvgSkyCond, b.AvgVis, b.AvgDBT, b.AvgDewPtTemp, b.AvgRelHumPerc,
               b.AvgWindSp, b.AvgWindDir, b.AvgWindGustVal, b.AvgStnPres from hly0403 a left join
               (select Key0, AvgSkyCond, AvgVis, AvgDBT, AvgDewPtTemp, AvgRelHumPerc, AvgWindSp,
               AvgWindDir, AvgWindGustVal, AvgStnPres from temp) b on a.Key1 = b.Key0')

names(hly0403)[names(hly0403) == "AvgSkyCond"] = "ClosestSkyCond"
names(hly0403)[names(hly0403) == "AvgVis"] = "ClosestVis"
names(hly0403)[names(hly0403) == "AvgDBT"] = "ClosestDBT"
names(hly0403)[names(hly0403) == "AvgDewPtTemp"] = "ClosestDewPtTemp"
names(hly0403)[names(hly0403) == "AvgRelHumPerc"] = "ClosestRelHumPerc"
names(hly0403)[names(hly0403) == "AvgWindSp"] = "ClosestWindSp"
names(hly0403)[names(hly0403) == "AvgWindDir"] = "ClosestWindDir"
names(hly0403)[names(hly0403) == "AvgWindGustVal"] = "ClosestWindGustVal"
names(hly0403)[names(hly0403) == "AvgStnPres"] = "ClosestStnPres"

gc()

hly0403<-sqldf('select a.*, b.AvgSkyCond, b.AvgVis, b.AvgDBT, b.AvgDewPtTemp, b.AvgRelHumPerc,
               b.AvgWindSp, b.AvgWindDir, b.AvgWindGustVal, b.AvgStnPres from hly0403 a left join
               (select Key0, AvgSkyCond, AvgVis, AvgDBT, AvgDewPtTemp, AvgRelHumPerc, AvgWindSp,
               AvgWindDir, AvgWindGustVal, AvgStnPres from temp) b on a.Key2 = b.Key0')

names(hly0403)[names(hly0403) == "AvgSkyCond"] = "Closest2ndSkyCond"
names(hly0403)[names(hly0403) == "AvgVis"] = "Closest2ndVis"
names(hly0403)[names(hly0403) == "AvgDBT"] = "Closest2ndDBT"
names(hly0403)[names(hly0403) == "AvgDewPtTemp"] = "Closest2ndDewPtTemp"
names(hly0403)[names(hly0403) == "AvgRelHumPerc"] = "Closest2ndRelHumPerc"
names(hly0403)[names(hly0403) == "AvgWindSp"] = "Closest2ndWindSp"
names(hly0403)[names(hly0403) == "AvgWindDir"] = "Closest2ndWindDir"
names(hly0403)[names(hly0403) == "AvgWindGustVal"] = "Closest2ndWindGustVal"
names(hly0403)[names(hly0403) == "AvgStnPres"] = "Closest2ndStnPres"

gc()

hly0403<-sqldf('select a.*, b.AvgSkyCond, b.AvgVis, b.AvgDBT, b.AvgDewPtTemp, b.AvgRelHumPerc,
               b.AvgWindSp, b.AvgWindDir, b.AvgWindGustVal, b.AvgStnPres from hly0403 a left join
               (select Key0, AvgSkyCond, AvgVis, AvgDBT, AvgDewPtTemp, AvgRelHumPerc, AvgWindSp,
               AvgWindDir, AvgWindGustVal, AvgStnPres from temp) b on a.Key3 = b.Key0')

names(hly0403)[names(hly0403) == "AvgSkyCond"] = "Closest3rdSkyCond"
names(hly0403)[names(hly0403) == "AvgVis"] = "Closest3rdVis"
names(hly0403)[names(hly0403) == "AvgDBT"] = "Closest3rdDBT"
names(hly0403)[names(hly0403) == "AvgDewPtTemp"] = "Closest3rdDewPtTemp"
names(hly0403)[names(hly0403) == "AvgRelHumPerc"] = "Closest3rdRelHumPerc"
names(hly0403)[names(hly0403) == "AvgWindSp"] = "Closest3rdWindSp"
names(hly0403)[names(hly0403) == "AvgWindDir"] = "Closest3rdWindDir"
names(hly0403)[names(hly0403) == "AvgWindGustVal"] = "Closest3rdWindGustVal"
names(hly0403)[names(hly0403) == "AvgStnPres"] = "Closest3rdStnPres"

gc()

hly0403<-sqldf('select a.*, b.AvgSkyCond, b.AvgVis, b.AvgDBT, b.AvgDewPtTemp, b.AvgRelHumPerc,
               b.AvgWindSp, b.AvgWindDir, b.AvgWindGustVal, b.AvgStnPres from hly0403 a left join
               (select Key0, AvgSkyCond, AvgVis, AvgDBT, AvgDewPtTemp, AvgRelHumPerc, AvgWindSp,
               AvgWindDir, AvgWindGustVal, AvgStnPres from temp) b on a.Key4 = b.Key0')

names(hly0403)[names(hly0403) == "AvgSkyCond"] = "Closest4thSkyCond"
names(hly0403)[names(hly0403) == "AvgVis"] = "Closest4thVis"
names(hly0403)[names(hly0403) == "AvgDBT"] = "Closest4thDBT"
names(hly0403)[names(hly0403) == "AvgDewPtTemp"] = "Closest4thDewPtTemp"
names(hly0403)[names(hly0403) == "AvgRelHumPerc"] = "Closest4thRelHumPerc"
names(hly0403)[names(hly0403) == "AvgWindSp"] = "Closest4thWindSp"
names(hly0403)[names(hly0403) == "AvgWindDir"] = "Closest4thWindDir"
names(hly0403)[names(hly0403) == "AvgWindGustVal"] = "Closest4thWindGustVal"
names(hly0403)[names(hly0403) == "AvgStnPres"] = "Closest4thStnPres"

gc()

hly0403<-sqldf('select a.*, b.AvgSkyCond, b.AvgVis, b.AvgDBT, b.AvgDewPtTemp, b.AvgRelHumPerc,
               b.AvgWindSp, b.AvgWindDir, b.AvgWindGustVal, b.AvgStnPres from hly0403 a left join
               (select Key0, AvgSkyCond, AvgVis, AvgDBT, AvgDewPtTemp, AvgRelHumPerc, AvgWindSp,
               AvgWindDir, AvgWindGustVal, AvgStnPres from temp) b on a.Key5 = b.Key0')

names(hly0403)[names(hly0403) == "AvgSkyCond"] = "Closest5thSkyCond"
names(hly0403)[names(hly0403) == "AvgVis"] = "Closest5thVis"
names(hly0403)[names(hly0403) == "AvgDBT"] = "Closest5thDBT"
names(hly0403)[names(hly0403) == "AvgDewPtTemp"] = "Closest5thDewPtTemp"
names(hly0403)[names(hly0403) == "AvgRelHumPerc"] = "Closest5thRelHumPerc"
names(hly0403)[names(hly0403) == "AvgWindSp"] = "Closest5thWindSp"
names(hly0403)[names(hly0403) == "AvgWindDir"] = "Closest5thWindDir"
names(hly0403)[names(hly0403) == "AvgWindGustVal"] = "Closest5thWindGustVal"
names(hly0403)[names(hly0403) == "AvgStnPres"] = "Closest5thStnPres"

gc()

# Check for null values & impute using closest neighbours

rm(temp) # Remove temp file
rm(closestation) # Remove unrequired file

colSums(is.na(hly0403)) # NA values in OrigPrecip column

hly0403$OrigSkyCond<-ifelse(is.na(hly0403$OrigSkyCond),rowMeans(hly0403[,c("ClosestSkyCond", "Closest2ndSkyCond",
                                                                           "Closest3rdSkyCond","Closest4thSkyCond",
                                                                           "Closest5thSkyCond")], na.rm=TRUE),
                            hly0403$OrigSkyCond)

hly0403$OrigVis<-ifelse(is.na(hly0403$OrigVis),rowMeans(hly0403[,c("ClosestVis", "Closest2ndVis",
                                                                   "Closest3rdVis","Closest4thVis",
                                                                   "Closest5thVis")], na.rm=TRUE),
                        hly0403$OrigVis)

hly0403$OrigDBT<-ifelse(is.na(hly0403$OrigDBT),rowMeans(hly0403[,c("ClosestDBT", "Closest2ndDBT",
                                                                   "Closest3rdDBT","Closest4thDBT",
                                                                   "Closest5thDBT")], na.rm=TRUE),
                        hly0403$OrigDBT)

hly0403$OrigDewPtTemp<-ifelse(is.na(hly0403$OrigDewPtTemp),rowMeans(hly0403[,c("ClosestDewPtTemp", "Closest2ndDewPtTemp",
                                                                               "Closest3rdDewPtTemp","Closest4thDewPtTemp",
                                                                               "Closest5thDewPtTemp")], na.rm=TRUE),
                              hly0403$OrigDewPtTemp)

hly0403$OrigRelHumPerc<-ifelse(is.na(hly0403$OrigRelHumPerc),rowMeans(hly0403[,c("ClosestRelHumPerc", "Closest2ndRelHumPerc",
                                                                                 "Closest3rdRelHumPerc","Closest4thRelHumPerc",
                                                                                 "Closest5thRelHumPerc")], na.rm=TRUE),
                               hly0403$OrigRelHumPerc)

hly0403$OrigWindSp<-ifelse(is.na(hly0403$OrigWindSp),rowMeans(hly0403[,c("ClosestWindSp", "Closest2ndWindSp",
                                                                         "Closest3rdWindSp","Closest4thWindSp",
                                                                         "Closest5thWindSp")], na.rm=TRUE),
                           hly0403$OrigWindSp)

hly0403$OrigWindDir<-ifelse(is.na(hly0403$OrigWindDir),rowMeans(hly0403[,c("ClosestWindDir", "Closest2ndWindDir",
                                                                           "Closest3rdWindDir","Closest4thWindDir",
                                                                           "Closest5thWindDir")], na.rm=TRUE),
                            hly0403$OrigWindDir)

hly0403$OrigWindGustVal<-ifelse(is.na(hly0403$OrigWindGustVal),rowMeans(hly0403[,c("ClosestWindGustVal", "Closest2ndWindGustVal",
                                                                                   "Closest3rdWindGustVal","Closest4thWindGustVal",
                                                                                   "Closest5thWindGustVal")], na.rm=TRUE),
                                hly0403$OrigWindGustVal)

hly0403$OrigStnPres<-ifelse(is.na(hly0403$OrigStnPres),rowMeans(hly0403[,c("ClosestStnPres", "Closest2ndStnPres",
                                                                           "Closest3rdStnPres","Closest4thStnPres",
                                                                           "Closest5thStnPres")], na.rm=TRUE),
                            hly0403$OrigStnPres)

saveRDS(hly0403, file = "hly0403.rds") # Saving externally