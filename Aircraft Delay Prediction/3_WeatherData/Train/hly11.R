rm(list=ls())

library(lubridate)
library(sqldf)
library(DMwR)

hly200411<- read.table("200411hourly.txt", sep=",",header = T, dec = ".") # Read 01 File
str(hly200411)

# Removing units from visibility & changing to numeric
hly200411$Visibility<-as.character(hly200411$Visibility)
hly200411$Visibility <- gsub("SM","",hly200411$Visibility,fixed = TRUE)
hly200411$Visibility<-as.numeric(hly200411$Visibility)


# Splitting Sky Condition & Deriving the lowest ceiling height with Broken or Overcast
hly200411$SkyConditions<-as.character(hly200411$SkyConditions)
library(data.table)
setDT(hly200411)[, paste0("SC", 1:3) := tstrsplit(SkyConditions, " ")]
library(stringr)
hly200411$SC4<-ifelse(str_sub(hly200411$SC1,1,3)%in%c('BKN','OVC') | str_sub(hly200411$SC1,1,2) == 'VV',
                      as.integer(str_sub(hly200411$SC1,-2-1))*100,12000)
hly200411$SC5<-ifelse(str_sub(hly200411$SC2,1,3)%in%c('BKN','OVC') | str_sub(hly200411$SC2,1,2) == 'VV',
                      as.integer(str_sub(hly200411$SC2,-2-1))*100,12000)
hly200411$SC6<-ifelse(str_sub(hly200411$SC3,1,3)%in%c('BKN','OVC') | str_sub(hly200411$SC3,1,2) == 'VV',
                      as.integer(str_sub(hly200411$SC3,-2-1))*100,12000)

hly200411$SC4<-ifelse(is.na(hly200411$SC4),12000,hly200411$SC4)
hly200411$SC5<-ifelse(is.na(hly200411$SC5),12000,hly200411$SC5)
hly200411$SC6<-ifelse(is.na(hly200411$SC6),12000,hly200411$SC6)

str(hly200411)

hly200411$SC7<-apply(hly200411[,16:18], 1, min)

hly200411$SkyConditions<-hly200411$SC7
hly200411$SC1<-NULL
hly200411$SC2<-NULL
hly200411$SC3<-NULL
hly200411$SC4<-NULL
hly200411$SC5<-NULL
hly200411$SC6<-NULL
hly200411$SC7<-NULL

# Changing Format as applicable

hly200411$YearMonthDay<-ymd(hly200411$YearMonthDay)
hly200411$YearMonthDay<-as.factor(hly200411$YearMonthDay)
hly200411$WeatherStationID<-as.factor(hly200411$WeatherStationID)

# Deriving time slots in weather data

hly200411$TimeSlot<-ifelse(hly200411$Time<200,'Midnight to 2AM',ifelse(hly200411$Time<400,'2AM to 4AM',
                                                                       ifelse(hly200411$Time<600,'4AM to 6AM',
                                                                              ifelse(hly200411$Time<800,'6AM to 8AM',
                                                                                     ifelse(hly200411$Time<1000,'8AM to 10AM',
                                                                                            ifelse(hly200411$Time<1200,'10AM to Noon',
                                                                                                   ifelse(hly200411$Time<1400,'Noon to 2PM',
                                                                                                          ifelse(hly200411$Time<1600,'2PM to 4PM',
                                                                                                                 ifelse(hly200411$Time<1800,'4PM to 6PM',
                                                                                                                        ifelse(hly200411$Time<2000,'6PM to 8PM',
                                                                                                                               ifelse(hly200411$Time<2200,'8PM to 10PM','10PM to Midnight')))))))))))

hly200411$Time<-NULL # Dropping time column

# Aggregating Hourly Precipitation by Station, Date & Slot

hly200411<-sqldf('select distinct a.WeatherStationID, a.YearMonthDay, a.TimeSlot, avg(a.SkyConditions) as AvgSkyCond,
                 avg(a.Visibility) as AvgVis, avg(a.DBT) as AvgDBT, avg(a.DewPointTemp) as AvgDewPtTemp,
                 avg(a.RelativeHumidityPercent) as AvgRelHumPerc, avg(a.WindSpeed) as AvgWindSp,
                 avg(a.WindDirection) as AvgWindDir, avg(a.WindGustValue) as AvgWindGustVal,
                 avg(a.StationPressure) as AvgStnPres from hly200411 a group by
                 a.WeatherStationID, a.YearMonthDay, a.TimeSlot')

# Merging with close station data

closestation <- readRDS("closestation.rds")

hly0411<-merge(hly200411,closestation,by.x="WeatherStationID",by.y="WeatherStationID")

# Creating Keys for future merging

hly0411$Key0<-paste(hly0411$WeatherStationID,hly0411$YearMonthDay,hly0411$TimeSlot)
hly0411$Key1<-paste(hly0411$ClosestWS,hly0411$YearMonthDay,hly0411$TimeSlot)
hly0411$Key2<-paste(hly0411$Closest2ndWS,hly0411$YearMonthDay,hly0411$TimeSlot)
hly0411$Key3<-paste(hly0411$Closest3rdWS,hly0411$YearMonthDay,hly0411$TimeSlot)
hly0411$Key4<-paste(hly0411$Closest4thWS,hly0411$YearMonthDay,hly0411$TimeSlot)
hly0411$Key5<-paste(hly0411$Closest5thWS,hly0411$YearMonthDay,hly0411$TimeSlot)

# Merging with Closest Weather Stations

rm(hly200411) # Free up memory
temp<-hly0411

names(hly0411)[names(hly0411) == "AvgSkyCond"] = "OrigSkyCond"
names(hly0411)[names(hly0411) == "AvgVis"] = "OrigVis"
names(hly0411)[names(hly0411) == "AvgDBT"] = "OrigDBT"
names(hly0411)[names(hly0411) == "AvgDewPtTemp"] = "OrigDewPtTemp"
names(hly0411)[names(hly0411) == "AvgRelHumPerc"] = "OrigRelHumPerc"
names(hly0411)[names(hly0411) == "AvgWindSp"] = "OrigWindSp"
names(hly0411)[names(hly0411) == "AvgWindDir"] = "OrigWindDir"
names(hly0411)[names(hly0411) == "AvgWindGustVal"] = "OrigWindGustVal"
names(hly0411)[names(hly0411) == "AvgStnPres"] = "OrigStnPres"

hly0411<-sqldf('select a.*, b.AvgSkyCond, b.AvgVis, b.AvgDBT, b.AvgDewPtTemp, b.AvgRelHumPerc,
               b.AvgWindSp, b.AvgWindDir, b.AvgWindGustVal, b.AvgStnPres from hly0411 a left join
               (select Key0, AvgSkyCond, AvgVis, AvgDBT, AvgDewPtTemp, AvgRelHumPerc, AvgWindSp,
               AvgWindDir, AvgWindGustVal, AvgStnPres from temp) b on a.Key1 = b.Key0')

names(hly0411)[names(hly0411) == "AvgSkyCond"] = "ClosestSkyCond"
names(hly0411)[names(hly0411) == "AvgVis"] = "ClosestVis"
names(hly0411)[names(hly0411) == "AvgDBT"] = "ClosestDBT"
names(hly0411)[names(hly0411) == "AvgDewPtTemp"] = "ClosestDewPtTemp"
names(hly0411)[names(hly0411) == "AvgRelHumPerc"] = "ClosestRelHumPerc"
names(hly0411)[names(hly0411) == "AvgWindSp"] = "ClosestWindSp"
names(hly0411)[names(hly0411) == "AvgWindDir"] = "ClosestWindDir"
names(hly0411)[names(hly0411) == "AvgWindGustVal"] = "ClosestWindGustVal"
names(hly0411)[names(hly0411) == "AvgStnPres"] = "ClosestStnPres"

gc()

hly0411<-sqldf('select a.*, b.AvgSkyCond, b.AvgVis, b.AvgDBT, b.AvgDewPtTemp, b.AvgRelHumPerc,
               b.AvgWindSp, b.AvgWindDir, b.AvgWindGustVal, b.AvgStnPres from hly0411 a left join
               (select Key0, AvgSkyCond, AvgVis, AvgDBT, AvgDewPtTemp, AvgRelHumPerc, AvgWindSp,
               AvgWindDir, AvgWindGustVal, AvgStnPres from temp) b on a.Key2 = b.Key0')

names(hly0411)[names(hly0411) == "AvgSkyCond"] = "Closest2ndSkyCond"
names(hly0411)[names(hly0411) == "AvgVis"] = "Closest2ndVis"
names(hly0411)[names(hly0411) == "AvgDBT"] = "Closest2ndDBT"
names(hly0411)[names(hly0411) == "AvgDewPtTemp"] = "Closest2ndDewPtTemp"
names(hly0411)[names(hly0411) == "AvgRelHumPerc"] = "Closest2ndRelHumPerc"
names(hly0411)[names(hly0411) == "AvgWindSp"] = "Closest2ndWindSp"
names(hly0411)[names(hly0411) == "AvgWindDir"] = "Closest2ndWindDir"
names(hly0411)[names(hly0411) == "AvgWindGustVal"] = "Closest2ndWindGustVal"
names(hly0411)[names(hly0411) == "AvgStnPres"] = "Closest2ndStnPres"

gc()

hly0411<-sqldf('select a.*, b.AvgSkyCond, b.AvgVis, b.AvgDBT, b.AvgDewPtTemp, b.AvgRelHumPerc,
               b.AvgWindSp, b.AvgWindDir, b.AvgWindGustVal, b.AvgStnPres from hly0411 a left join
               (select Key0, AvgSkyCond, AvgVis, AvgDBT, AvgDewPtTemp, AvgRelHumPerc, AvgWindSp,
               AvgWindDir, AvgWindGustVal, AvgStnPres from temp) b on a.Key3 = b.Key0')

names(hly0411)[names(hly0411) == "AvgSkyCond"] = "Closest3rdSkyCond"
names(hly0411)[names(hly0411) == "AvgVis"] = "Closest3rdVis"
names(hly0411)[names(hly0411) == "AvgDBT"] = "Closest3rdDBT"
names(hly0411)[names(hly0411) == "AvgDewPtTemp"] = "Closest3rdDewPtTemp"
names(hly0411)[names(hly0411) == "AvgRelHumPerc"] = "Closest3rdRelHumPerc"
names(hly0411)[names(hly0411) == "AvgWindSp"] = "Closest3rdWindSp"
names(hly0411)[names(hly0411) == "AvgWindDir"] = "Closest3rdWindDir"
names(hly0411)[names(hly0411) == "AvgWindGustVal"] = "Closest3rdWindGustVal"
names(hly0411)[names(hly0411) == "AvgStnPres"] = "Closest3rdStnPres"

gc()

hly0411<-sqldf('select a.*, b.AvgSkyCond, b.AvgVis, b.AvgDBT, b.AvgDewPtTemp, b.AvgRelHumPerc,
               b.AvgWindSp, b.AvgWindDir, b.AvgWindGustVal, b.AvgStnPres from hly0411 a left join
               (select Key0, AvgSkyCond, AvgVis, AvgDBT, AvgDewPtTemp, AvgRelHumPerc, AvgWindSp,
               AvgWindDir, AvgWindGustVal, AvgStnPres from temp) b on a.Key4 = b.Key0')

names(hly0411)[names(hly0411) == "AvgSkyCond"] = "Closest4thSkyCond"
names(hly0411)[names(hly0411) == "AvgVis"] = "Closest4thVis"
names(hly0411)[names(hly0411) == "AvgDBT"] = "Closest4thDBT"
names(hly0411)[names(hly0411) == "AvgDewPtTemp"] = "Closest4thDewPtTemp"
names(hly0411)[names(hly0411) == "AvgRelHumPerc"] = "Closest4thRelHumPerc"
names(hly0411)[names(hly0411) == "AvgWindSp"] = "Closest4thWindSp"
names(hly0411)[names(hly0411) == "AvgWindDir"] = "Closest4thWindDir"
names(hly0411)[names(hly0411) == "AvgWindGustVal"] = "Closest4thWindGustVal"
names(hly0411)[names(hly0411) == "AvgStnPres"] = "Closest4thStnPres"

gc()

hly0411<-sqldf('select a.*, b.AvgSkyCond, b.AvgVis, b.AvgDBT, b.AvgDewPtTemp, b.AvgRelHumPerc,
               b.AvgWindSp, b.AvgWindDir, b.AvgWindGustVal, b.AvgStnPres from hly0411 a left join
               (select Key0, AvgSkyCond, AvgVis, AvgDBT, AvgDewPtTemp, AvgRelHumPerc, AvgWindSp,
               AvgWindDir, AvgWindGustVal, AvgStnPres from temp) b on a.Key5 = b.Key0')

names(hly0411)[names(hly0411) == "AvgSkyCond"] = "Closest5thSkyCond"
names(hly0411)[names(hly0411) == "AvgVis"] = "Closest5thVis"
names(hly0411)[names(hly0411) == "AvgDBT"] = "Closest5thDBT"
names(hly0411)[names(hly0411) == "AvgDewPtTemp"] = "Closest5thDewPtTemp"
names(hly0411)[names(hly0411) == "AvgRelHumPerc"] = "Closest5thRelHumPerc"
names(hly0411)[names(hly0411) == "AvgWindSp"] = "Closest5thWindSp"
names(hly0411)[names(hly0411) == "AvgWindDir"] = "Closest5thWindDir"
names(hly0411)[names(hly0411) == "AvgWindGustVal"] = "Closest5thWindGustVal"
names(hly0411)[names(hly0411) == "AvgStnPres"] = "Closest5thStnPres"

gc()

# Check for null values & impute using closest neighbours

rm(temp) # Remove temp file
rm(closestation) # Remove unrequired file

colSums(is.na(hly0411)) # NA values in OrigPrecip column

hly0411$OrigSkyCond<-ifelse(is.na(hly0411$OrigSkyCond),rowMeans(hly0411[,c("ClosestSkyCond", "Closest2ndSkyCond",
                                                                           "Closest3rdSkyCond","Closest4thSkyCond",
                                                                           "Closest5thSkyCond")], na.rm=TRUE),
                            hly0411$OrigSkyCond)

hly0411$OrigVis<-ifelse(is.na(hly0411$OrigVis),rowMeans(hly0411[,c("ClosestVis", "Closest2ndVis",
                                                                   "Closest3rdVis","Closest4thVis",
                                                                   "Closest5thVis")], na.rm=TRUE),
                        hly0411$OrigVis)

hly0411$OrigDBT<-ifelse(is.na(hly0411$OrigDBT),rowMeans(hly0411[,c("ClosestDBT", "Closest2ndDBT",
                                                                   "Closest3rdDBT","Closest4thDBT",
                                                                   "Closest5thDBT")], na.rm=TRUE),
                        hly0411$OrigDBT)

hly0411$OrigDewPtTemp<-ifelse(is.na(hly0411$OrigDewPtTemp),rowMeans(hly0411[,c("ClosestDewPtTemp", "Closest2ndDewPtTemp",
                                                                               "Closest3rdDewPtTemp","Closest4thDewPtTemp",
                                                                               "Closest5thDewPtTemp")], na.rm=TRUE),
                              hly0411$OrigDewPtTemp)

hly0411$OrigRelHumPerc<-ifelse(is.na(hly0411$OrigRelHumPerc),rowMeans(hly0411[,c("ClosestRelHumPerc", "Closest2ndRelHumPerc",
                                                                                 "Closest3rdRelHumPerc","Closest4thRelHumPerc",
                                                                                 "Closest5thRelHumPerc")], na.rm=TRUE),
                               hly0411$OrigRelHumPerc)

hly0411$OrigWindSp<-ifelse(is.na(hly0411$OrigWindSp),rowMeans(hly0411[,c("ClosestWindSp", "Closest2ndWindSp",
                                                                         "Closest3rdWindSp","Closest4thWindSp",
                                                                         "Closest5thWindSp")], na.rm=TRUE),
                           hly0411$OrigWindSp)

hly0411$OrigWindDir<-ifelse(is.na(hly0411$OrigWindDir),rowMeans(hly0411[,c("ClosestWindDir", "Closest2ndWindDir",
                                                                           "Closest3rdWindDir","Closest4thWindDir",
                                                                           "Closest5thWindDir")], na.rm=TRUE),
                            hly0411$OrigWindDir)

hly0411$OrigWindGustVal<-ifelse(is.na(hly0411$OrigWindGustVal),rowMeans(hly0411[,c("ClosestWindGustVal", "Closest2ndWindGustVal",
                                                                                   "Closest3rdWindGustVal","Closest4thWindGustVal",
                                                                                   "Closest5thWindGustVal")], na.rm=TRUE),
                                hly0411$OrigWindGustVal)

hly0411$OrigStnPres<-ifelse(is.na(hly0411$OrigStnPres),rowMeans(hly0411[,c("ClosestStnPres", "Closest2ndStnPres",
                                                                           "Closest3rdStnPres","Closest4thStnPres",
                                                                           "Closest5thStnPres")], na.rm=TRUE),
                            hly0411$OrigStnPres)

saveRDS(hly0411, file = "hly0411.rds") # Saving externally