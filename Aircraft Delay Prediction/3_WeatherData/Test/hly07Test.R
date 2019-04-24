rm(list=ls())

library(lubridate)
library(sqldf)
library(DMwR)

hly200507<- read.table("200507hourly.txt", sep=",",header = T, dec = ".") # Read 07 File
str(hly200507)

# Removing units from visibility & changing to numeric
hly200507$Visibility<-as.character(hly200507$Visibility)
hly200507$Visibility <- gsub("SM","",hly200507$Visibility,fixed = TRUE)
hly200507$Visibility<-as.numeric(hly200507$Visibility)


# Splitting Sky Condition & Deriving the lowest ceiling height with Broken or Overcast
hly200507$SkyConditions<-as.character(hly200507$SkyConditions)
library(data.table)
setDT(hly200507)[, paste0("SC", 1:3) := tstrsplit(SkyConditions, " ")]
library(stringr)
hly200507$SC4<-ifelse(str_sub(hly200507$SC1,1,3)%in%c('BKN','OVC') | str_sub(hly200507$SC1,1,2) == 'VV',
                      as.integer(str_sub(hly200507$SC1,-2-1))*100,12000)
hly200507$SC5<-ifelse(str_sub(hly200507$SC2,1,3)%in%c('BKN','OVC') | str_sub(hly200507$SC2,1,2) == 'VV',
                      as.integer(str_sub(hly200507$SC2,-2-1))*100,12000)
hly200507$SC6<-ifelse(str_sub(hly200507$SC3,1,3)%in%c('BKN','OVC') | str_sub(hly200507$SC3,1,2) == 'VV',
                      as.integer(str_sub(hly200507$SC3,-2-1))*100,12000)

hly200507$SC4<-ifelse(is.na(hly200507$SC4),12000,hly200507$SC4)
hly200507$SC5<-ifelse(is.na(hly200507$SC5),12000,hly200507$SC5)
hly200507$SC6<-ifelse(is.na(hly200507$SC6),12000,hly200507$SC6)

str(hly200507)

hly200507$SC7<-apply(hly200507[,16:18], 1, min)

hly200507$SkyConditions<-hly200507$SC7
hly200507$SC1<-NULL
hly200507$SC2<-NULL
hly200507$SC3<-NULL
hly200507$SC4<-NULL
hly200507$SC5<-NULL
hly200507$SC6<-NULL
hly200507$SC7<-NULL

# Changing Format as applicable

hly200507$YearMonthDay<-ymd(hly200507$YearMonthDay)
hly200507$YearMonthDay<-as.factor(hly200507$YearMonthDay)
hly200507$WeatherStationID<-as.factor(hly200507$WeatherStationID)

# Deriving time slots in weather data

hly200507$TimeSlot<-ifelse(hly200507$Time<200,'Midnight to 2AM',ifelse(hly200507$Time<400,'2AM to 4AM',
                                                                       ifelse(hly200507$Time<600,'4AM to 6AM',
                                                                              ifelse(hly200507$Time<800,'6AM to 8AM',
                                                                                     ifelse(hly200507$Time<1000,'8AM to 10AM',
                                                                                            ifelse(hly200507$Time<1200,'10AM to Noon',
                                                                                                   ifelse(hly200507$Time<1400,'Noon to 2PM',
                                                                                                          ifelse(hly200507$Time<1600,'2PM to 4PM',
                                                                                                                 ifelse(hly200507$Time<1800,'4PM to 6PM',
                                                                                                                        ifelse(hly200507$Time<2000,'6PM to 8PM',
                                                                                                                               ifelse(hly200507$Time<2200,'8PM to 10PM','10PM to Midnight')))))))))))

hly200507$Time<-NULL # Dropping time column

# Aggregating Hourly Precipitation by Station, Date & Slot

hly200507<-sqldf('select distinct a.WeatherStationID, a.YearMonthDay, a.TimeSlot, avg(a.SkyConditions) as AvgSkyCond,
                 avg(a.Visibility) as AvgVis, avg(a.DBT) as AvgDBT, avg(a.DewPointTemp) as AvgDewPtTemp,
                 avg(a.RelativeHumidityPercent) as AvgRelHumPerc, avg(a.WindSpeed) as AvgWindSp,
                 avg(a.WindDirection) as AvgWindDir, avg(a.WindGustValue) as AvgWindGustVal,
                 avg(a.StationPressure) as AvgStnPres from hly200507 a group by
                 a.WeatherStationID, a.YearMonthDay, a.TimeSlot')

# Merging with close station data

closestation <- readRDS("closestation.rds")

hly0507<-merge(hly200507,closestation,by.x="WeatherStationID",by.y="WeatherStationID")

# Creating Keys for future merging

hly0507$Key0<-paste(hly0507$WeatherStationID,hly0507$YearMonthDay,hly0507$TimeSlot)
hly0507$Key1<-paste(hly0507$ClosestWS,hly0507$YearMonthDay,hly0507$TimeSlot)
hly0507$Key2<-paste(hly0507$Closest2ndWS,hly0507$YearMonthDay,hly0507$TimeSlot)
hly0507$Key3<-paste(hly0507$Closest3rdWS,hly0507$YearMonthDay,hly0507$TimeSlot)
hly0507$Key4<-paste(hly0507$Closest4thWS,hly0507$YearMonthDay,hly0507$TimeSlot)
hly0507$Key5<-paste(hly0507$Closest5thWS,hly0507$YearMonthDay,hly0507$TimeSlot)

# Merging with Closest Weather Stations

rm(hly200507) # Free up memory
temp<-hly0507

names(hly0507)[names(hly0507) == "AvgSkyCond"] = "OrigSkyCond"
names(hly0507)[names(hly0507) == "AvgVis"] = "OrigVis"
names(hly0507)[names(hly0507) == "AvgDBT"] = "OrigDBT"
names(hly0507)[names(hly0507) == "AvgDewPtTemp"] = "OrigDewPtTemp"
names(hly0507)[names(hly0507) == "AvgRelHumPerc"] = "OrigRelHumPerc"
names(hly0507)[names(hly0507) == "AvgWindSp"] = "OrigWindSp"
names(hly0507)[names(hly0507) == "AvgWindDir"] = "OrigWindDir"
names(hly0507)[names(hly0507) == "AvgWindGustVal"] = "OrigWindGustVal"
names(hly0507)[names(hly0507) == "AvgStnPres"] = "OrigStnPres"

hly0507<-sqldf('select a.*, b.AvgSkyCond, b.AvgVis, b.AvgDBT, b.AvgDewPtTemp, b.AvgRelHumPerc,
               b.AvgWindSp, b.AvgWindDir, b.AvgWindGustVal, b.AvgStnPres from hly0507 a left join
               (select Key0, AvgSkyCond, AvgVis, AvgDBT, AvgDewPtTemp, AvgRelHumPerc, AvgWindSp,
               AvgWindDir, AvgWindGustVal, AvgStnPres from temp) b on a.Key1 = b.Key0')

names(hly0507)[names(hly0507) == "AvgSkyCond"] = "ClosestSkyCond"
names(hly0507)[names(hly0507) == "AvgVis"] = "ClosestVis"
names(hly0507)[names(hly0507) == "AvgDBT"] = "ClosestDBT"
names(hly0507)[names(hly0507) == "AvgDewPtTemp"] = "ClosestDewPtTemp"
names(hly0507)[names(hly0507) == "AvgRelHumPerc"] = "ClosestRelHumPerc"
names(hly0507)[names(hly0507) == "AvgWindSp"] = "ClosestWindSp"
names(hly0507)[names(hly0507) == "AvgWindDir"] = "ClosestWindDir"
names(hly0507)[names(hly0507) == "AvgWindGustVal"] = "ClosestWindGustVal"
names(hly0507)[names(hly0507) == "AvgStnPres"] = "ClosestStnPres"

gc()

hly0507<-sqldf('select a.*, b.AvgSkyCond, b.AvgVis, b.AvgDBT, b.AvgDewPtTemp, b.AvgRelHumPerc,
               b.AvgWindSp, b.AvgWindDir, b.AvgWindGustVal, b.AvgStnPres from hly0507 a left join
               (select Key0, AvgSkyCond, AvgVis, AvgDBT, AvgDewPtTemp, AvgRelHumPerc, AvgWindSp,
               AvgWindDir, AvgWindGustVal, AvgStnPres from temp) b on a.Key2 = b.Key0')

names(hly0507)[names(hly0507) == "AvgSkyCond"] = "Closest2ndSkyCond"
names(hly0507)[names(hly0507) == "AvgVis"] = "Closest2ndVis"
names(hly0507)[names(hly0507) == "AvgDBT"] = "Closest2ndDBT"
names(hly0507)[names(hly0507) == "AvgDewPtTemp"] = "Closest2ndDewPtTemp"
names(hly0507)[names(hly0507) == "AvgRelHumPerc"] = "Closest2ndRelHumPerc"
names(hly0507)[names(hly0507) == "AvgWindSp"] = "Closest2ndWindSp"
names(hly0507)[names(hly0507) == "AvgWindDir"] = "Closest2ndWindDir"
names(hly0507)[names(hly0507) == "AvgWindGustVal"] = "Closest2ndWindGustVal"
names(hly0507)[names(hly0507) == "AvgStnPres"] = "Closest2ndStnPres"

gc()

hly0507<-sqldf('select a.*, b.AvgSkyCond, b.AvgVis, b.AvgDBT, b.AvgDewPtTemp, b.AvgRelHumPerc,
               b.AvgWindSp, b.AvgWindDir, b.AvgWindGustVal, b.AvgStnPres from hly0507 a left join
               (select Key0, AvgSkyCond, AvgVis, AvgDBT, AvgDewPtTemp, AvgRelHumPerc, AvgWindSp,
               AvgWindDir, AvgWindGustVal, AvgStnPres from temp) b on a.Key3 = b.Key0')

names(hly0507)[names(hly0507) == "AvgSkyCond"] = "Closest3rdSkyCond"
names(hly0507)[names(hly0507) == "AvgVis"] = "Closest3rdVis"
names(hly0507)[names(hly0507) == "AvgDBT"] = "Closest3rdDBT"
names(hly0507)[names(hly0507) == "AvgDewPtTemp"] = "Closest3rdDewPtTemp"
names(hly0507)[names(hly0507) == "AvgRelHumPerc"] = "Closest3rdRelHumPerc"
names(hly0507)[names(hly0507) == "AvgWindSp"] = "Closest3rdWindSp"
names(hly0507)[names(hly0507) == "AvgWindDir"] = "Closest3rdWindDir"
names(hly0507)[names(hly0507) == "AvgWindGustVal"] = "Closest3rdWindGustVal"
names(hly0507)[names(hly0507) == "AvgStnPres"] = "Closest3rdStnPres"

gc()

hly0507<-sqldf('select a.*, b.AvgSkyCond, b.AvgVis, b.AvgDBT, b.AvgDewPtTemp, b.AvgRelHumPerc,
               b.AvgWindSp, b.AvgWindDir, b.AvgWindGustVal, b.AvgStnPres from hly0507 a left join
               (select Key0, AvgSkyCond, AvgVis, AvgDBT, AvgDewPtTemp, AvgRelHumPerc, AvgWindSp,
               AvgWindDir, AvgWindGustVal, AvgStnPres from temp) b on a.Key4 = b.Key0')

names(hly0507)[names(hly0507) == "AvgSkyCond"] = "Closest4thSkyCond"
names(hly0507)[names(hly0507) == "AvgVis"] = "Closest4thVis"
names(hly0507)[names(hly0507) == "AvgDBT"] = "Closest4thDBT"
names(hly0507)[names(hly0507) == "AvgDewPtTemp"] = "Closest4thDewPtTemp"
names(hly0507)[names(hly0507) == "AvgRelHumPerc"] = "Closest4thRelHumPerc"
names(hly0507)[names(hly0507) == "AvgWindSp"] = "Closest4thWindSp"
names(hly0507)[names(hly0507) == "AvgWindDir"] = "Closest4thWindDir"
names(hly0507)[names(hly0507) == "AvgWindGustVal"] = "Closest4thWindGustVal"
names(hly0507)[names(hly0507) == "AvgStnPres"] = "Closest4thStnPres"

gc()

hly0507<-sqldf('select a.*, b.AvgSkyCond, b.AvgVis, b.AvgDBT, b.AvgDewPtTemp, b.AvgRelHumPerc,
               b.AvgWindSp, b.AvgWindDir, b.AvgWindGustVal, b.AvgStnPres from hly0507 a left join
               (select Key0, AvgSkyCond, AvgVis, AvgDBT, AvgDewPtTemp, AvgRelHumPerc, AvgWindSp,
               AvgWindDir, AvgWindGustVal, AvgStnPres from temp) b on a.Key5 = b.Key0')

names(hly0507)[names(hly0507) == "AvgSkyCond"] = "Closest5thSkyCond"
names(hly0507)[names(hly0507) == "AvgVis"] = "Closest5thVis"
names(hly0507)[names(hly0507) == "AvgDBT"] = "Closest5thDBT"
names(hly0507)[names(hly0507) == "AvgDewPtTemp"] = "Closest5thDewPtTemp"
names(hly0507)[names(hly0507) == "AvgRelHumPerc"] = "Closest5thRelHumPerc"
names(hly0507)[names(hly0507) == "AvgWindSp"] = "Closest5thWindSp"
names(hly0507)[names(hly0507) == "AvgWindDir"] = "Closest5thWindDir"
names(hly0507)[names(hly0507) == "AvgWindGustVal"] = "Closest5thWindGustVal"
names(hly0507)[names(hly0507) == "AvgStnPres"] = "Closest5thStnPres"

gc()

# Check for null values & impute using closest neighbours

rm(temp) # Remove temp file
rm(closestation) # Remove unrequired file

colSums(is.na(hly0507)) # NA values in OrigPrecip column

hly0507$OrigSkyCond<-ifelse(is.na(hly0507$OrigSkyCond),rowMeans(hly0507[,c("ClosestSkyCond", "Closest2ndSkyCond",
                                                                           "Closest3rdSkyCond","Closest4thSkyCond",
                                                                           "Closest5thSkyCond")], na.rm=TRUE),
                            hly0507$OrigSkyCond)

hly0507$OrigVis<-ifelse(is.na(hly0507$OrigVis),rowMeans(hly0507[,c("ClosestVis", "Closest2ndVis",
                                                                   "Closest3rdVis","Closest4thVis",
                                                                   "Closest5thVis")], na.rm=TRUE),
                        hly0507$OrigVis)

hly0507$OrigDBT<-ifelse(is.na(hly0507$OrigDBT),rowMeans(hly0507[,c("ClosestDBT", "Closest2ndDBT",
                                                                   "Closest3rdDBT","Closest4thDBT",
                                                                   "Closest5thDBT")], na.rm=TRUE),
                        hly0507$OrigDBT)

hly0507$OrigDewPtTemp<-ifelse(is.na(hly0507$OrigDewPtTemp),rowMeans(hly0507[,c("ClosestDewPtTemp", "Closest2ndDewPtTemp",
                                                                               "Closest3rdDewPtTemp","Closest4thDewPtTemp",
                                                                               "Closest5thDewPtTemp")], na.rm=TRUE),
                              hly0507$OrigDewPtTemp)

hly0507$OrigRelHumPerc<-ifelse(is.na(hly0507$OrigRelHumPerc),rowMeans(hly0507[,c("ClosestRelHumPerc", "Closest2ndRelHumPerc",
                                                                                 "Closest3rdRelHumPerc","Closest4thRelHumPerc",
                                                                                 "Closest5thRelHumPerc")], na.rm=TRUE),
                               hly0507$OrigRelHumPerc)

hly0507$OrigWindSp<-ifelse(is.na(hly0507$OrigWindSp),rowMeans(hly0507[,c("ClosestWindSp", "Closest2ndWindSp",
                                                                         "Closest3rdWindSp","Closest4thWindSp",
                                                                         "Closest5thWindSp")], na.rm=TRUE),
                           hly0507$OrigWindSp)

hly0507$OrigWindDir<-ifelse(is.na(hly0507$OrigWindDir),rowMeans(hly0507[,c("ClosestWindDir", "Closest2ndWindDir",
                                                                           "Closest3rdWindDir","Closest4thWindDir",
                                                                           "Closest5thWindDir")], na.rm=TRUE),
                            hly0507$OrigWindDir)

hly0507$OrigWindGustVal<-ifelse(is.na(hly0507$OrigWindGustVal),rowMeans(hly0507[,c("ClosestWindGustVal", "Closest2ndWindGustVal",
                                                                                   "Closest3rdWindGustVal","Closest4thWindGustVal",
                                                                                   "Closest5thWindGustVal")], na.rm=TRUE),
                                hly0507$OrigWindGustVal)

hly0507$OrigStnPres<-ifelse(is.na(hly0507$OrigStnPres),rowMeans(hly0507[,c("ClosestStnPres", "Closest2ndStnPres",
                                                                           "Closest3rdStnPres","Closest4thStnPres",
                                                                           "Closest5thStnPres")], na.rm=TRUE),
                            hly0507$OrigStnPres)

saveRDS(hly0507, file = "hly0507Test.rds") # Saving externally