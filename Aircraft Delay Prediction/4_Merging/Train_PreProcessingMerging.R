library(sqldf)
library(lubridate)

rm(list=ls())

flight<-read.csv("Train.csv",header = T) # Reading the CSV

str(flight) # Checking the structure

# Deriving variable SchedDate

flight$SchedDate<-paste(flight$Year,'-',flight$Month,'-',flight$DayofMonth,sep = "") # Joining

# Deriving variable Scheduled Arrival TimeStamp

flight$SchedArrTime <- substr(as.POSIXct(sprintf("%04.0f", flight$ScheduledArrTime),
                                         format='%H%M'), 12, 16)

flight$ScheduledArrivalTimeStamp<- as.POSIXct(paste(flight$SchedDate, flight$SchedArrTime), 
                                              format="%Y-%m-%d %H:%M",tz = 'GMT')

flight$SchedArrTime<-NULL # Removing unrequired column
flight$SchedDate<-ymd(flight$SchedDate) # Converting to date format

# Converting Actual Arrival TimeStamp

flight$ActualArrivalTimeStamp<-dmy_hm(flight$ActualArrivalTimeStamp)

# Deriving Target Variable

flight$Delay<-difftime(flight$ActualArrivalTimeStamp, flight$ScheduledArrivalTimeStamp, 
                       units = "mins") # Calculating time difference

flight$FlightDelayStatus<-ifelse(flight$Delay>15,1,0) # Deriving target variable
flight$FlightDelayStatus<-as.factor(flight$FlightDelayStatus)

# Creating Time Slots as per Scheduled Departure & Scheduled Arrival Times

flight$SchDepSlot<-ifelse(flight$ScheduledDepTime<200,'Midnight to 2AM',ifelse(flight$ScheduledDepTime<400,'2AM to 4AM',
                                                                         ifelse(flight$ScheduledDepTime<600,'4AM to 6AM',
                                                                                ifelse(flight$ScheduledDepTime<800,'6AM to 8AM',
                                                                                       ifelse(flight$ScheduledDepTime<1000,'8AM to 10AM',
                                                                                              ifelse(flight$ScheduledDepTime<1200,'10AM to Noon',
                                                                                                     ifelse(flight$ScheduledDepTime<1400,'Noon to 2PM',
                                                                                                            ifelse(flight$ScheduledDepTime<1600,'2PM to 4PM',
                                                                                                                   ifelse(flight$ScheduledDepTime<1800,'4PM to 6PM',
                                                                                                                          ifelse(flight$ScheduledDepTime<2000,'6PM to 8PM',
                                                                                                                                 ifelse(flight$ScheduledDepTime<2200,'8PM to 10PM','10PM to Midnight')))))))))))

flight$SchArrSlot<-ifelse(flight$ScheduledArrTime<200,'Midnight to 2AM',ifelse(flight$ScheduledArrTime<400,'2AM to 4AM',
                                                                               ifelse(flight$ScheduledArrTime<600,'4AM to 6AM',
                                                                                      ifelse(flight$ScheduledArrTime<800,'6AM to 8AM',
                                                                                             ifelse(flight$ScheduledArrTime<1000,'8AM to 10AM',
                                                                                                    ifelse(flight$ScheduledArrTime<1200,'10AM to Noon',
                                                                                                           ifelse(flight$ScheduledArrTime<1400,'Noon to 2PM',
                                                                                                                  ifelse(flight$ScheduledArrTime<1600,'2PM to 4PM',
                                                                                                                         ifelse(flight$ScheduledArrTime<1800,'4PM to 6PM',
                                                                                                                                ifelse(flight$ScheduledArrTime<2000,'6PM to 8PM',
                                                                                                                                       ifelse(flight$ScheduledArrTime<2200,'8PM to 10PM','10PM to Midnight')))))))))))
# Changing delay to integer

flight$Delay<-as.integer(flight$Delay)

# Merging with BTS Data to obtain carrier information
# BTS Data can be downloaded from here: https://drive.google.com/drive/folders/1_guRqrP__cYQk0IgXpdSruteMCANzfBO

data0401<-read.csv('2004_01.csv',header = TRUE)
data0403<-read.csv('2004_03.csv',header = TRUE)
data0405<-read.csv('2004_05.csv',header = TRUE)
data0407<-read.csv('2004_07.csv',header = TRUE)
data0409<-read.csv('2004_09.csv',header = TRUE)
data0411<-read.csv('2004_11.csv',header = TRUE)

alldata<-rbind(data0401,data0403,data0405,data0407,data0409,data0411)
rm(data0401,data0403,data0405,data0407,data0409,data0411) # Free up memory

str(alldata)
str(flight)

flightdata<-sqldf('select a.*, b.CARRIER, b.TAIL_NUM, b.FL_NUM from flight a left join 
                  (select distinct YEAR, MONTH, DAY_OF_MONTH,CARRIER,TAIL_NUM,FL_NUM,ORIGIN,DEST,
                  CRS_DEP_TIME,CRS_ARR_TIME from alldata) b
                  on a.Year = b.Year and a.Month = b.Month and a.DayofMonth = b.DAY_OF_MONTH and
                  a.Origin = b.ORIGIN and a.Destination = b.DEST and a.ScheduledDepTime = b.CRS_DEP_TIME
                  and a.ScheduledArrTime = b.CRS_ARR_TIME')

rm(alldata) # Free up memory

flightdata<-sqldf('select FlightNumber,Year,Month,DayofMonth,DayOfWeek,ScheduledDepTime,ScheduledArrTime,
                  ScheduledTravelTime,Origin,Destination,Distance,ActualArrivalTimeStamp,SchedDate,
                  ScheduledArrivalTimeStamp,Delay,FlightDelayStatus,SchDepSlot,SchArrSlot,
                  min(CARRIER) as CARRIER, min(TAIL_NUM) as TAILNUM, min(FL_NUM) as FLNUM
                  from flightdata group by
                  FlightNumber,Year,Month,DayofMonth,DayOfWeek,ScheduledDepTime,ScheduledArrTime,
                  ScheduledTravelTime,Origin,Destination,Distance,ActualArrivalTimeStamp,SchedDate,
                  ScheduledArrivalTimeStamp,Delay,FlightDelayStatus,SchDepSlot,SchArrSlot')

flight<-flightdata # Re-assigning
rm(flightdata) # Remove temp variable

str(flight)

# Removing some variables which are not required in modeling

flight$ScheduledArrivalTimeStamp<-NULL
flight$ActualArrivalTimeStamp<-NULL
flight$Year<-NULL
flight$ScheduledDepTime<-NULL
flight$ScheduledArrTime<-NULL

str(flight) # Checking revised data structure

# Converting to required formats

flight$SchedDate<-as.factor(flight$SchedDate)
flight$SchDepSlot<-as.factor(flight$SchDepSlot)
flight$SchArrSlot<-as.factor(flight$SchArrSlot)

# Deriving information from Flight Numbers

flight$Direction<-ifelse(flight$FLNUM %% 2==0,'North or East Bound','South or West Bound')
flight$Type<-ifelse(flight$FLNUM<99,'LongHaul/Premium',ifelse(flight$FLNUM<999,'Domestic',
                                                                     ifelse(flight$FLNUM<2999,'Regional',
                                                                                   ifelse(flight$FLNUM<5999,'RegionalAffiliate',
                                                                                          ifelse(flight$FLNUM<9000,'Codeshare','Ferry')))))

# Upon cross - checking day of week and month details with September 2004 calendar it was found that dayofweek = 1
# means Monday, dayofweek = 2 means Tuesday and so on...

flight$DayOfWeek<-ifelse(flight$DayOfWeek==1,'Mon',ifelse(flight$DayOfWeek==2,'Tue',
                                                            ifelse(flight$DayOfWeek==3,'Wed',
                                                                   ifelse(flight$DayOfWeek==4,'Thu',
                                                                          ifelse(flight$DayOfWeek==5,'Fri',
                                                                                 ifelse(flight$DayOfWeek==6,'Sat',
                                                                                        'Sun'))))))
flight$DayOfWeek<-as.factor(flight$DayOfWeek) # Converting to factor

flight$Month<-ifelse(flight$Month==1,'Jan',ifelse(flight$Month==2,'Feb',ifelse(flight$Month==3,'Mar',
                                                                               ifelse(flight$Month==4,'Apr',
                                                                                      ifelse(flight$Month==5,'May',
                                                                                             ifelse(flight$Month==6,'Jun',
                                                                                                    ifelse(flight$Month==7,'Jul',
                                                                                                           ifelse(flight$Month==8,'Aug',
                                                                                                                  ifelse(flight$Month==9,'Sep',
                                                                                                                         ifelse(flight$Month==10,'Oct',
                                                                                                                                ifelse(flight$Month==11,'Nov','Dec')))))))))))
flight$Month<-as.factor(flight$Month) # Converting to factor

str(flight) # Checking structure of revised data frame

# Loading hpd data

hpd<-readRDS("hpd.rds")
hpd$TimeSlot<-as.factor(hpd$TimeSlot)

str(hpd)

# Merging with flight data first by origin & then by destination

flight<-sqldf('select a.*, b.OrigPrecip from flight a left join (select distinct AirportID, YearMonthDay, TimeSlot,
              OrigPrecip from hpd) b on a.Origin = b.AirportID and a.SchedDate = b.YearMonthDay
              and a.SchDepSlot = b.TimeSlot')

flight<-sqldf('select a.*, b.DestPrecip from flight a left join (select distinct AirportID, YearMonthDay, TimeSlot,
              OrigPrecip as DestPrecip from hpd) b on a.Destination = b.AirportID and a.SchedDate = b.YearMonthDay
              and a.SchArrSlot = b.TimeSlot')

rm(hpd) # Freeing up memory

# Loading hly data

hly<-readRDS("hly.rds")
hly$TimeSlot<-as.factor(hly$TimeSlot)

str(hly)

# Merging with flight data first by origin & then by destination

flight<-sqldf('select a.*, b.OrigSkyCond, b.OrigVis, b.OrigDBT, b.OrigDewPtTemp, b.OrigRelHumPerc,
              b.OrigWindSp, b.OrigWindDir, b.OrigWindGustVal, b.OrigStnPres from flight a left join
              (select distinct AirportID, YearMonthDay, TimeSlot, OrigSkyCond, OrigVis, OrigDBT,
              OrigDewPtTemp, OrigRelHumPerc, OrigWindSp, OrigWindDir, OrigWindGustVal, OrigStnPres
              from hly) b on a.Origin = b.AirportID and a.SchedDate = b.YearMonthDay
              and a.SchDepSlot = b.TimeSlot')

flight<-sqldf('select a.*, b.DestSkyCond, b.DestVis, b.DestDBT, b.DestDewPtTemp, b.DestRelHumPerc,
              b.DestWindSp, b.DestWindDir, b.DestWindGustVal, b.DestStnPres from flight a left join
              (select distinct AirportID, YearMonthDay, TimeSlot, OrigSkyCond as DestSkyCond, OrigVis as DestVis, OrigDBT as DestDBT,
              OrigDewPtTemp as DestDewPtTemp, OrigRelHumPerc as DestRelHumPerc, OrigWindSp as DestWindSp, OrigWindDir as DestWindDir, 
              OrigWindGustVal as DestWindGustVal, OrigStnPres as DestStnPres
              from hly) b on a.Destination = b.AirportID and a.SchedDate = b.YearMonthDay
              and a.SchArrSlot = b.TimeSlot')

rm(hly) # Freeing up memory

## Write CSV for Visualization

write.csv(flight,file="flight.csv")

# Remove Delay Column & save

flight$Delay<-NULL

saveRDS(flight, file = "flight.rds")