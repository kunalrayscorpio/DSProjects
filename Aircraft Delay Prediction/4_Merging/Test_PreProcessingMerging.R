library(sqldf)
library(lubridate)

rm(list=ls())

flighttest<-read.csv("Test.csv",header = T) # Reading the CSV

str(flighttest) # Checking the structure

# Deriving variable SchedDate

flighttest$SchedDate<-paste(flighttest$Year,'-',flighttest$Month,'-',flighttest$DayofMonth,sep = "") # Joining
flighttest$SchedDate<-ymd(flighttest$SchedDate) # Converting to date format

# Creating Time Slots as per Scheduled Departure & Scheduled Arrival Times

flighttest$SchDepSlot<-ifelse(flighttest$ScheduledDepTime<200,'Midnight to 2AM',ifelse(flighttest$ScheduledDepTime<400,'2AM to 4AM',
                                                                               ifelse(flighttest$ScheduledDepTime<600,'4AM to 6AM',
                                                                                      ifelse(flighttest$ScheduledDepTime<800,'6AM to 8AM',
                                                                                             ifelse(flighttest$ScheduledDepTime<1000,'8AM to 10AM',
                                                                                                    ifelse(flighttest$ScheduledDepTime<1200,'10AM to Noon',
                                                                                                           ifelse(flighttest$ScheduledDepTime<1400,'Noon to 2PM',
                                                                                                                  ifelse(flighttest$ScheduledDepTime<1600,'2PM to 4PM',
                                                                                                                         ifelse(flighttest$ScheduledDepTime<1800,'4PM to 6PM',
                                                                                                                                ifelse(flighttest$ScheduledDepTime<2000,'6PM to 8PM',
                                                                                                                                       ifelse(flighttest$ScheduledDepTime<2200,'8PM to 10PM','10PM to Midnight')))))))))))

flighttest$SchArrSlot<-ifelse(flighttest$ScheduledArrTime<200,'Midnight to 2AM',ifelse(flighttest$ScheduledArrTime<400,'2AM to 4AM',
                                                                               ifelse(flighttest$ScheduledArrTime<600,'4AM to 6AM',
                                                                                      ifelse(flighttest$ScheduledArrTime<800,'6AM to 8AM',
                                                                                             ifelse(flighttest$ScheduledArrTime<1000,'8AM to 10AM',
                                                                                                    ifelse(flighttest$ScheduledArrTime<1200,'10AM to Noon',
                                                                                                           ifelse(flighttest$ScheduledArrTime<1400,'Noon to 2PM',
                                                                                                                  ifelse(flighttest$ScheduledArrTime<1600,'2PM to 4PM',
                                                                                                                         ifelse(flighttest$ScheduledArrTime<1800,'4PM to 6PM',
                                                                                                                                ifelse(flighttest$ScheduledArrTime<2000,'6PM to 8PM',
                                                                                                                                       ifelse(flighttest$ScheduledArrTime<2200,'8PM to 10PM','10PM to Midnight')))))))))))

# Merging with BTS Data to obtain carrier information
# BTS Data can be downloaded from here: https://drive.google.com/drive/folders/1_guRqrP__cYQk0IgXpdSruteMCANzfBO

data0503<-read.csv('2005_03.csv',header = TRUE)
data0507<-read.csv('2005_07.csv',header = TRUE)
data0509<-read.csv('2005_09.csv',header = TRUE)
data0511<-read.csv('2005_11.csv',header = TRUE)

alldata<-rbind(data0503,data0507,data0509,data0511)
rm(data0503,data0507,data0509,data0511) # Free up memory

str(alldata)
str(flighttest)

flighttestdata<-sqldf('select a.*, b.CARRIER, b.TAIL_NUM, b.FL_NUM from flighttest a left join 
                  (select distinct YEAR, MONTH, DAY_OF_MONTH,CARRIER,TAIL_NUM,FL_NUM,ORIGIN,DEST,
                  CRS_DEP_TIME,CRS_ARR_TIME from alldata) b
                  on a.Year = b.Year and a.Month = b.Month and a.DayofMonth = b.DAY_OF_MONTH and
                  a.Origin = b.ORIGIN and a.Destination = b.DEST and a.ScheduledDepTime = b.CRS_DEP_TIME
                  and a.ScheduledArrTime = b.CRS_ARR_TIME') # Left join with all data

rm(alldata) # Free up memory
flighttestdata<-sqldf('select distinct FlightNumber,Year,Month,DayofMonth,DayofWeek,ScheduledDeptime,
                      ScheduledArrTime,ScheduledTravelTime,Origin,Destination,Distance,SchedDate,
                      SchDepSlot,SchArrSlot,min(CARRIER) as CARRIER, min(TAIL_NUM) as TAILNUM,
                      min(FL_NUM) as FLNUM from flighttestdata group by
                      FlightNumber,Year,Month,DayofMonth,DayofWeek,ScheduledDeptime,
                      ScheduledArrTime,ScheduledTravelTime,Origin,Destination,Distance,SchedDate,
                      SchDepSlot,SchArrSlot') # Removing 20-30 rows with more than one level

flighttest<-flighttestdata # Re-assigning
rm(flighttestdata) # Remove temp variable

str(flighttest)

# Removing some variables which are not required in modeling

flighttest$Year<-NULL
flighttest$ScheduledDepTime<-NULL
flighttest$ScheduledArrTime<-NULL

# Converting to required formats

flighttest$SchedDate<-as.factor(flighttest$SchedDate)
flighttest$SchDepSlot<-as.factor(flighttest$SchDepSlot)
flighttest$SchArrSlot<-as.factor(flighttest$SchArrSlot)

# Deriving information from Flight Numbers
flighttest$Direction<-ifelse(flighttest$FLNUM %% 2==0,'North or East Bound','South or West Bound')
flighttest$Type<-ifelse(flighttest$FLNUM<99,'LongHaul/Premium',ifelse(flighttest$FLNUM<999,'Domestic',
                                                              ifelse(flighttest$FLNUM<2999,'Regional',
                                                                     ifelse(flighttest$FLNUM<5999,'RegionalAffiliate',
                                                                            ifelse(flighttest$FLNUM<9000,'Codeshare','Ferry')))))

# Converting Day of Week & Month

flighttest$DayOfWeek<-ifelse(flighttest$DayOfWeek==1,'Mon',ifelse(flighttest$DayOfWeek==2,'Tue',
                                                          ifelse(flighttest$DayOfWeek==3,'Wed',
                                                                 ifelse(flighttest$DayOfWeek==4,'Thu',
                                                                        ifelse(flighttest$DayOfWeek==5,'Fri',
                                                                               ifelse(flighttest$DayOfWeek==6,'Sat',
                                                                                      'Sun'))))))
flighttest$DayOfWeek<-as.factor(flighttest$DayOfWeek) # Converting to factor

flighttest$Month<-ifelse(flighttest$Month==1,'Jan',ifelse(flighttest$Month==2,'Feb',ifelse(flighttest$Month==3,'Mar',
                                                                               ifelse(flighttest$Month==4,'Apr',
                                                                                      ifelse(flighttest$Month==5,'May',
                                                                                             ifelse(flighttest$Month==6,'Jun',
                                                                                                    ifelse(flighttest$Month==7,'Jul',
                                                                                                           ifelse(flighttest$Month==8,'Aug',
                                                                                                                  ifelse(flighttest$Month==9,'Sep',
                                                                                                                         ifelse(flighttest$Month==10,'Oct',
                                                                                                                                ifelse(flighttest$Month==11,'Nov','Dec')))))))))))
flighttest$Month<-as.factor(flighttest$Month) # Converting to factor

str(flighttest) # Checking structure of revised data frame

# Loading the hpd data

hpdTest<-readRDS("hpdTest.rds")
hpdTest$TimeSlot<-as.factor(hpdTest$TimeSlot)

str(hpdTest)

# Merging with flight data first by origin & then by destination

flighttest<-sqldf('select a.*, b.OrigPrecip from flighttest a left join (select distinct AirportID, YearMonthDay, TimeSlot,
              OrigPrecip from hpdTest) b on a.Origin = b.AirportID and a.SchedDate = b.YearMonthDay
              and a.SchDepSlot = b.TimeSlot')

flighttest<-sqldf('select a.*, b.DestPrecip from flighttest a left join (select distinct AirportID, YearMonthDay, TimeSlot,
              OrigPrecip as DestPrecip from hpdTest) b on a.Destination = b.AirportID and a.SchedDate = b.YearMonthDay
              and a.SchArrSlot = b.TimeSlot')

rm(hpdTest) # Freeing up memory

# Loading hly data

hlyTest<-readRDS("hlyTest.rds")
hlyTest$TimeSlot<-as.factor(hlyTest$TimeSlot)

str(hlyTest)

# Merging with flight data first by origin & then by destination

flighttest<-sqldf('select a.*, b.OrigSkyCond, b.OrigVis, b.OrigDBT, b.OrigDewPtTemp, b.OrigRelHumPerc,
              b.OrigWindSp, b.OrigWindDir, b.OrigWindGustVal, b.OrigStnPres from flighttest a left join
              (select distinct AirportID, YearMonthDay, TimeSlot, OrigSkyCond, OrigVis, OrigDBT,
              OrigDewPtTemp, OrigRelHumPerc, OrigWindSp, OrigWindDir, OrigWindGustVal, OrigStnPres
              from hlyTest) b on a.Origin = b.AirportID and a.SchedDate = b.YearMonthDay
              and a.SchDepSlot = b.TimeSlot')

flighttest<-sqldf('select a.*, b.DestSkyCond, b.DestVis, b.DestDBT, b.DestDewPtTemp, b.DestRelHumPerc,
              b.DestWindSp, b.DestWindDir, b.DestWindGustVal, b.DestStnPres from flighttest a left join
              (select distinct AirportID, YearMonthDay, TimeSlot, OrigSkyCond as DestSkyCond, OrigVis as DestVis, OrigDBT as DestDBT,
              OrigDewPtTemp as DestDewPtTemp, OrigRelHumPerc as DestRelHumPerc, OrigWindSp as DestWindSp, OrigWindDir as DestWindDir, OrigWindGustVal as DestWindGustVal, OrigStnPres as DestStnPres
              from hlyTest) b on a.Destination = b.AirportID and a.SchedDate = b.YearMonthDay
              and a.SchArrSlot = b.TimeSlot')

rm(hlyTest) # Freeing up memory

saveRDS(flighttest, file = "flighttest.rds")