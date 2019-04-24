rm(list=ls()) # Clearing environment

# Read Train & Test Pre-Processed Files

flight<-readRDS("flight.rds")
flight<-flight[,c(1:9,11:37,10)]
flighttest<-readRDS("flighttest.rds")

# Read in 2004 Holiday List

library(lubridate)
hols04<-read.csv('holiday2004.csv',header = TRUE)
str(hols04)
hols04<-hols04[hols04$Type=="Federal Holiday",] # Selecting only federal holidays
hols04$Date<- as.POSIXct(hols04$Date, format="%m/%d/%Y",tz = 'GMT')

# Read in 2004 Holiday List

hols05<-read.csv('holiday2005.csv',header = TRUE)
str(hols05)
hols05<-hols05[hols05$Type=="Federal Holiday",] # Selecting only federal holidays
hols05$Date<- as.POSIXct(hols05$Date, format="%m/%d/%Y",tz = 'GMT')

# Pre-processing

str(flight) # Check structure

row.names(flight)<-flight$FlightNumber # Assign row names
flight$FlightNumber<-NULL # Remove column with unique values

row.names(flighttest)<-flighttest$FlightNumber # Assign row names
flighttest$FlightNumber<-NULL # Remove column with unique values

# Deriving column days from nearest holiday for 2004

x<-1:nrow(flight) # Calculating number of rows
y<-1:nrow(hols04) # Calculating number of rows

flight$nrsthol<-365
flight$nrsthol<-as.integer(flight$nrsthol)
flight$SchedDate<- as.POSIXct(flight$SchedDate, format="%Y-%m-%d",tz = 'GMT')

for (i in seq_along(x)) {
  for (j in seq_along(y)) {
    temp1<-flight[i,c('nrsthol')]
    temp2<-difftime(flight[i,c('SchedDate')], hols04[j,c('Date')],units = "days")
    temp2<-abs(as.integer(temp2))
    temp1<-ifelse(temp2<temp1,temp2,temp1)
    flight[i,c('nrsthol')]<-temp1
  }
}

rm(hols04,i,j,temp1,temp2,x,y) # Remove unrequired variables

# Deriving column days from nearest holiday for 2005

x<-1:nrow(flighttest) # Calculating number of rows
y<-1:nrow(hols05) # Calculating number of rows

flighttest$nrsthol<-365
flighttest$nrsthol<-as.integer(flighttest$nrsthol)
flighttest$SchedDate<- as.POSIXct(flighttest$SchedDate, format="%Y-%m-%d",tz = 'GMT')

for (i in seq_along(x)) {
  for (j in seq_along(y)) {
    temp1<-flighttest[i,c('nrsthol')]
    temp2<-difftime(flighttest[i,c('SchedDate')], hols05[j,c('Date')],units = "days")
    temp2<-abs(as.integer(temp2))
    temp1<-ifelse(temp2<temp1,temp2,temp1)
    flighttest[i,c('nrsthol')]<-temp1
  }
}

rm(hols05,i,j,temp1,temp2,x,y) # Remove unrequired variables

boxplot(flight$nrsthol~flight$FlightDelayStatus) # Checking variation of nrstlol by DelayStatus

flight$DayofMonth<-NULL # Removing column
flighttest$DayofMonth<-NULL # Removing column

flight$SchedDate<-NULL # Removing column
flighttest$SchedDate<-NULL # Removing column

flight$TAILNUM<-NULL
flighttest$TAILNUM<-NULL

flight$FLNUM<-NULL
flighttest$FLNUM<-NULL

flight$Direction<-NULL
flighttest$Direction<-NULL

flight$Type<-as.factor(flight$Type)
flighttest$Type<-as.factor(flighttest$Type)

# Check NULL Values

library(DMwR)

temp<-flight # Assigning to temporary variable
rem<-manyNAs(flight) # Identifying rows with high number of null values
temp<-flight[-rem,]
flight<-temp # Re-assigning
rm(temp,rem) # Removing unrequired variables

colSums(is.na(flight)) # A few NA values can still be seen
flight<-centralImputation(flight) # Using Central Imputation
colSums(is.na(flighttest)) # A few NA values can still be seen
flighttest<-centralImputation(flighttest)

# Re-ordering columns

flight<-flight[,c(1:30,32,31)]

# Reducing number of levels in Origin/Destination

'%!in%' <- function(x,y)!('%in%'(x,y))
flight$Origin<-as.character(flight$Origin) # Changing to character
flight$Origin<-ifelse(flight$Origin %!in% c('ATL','ORD','DFW','CVG'),'Others',flight$Origin) # Reducing Orig Levels
flight$Origin<-as.factor(flight$Origin) # Changing back to factor
flight$Destination<-as.character(flight$Destination) # Changing to character
flight$Destination<-ifelse(flight$Destination %!in% c('ATL','CVG','ORD','DFW','EWR','IAH'),'Others',
                           flight$Destination) # Reducing Dest Levels
flight$Destination<-as.factor(flight$Destination) # Changing back to factor

flighttest$Origin<-as.character(flighttest$Origin) # Changing to character
flighttest$Origin<-ifelse(flighttest$Origin %!in% c('ATL','ORD','DFW','CVG'),'Others',flighttest$Origin) # Reducing Orig Levels
flighttest$Origin<-as.factor(flighttest$Origin) # Changing back to factor
flighttest$Destination<-as.character(flighttest$Destination) # Changing to character
flighttest$Destination<-ifelse(flighttest$Destination %!in% c('ATL','CVG','ORD','DFW','EWR','IAH'),'Others',flighttest$Destination) # Reducing Dest Levels
flighttest$Destination<-as.factor(flighttest$Destination) # Changing back to factor

# Reducing number of levels in TimeSlots

flight$SchDepSlot<-as.character(flight$SchDepSlot) # Changing to character
flight$SchDepSlot<-ifelse(flight$SchDepSlot %!in% c('4PM to 6PM','Noon to 2PM','2PM to 4PM',
                                                    '10AM to Noon','8AM to 10AM','6AM to 8AM'),
                          'Others',flight$SchDepSlot) # Reducing Orig Levels
flight$SchDepSlot<-as.factor(flight$SchDepSlot) # Changing back to factor
flight$SchArrSlot<-as.character(flight$SchArrSlot) # Changing to character
flight$SchArrSlot<-ifelse(flight$SchArrSlot %!in% c('4PM to 6PM','6PM to 8PM','2PM to 4PM',
                                                    'Noon to 2PM','10AM to Noon','8AM to 10AM'),
                          'Others',flight$SchArrSlot) # Reducing Orig Levels
flight$SchArrSlot<-as.factor(flight$SchArrSlot) # Changing back to factor

flighttest$SchDepSlot<-as.character(flighttest$SchDepSlot) # Changing to character
flighttest$SchDepSlot<-ifelse(flighttest$SchDepSlot %!in% c('4PM to 6PM','Noon to 2PM','2PM to 4PM',
                                                            '10AM to Noon','8AM to 10AM','6AM to 8AM'),
                              'Others',flighttest$SchDepSlot) # Reducing Orig Levels
flighttest$SchDepSlot<-as.factor(flighttest$SchDepSlot) # Changing back to factor
flighttest$SchArrSlot<-as.character(flighttest$SchArrSlot) # Changing to character
flighttest$SchArrSlot<-ifelse(flighttest$SchArrSlot %!in% c('4PM to 6PM','6PM to 8PM','2PM to 4PM',
                                                            'Noon to 2PM','10AM to Noon','8AM to 10AM'),
                              'Others',flighttest$SchArrSlot) # Reducing Orig Levels
flighttest$SchArrSlot<-as.factor(flighttest$SchArrSlot) # Changing back to factor

# Reducing the number of levels in Carriers

flight$CARRIER<-as.character(flight$CARRIER) # Changing to character
flight$CARRIER<-ifelse(flight$CARRIER %!in% c('OH','EV','AA','DL','NW','RU','WN'),'Others',flight$CARRIER) # Reducing Orig Levels
flight$CARRIER<-as.factor(flight$CARRIER) # Changing back to factor

flighttest$CARRIER<-as.character(flighttest$CARRIER) # Changing to character
flighttest$CARRIER<-ifelse(flighttest$CARRIER %!in% c('OH','EV','AA','DL','NW','RU','WN'),'Others',flighttest$CARRIER) # Reducing Orig Levels
flighttest$CARRIER<-as.factor(flighttest$CARRIER) # Changing back to factor

# Train - Validation Split

library(caret)

set.seed(123)

train_rows<-createDataPartition(flight$FlightDelayStatus,p = 0.7, list = F) # Stratified sampling for train rows

train_data<-flight[train_rows,]

validation_data<-flight[-train_rows,]

rm(train_rows) ## Remove the temporary variable train_rows

# Standardization

str(flight)

std_obj <- preProcess(x = train_data[, !colnames(train_data) %in% c("FlightDelayStatus")],
                      method = c("center", "scale")) # Creating standardizing object

train_std_data <- predict(std_obj, train_data) # Standardizing train & storing in new variable

val_std_data <- predict(std_obj, validation_data) # Standardizing val & storing in new variable

test_std_data <- predict(std_obj,flighttest) # Standardizing test & storing in new variable

# Dummification on Train

train_std_data$FlightDelayStatus<-as.integer(train_std_data$FlightDelayStatus)

dummy_obj<-dummyVars(~.,train_std_data,fullRank = T)

train_dummy_data<-as.data.frame(predict(dummy_obj,train_std_data))

train_std_data$FlightDelayStatus<-as.character(train_std_data$FlightDelayStatus)
train_std_data$FlightDelayStatus<-ifelse(train_std_data$FlightDelayStatus=='1','0','1')
train_std_data$FlightDelayStatus<-as.factor(train_std_data$FlightDelayStatus)

train_dummy_data$FlightDelayStatus<-as.character(train_dummy_data$FlightDelayStatus)
train_dummy_data$FlightDelayStatus<-ifelse(train_dummy_data$FlightDelayStatus=='1','0','1')
train_dummy_data$FlightDelayStatus<-as.factor(train_dummy_data$FlightDelayStatus)

# Dummification on Validation

val_std_data$FlightDelayStatus<-as.integer(val_std_data$FlightDelayStatus)

val_dummy_data<-as.data.frame(predict(dummy_obj,val_std_data))

val_std_data$FlightDelayStatus<-as.character(val_std_data$FlightDelayStatus)
val_std_data$FlightDelayStatus<-ifelse(val_std_data$FlightDelayStatus=='1','0','1')
val_std_data$FlightDelayStatus<-as.factor(val_std_data$FlightDelayStatus)

val_dummy_data$FlightDelayStatus<-as.character(val_dummy_data$FlightDelayStatus)
val_dummy_data$FlightDelayStatus<-ifelse(val_dummy_data$FlightDelayStatus=='1','0','1')
val_dummy_data$FlightDelayStatus<-as.factor(val_dummy_data$FlightDelayStatus)

outcomeName<-'FlightDelayStatus'

# Parameter tuning

fitControl <- trainControl(
  method = "repeatedcv",
  number = 5,
  repeats = 5)

# Predictors obtained from previous rfe exercise

predictors<-c('OrigVis','OrigSkyCond','DestRelHumPerc','DestVis','OrigRelHumPerc','DestSkyCond',
              'OrigDewPtTemp','OrigPrecip','OrigStnPres','OrigDBT','DestDewPtTemp','DestPrecip',
              'DestDBT','Distance','nrsthol','Origin.Others')

# Modeling using ADABoost

model_adaTL<-train(train_dummy_data[,predictors],train_dummy_data[,outcomeName],method='ada',
                   trControl=fitControl,tuneLength=10,metric = "Kappa")

plot(model_adaTL)

print(model_adaTL)

preds_adaTL<-predict(model_adaTL,val_dummy_data) # Predicting on Val Data

confusionMatrix(preds_adaTL,val_dummy_data$FlightDelayStatus, positive = "1")