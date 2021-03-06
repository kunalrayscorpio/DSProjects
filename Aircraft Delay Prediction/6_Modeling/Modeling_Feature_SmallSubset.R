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

# Creating DewPtTemp DBT Ratio

flight$OrigDPTDBTRat<-flight$OrigDewPtTemp/flight$OrigDBT
flight$DestPTDBTRat<-flight$DestDewPtTemp/flight$DestDBT

flighttest$OrigDPTDBTRat<-flighttest$OrigDewPtTemp/flighttest$OrigDBT
flighttest$DestPTDBTRat<-flighttest$DestDewPtTemp/flighttest$DestDBT

# Calculating MaxDewPtTempDBTRatio

flight$DEwPTTempDBTRatMax<-apply(flight[,c('OrigDPTDBTRat','DestPTDBTRat')],1,max)
flighttest$DEwPTTempDBTRatMax<-apply(flighttest[,c('OrigDPTDBTRat','DestPTDBTRat')],1,max)

flight$OrigDPTDBTRat<-NULL
flight$DestPTDBTRat<-NULL
flighttest$OrigDPTDBTRat<-NULL
flighttest$DestPTDBTRat<-NULL

# Calculating Density Altitude

flight$OrigDBTF<-flight$OrigDBT*1.8+32
flight$DestDBTF<-flight$DestDBT*1.8+32

flighttest$OrigDBTF<-flighttest$OrigDBT*1.8+32
flighttest$DestDBTF<-flighttest$DestDBT*1.8+32

flight$OrigDA<-145442.16*(1-((17.326*flight$OrigStnPres)/(459.67+flight$OrigDBTF))^0.235)
flight$DestDA<-145442.16*(1-((17.326*flight$DestStnPres)/(459.67+flight$DestDBTF))^0.235)

flighttest$OrigDA<-145442.16*(1-((17.326*flighttest$OrigStnPres)/(459.67+flighttest$OrigDBTF))^0.235)
flighttest$DestDA<-145442.16*(1-((17.326*flighttest$DestStnPres)/(459.67+flighttest$DestDBTF))^0.235)

flight$MaxDA<-apply(flight[,c('OrigDA','DestDA')],1,max)
flighttest$MaxDA<-apply(flighttest[,c('OrigDA','DestDA')],1,max)

flight$OrigDBTF<-NULL
flight$DestDBTF<-NULL
flighttest$OrigDBTF<-NULL
flighttest$DestDBTF<-NULL
flight$OrigDA<-NULL
flight$DestDA<-NULL
flighttest$OrigDA<-NULL
flighttest$DestDA<-NULL

# Re-ordering columns

flight<-flight[,c(1:30,32:34,31)]

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

# Trying Logistic Regression with Step AIC - Attempt #1

log_reg1<-glm(FlightDelayStatus~.,data=train_std_data,family = binomial) # Build the model

summary(log_reg1) # Generate model summary

library(MASS)

aic_model1<-stepAIC(log_reg1, direction = "both", trace = TRUE) # Run Step AIC on Model

library(car)

vif(aic_model1) # Check for Multi-colinearity

# Need to remove the variables ScheduledTravelTime & Distance

# Trying Logistic Regression with Step AIC - Attempt #2

log_reg2<-glm(FlightDelayStatus~. - Distance - ScheduledTravelTime,data=train_std_data,
              family = binomial) # Build the model

summary(log_reg2) # Generate model summary

aic_model2<-stepAIC(log_reg2, direction = "both", trace = TRUE) # Run Step AIC on Model

vif(aic_model2) # All multi-colinearities resolved

# Creating ROC Plot on the final model

prob_train<-predict(aic_model2,type="response")

library(ROCR)

pred<-prediction(prob_train,train_std_data$FlightDelayStatus)

perf<-performance(pred,measure = "tpr", x.measure = "fpr")

plot(perf, col = rainbow(10), colorize = T, print.cutoffs.at = seq(0,1,0.05))

perf_auc<-performance(pred,measure = "auc")

auc<-perf_auc@y.values[[1]]

print(auc)

# Iteratively checking performance & maximizing F1 on Train Data

preds_train<-ifelse(prob_train>0.4,1,0) # Get train data predicted values

train_data_labs<-train_std_data$FlightDelayStatus # Getting train data target values

train_conf_matrix<-table(train_data_labs,preds_train) # Generate confusion matrix on Train

print(train_conf_matrix)

# Train Accuracy

tr_accuracy <- sum(diag(train_conf_matrix))/sum(train_conf_matrix) # Calculate train accuracy

print(tr_accuracy) # 0.8409

# Train Sensitivity

tr_sens<-train_conf_matrix[2, 2]/sum(train_conf_matrix[2, ])

tr_sens # 0.4777

# Train Precision

tr_pr<-train_conf_matrix[2, 2]/sum(train_conf_matrix[, 2])

tr_pr # 0.6034

# Train F1 Score

tr_f1<-(2*tr_sens*tr_pr)/(tr_sens+tr_pr)

tr_f1 # 0.533

# Check validation data performance

prob_val <- predict(aic_model2,val_std_data,type = "response")

preds_val <- ifelse(prob_val>0.4,1,0)

val_data_labs<-val_std_data$FlightDelayStatus # Getting train data target values

val_conf_matrix<-table(val_data_labs,preds_val) # Generate confusion matrix on Train

print(val_conf_matrix)

# Val Accuracy

val_accuracy <- sum(diag(val_conf_matrix))/sum(val_conf_matrix) # Calculate train accuracy

print(val_accuracy) # 0.8343

# Train Sensitivity

val_sens<-val_conf_matrix[2, 2]/sum(val_conf_matrix[2, ])

val_sens # 0.4819

# Train Precision

val_pr<-val_conf_matrix[2, 2]/sum(val_conf_matrix[, 2])

val_pr # 0.577

# Train F1 Score

val_f1<-(2*val_sens*val_pr)/(val_sens+val_pr)

val_f1 # 0.525

# Predict on Test Data

prob_test <- predict(aic_model2,test_std_data,type = "response")

preds_test <- ifelse(prob_test>0.4,'Yes','No')

rm(aic_model1,auc,log_reg1,perf,perf_auc,pred,preds_test,preds_train,preds_val,prob_test,prob_train,
   prob_val,tr_accuracy,tr_f1,tr_pr,tr_sens,train_conf_matrix,train_data_labs,val_accuracy,
   val_conf_matrix,val_data_labs,val_f1,val_pr,val_sens) # Remove unrequired variables

# Feature Selection using CARET

# Dummifying Train, Validation & Test

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

#Feature selection using rfe in caret

control <- rfeControl(functions = rfFuncs,
                      method = "repeatedcv",
                      repeats = 3,
                      verbose = FALSE)

outcomeName<-'FlightDelayStatus'

predictors<-names(train_dummy_data)[!names(train_dummy_data) %in% outcomeName]

Delay_Pred_Profile <- rfe(train_dummy_data[,predictors], train_dummy_data[,outcomeName],
                         rfeControl = control, metric = "Kappa")

Delay_Pred_Profile$optVariables

# Top 5<- OrigVis, OrigSkyCond, DestVis, DEwPTTempDBTRatMax, DestRelHumPerc

predictors<-c('OrigVis','OrigSkyCond','DestVis','DEwPTTempDBTRatMax','DestRelHumPerc')

# Parameter tuning

fitControl <- trainControl(
  method = "repeatedcv",
  number = 5,
  repeats = 5)

# Trying C5

model_C5TL<-train(train_dummy_data[,predictors],train_dummy_data[,outcomeName],method='C5.0',
                  trControl=fitControl,tuneLength=10,metric = "Kappa")

preds_c5<-predict(model_C5TL,val_dummy_data) # Predicting on Val Data

confusionMatrix(preds_c5,val_dummy_data$FlightDelayStatus, positive = "1")

plot(model_C5TL)

# Trying RF

model_rfTL<-train(train_dummy_data[,predictors],train_dummy_data[,outcomeName],method='rf',
                  trControl=fitControl,tuneLength=10,metric = "Kappa")

plot(model_rfTL)

preds_rf<-predict(model_rfTL,val_dummy_data) # Predicting on Val Data

confusionMatrix(preds_rf,val_dummy_data$FlightDelayStatus, positive = "1")

# Trying ADABoost

model_adaTL<-train(train_dummy_data[,predictors],train_dummy_data[,outcomeName],method='ada',
                  trControl=fitControl,tuneLength=10,metric = "Kappa")

plot(model_adaTL)

preds_adaTL<-predict(model_adaTL,val_dummy_data) # Predicting on Val Data

confusionMatrix(preds_adaTL,val_dummy_data$FlightDelayStatus, positive = "1")