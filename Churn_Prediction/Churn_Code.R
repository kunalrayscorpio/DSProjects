### Clear Environment & Load Data

# par(mfrow=c(1,1))

library(caret)

rm(list = ls(all=TRUE))

churn<-read.csv("churndata.csv",header = T)

paste("No. of rows in original dataframe is ", nrow(churn))
paste("No. of columns in original dataframe is ", ncol(churn))
table(churn$Flag) # Checking count of Attrtion vs No Attrition

str(churn) ## Check structure

rem <- c('V1','V2','V3','V4','V5','V6') ## columns to be removed

validcols <- setdiff(x=colnames(churn),y=rem) ## Getting list of valid columns

temp<-churn[,validcols] ## Assigning valid data to temp variable

churn<-temp ## Re-assigning to original variable

rm(temp,rem,validcols) ## Removing temporary variables

paste("No. of rows in revised dataframe is ", nrow(churn))
paste("No. of columns in revised dataframe is ", ncol(churn))

## Moving variable Key to RowID

temp<-churn ## Assign data to temp variable

row.names(temp)<-temp$Key ## Assigning Key as row names

temp$Key<-NULL ## Removing Key3 from data frame

head(temp)

## Changing data types

str(temp)

temp$V7<-as.factor(temp$V7) # Converting to factor
temp$V8<-as.factor(temp$V8) # Converting to factor
temp$V9<- as.factor(temp$V9) # Converting to factor
temp$V10<-as.factor(temp$V10) # Converting to factor
temp$V11<-as.factor(temp$V11) # Converting to factor
temp$V12<-as.factor(temp$V12) # Converting to factor
temp$V13<-as.factor(temp$V13) # Converting to factor
temp$V14<-as.factor(temp$V14) # Converting to factor
temp$V15<-as.factor(temp$V15) # Converting to factor
temp$V16<-as.factor(temp$V16) # Converting to factor
temp$Flag<-as.factor(temp$Flag) # Converting to factor

temp$V17 <- as.character(temp$V17) # Converting to character
temp$V17<-as.numeric(temp$V17) # Converting to numeric

temp$V18<-as.character(temp$V18) # Converting to character
temp$V18<-ifelse(temp$V18=='Auto Renewal','Y','N')
temp$V18<-as.factor(temp$V18) # Re-convert to factor

str(temp)

## Checking for NA Values & Imputing

library(DMwR)

rem<-manyNAs(temp) ## Assign rows with high no. of NA values to variable rem --> None found

rm(rem) ## Remove temporary variable

colSums(is.na(temp)) ## Check for NA values

churn<-temp ## Re-assigning back to original variable churn

temp<-knnImputation(data = churn,k = 15) ## Doing KNN Imputation to fill missing values

colSums(is.na(temp)) ## All NA values imputed

churn<-temp ## Re-assigning values back from temp to churn

rm(temp) ## Removing temp variable

## Taking out out of sample validation rows

test_data<-subset(churn,Qtr=='Q1-19')

train_data<-subset(churn,Qtr=='Q1-18'|Qtr=='Q2-18'|Qtr=='Q3-18'|Qtr=='Q4-18')

## Dropping Qtr from all data sets

churn$Qtr<-NULL
test_data$Qtr<-NULL
train_data$Qtr<-NULL

## Standardization

std_obj <- preProcess(x = train_data[, !colnames(train_data) %in% c("Flag")],
                      method = c("center", "scale")) # Creating standardizing object

train_std_data <- predict(std_obj, train_data) # Standardizing train & storing in new variable

test_std_data<- predict(std_obj, test_data) # Standardizing test & storing in new variable

head(train_std_data)

## Dummification

train_std_data$Flag <- as.integer(as.character(train_std_data$Flag))

test_std_data$Flag <- as.integer(as.character(test_std_data$Flag))

dummy_obj <- dummyVars(~.,train_std_data)

train_dummy_data <- as.data.frame(predict(dummy_obj,train_std_data))

test_dummy_data <- as.data.frame(predict(dummy_obj,test_std_data))

train_std_data$Flag <- as.factor(as.character(train_std_data$Flag))
train_dummy_data$Flag <- as.factor(as.character(train_dummy_data$Flag))
test_std_data$Flag<-as.factor(as.character(test_std_data$Flag))
test_dummy_data$Flag<-as.factor(as.character(test_dummy_data$Flag))

## Modeling using Logistic Regression with Step AIC

train_dummy_data$Flag <- as.factor(ifelse(train_dummy_data$Flag == "1",
                                          "Attrited","Not Attrited")) # Renaming levels

levels(train_dummy_data$Flag) <- make.names(levels(factor(train_dummy_data$Flag)))

objControl <- trainControl(method = "repeatedcv", number = 2, returnResamp = 'final', 
                           summaryFunction = twoClassSummary, repeats = 4,
                           classProbs = TRUE,
                           savePredictions = TRUE) # Describe object control


set.seed(766)

GLMCaretModel <- train(train_dummy_data[,1:41],
                       train_dummy_data[,42], 
                       method = 'glmStepAIC',
                       direction = "both",
                       trControl = objControl,
                       metric = "ROC",
                       verbose = FALSE) # Build model

## Model Evaluation

GLMCaretModel

plot(varImp(GLMCaretModel, scale = TRUE)) # Plotting important predictors

## Performance on Train Data

caretTrainPredictedClass <- predict(object = GLMCaretModel, train_dummy_data[,1:41],
                                    type = 'raw') # Predict on Train Data

confusionMatrix(caretTrainPredictedClass,train_dummy_data$Flag,positive = "Attrited") # Build conf matrix

## Deriving optimal cutoff value

# Creating empty vectors to store the results

msclaf.cost <- c()
youden.index <- c()
cutoff <- c()
P00 <- c() #correct classification of positive as positive
P11 <- c() #correct classification of negative as negative
P01 <- c() #misclassification of positive class to negative class
P10 <- c() #misclassification of negative class to positive class

costs = matrix(c(0,1,1.5, 0), ncol = 2) # Assigning cost of mis-classification

colnames(costs) = rownames(costs) = c("Not Attrited", "Attrited")

as.table(costs)

## Calculating probability on train data & mis - classification cost table

GLMCaretTrainPredictedProbability = predict(object = GLMCaretModel, 
                                            train_dummy_data[,1:41], type = 'prob') # Predict on train data

GLMCaretTestPredictedProbability = predict(object = GLMCaretModel, 
                                           test_dummy_data[,1:41], type = 'prob') # Predict on test data

n <- length(train_dummy_data$Flag)

## The misclassification cost table is

for (i in seq(0.05, 1, .05)) {
  predicted.y = rep("NotAttrited", n)
  predicted.y[GLMCaretTrainPredictedProbability[1] > i] = "Attrited"
  tbl <- table(train_dummy_data$Flag, predicted.y)
  if ( i <= 1) {
    #Classifying Attrited as Not Attrited
    P01[20*i] <- tbl[3]/(tbl[1] + tbl[3])
    
    #Classifying Attrited as Attrited
    P00[20*i] <- tbl[1]/(tbl[1] + tbl[3])
    
    #Classifying Not Attrited as Not Attrited
    P11[20*i] <- tbl[4]/(tbl[2] + tbl[4])
    
    #Classifying Not Attrited as Attrited
    P10[20*i] <- tbl[2]/(tbl[2] + tbl[4])
    
    cutoff[20*i] <- i
    msclaf.cost[20*i] <- P01[20*i]*costs[2] + P10[20*i]*costs[3]
    youden.index[20*i] <- P00[20*i] + P11[20*i] - 1
  }
}

df.cost.table <- cbind(cutoff,P10,P01,msclaf.cost, P11, P00, youden.index)

df.cost.table

stack_train <- GLMCaretTrainPredictedProbability
stack_train$Logistic<-ifelse(stack_train$Attrited>0.30,1,0)
stack_train$Logistic<-as.character(stack_train$Logistic)
stack_train$Logistic<-as.factor(stack_train$Logistic)
stack_train$Attrited<-NULL
stack_train$Not.Attrited<-NULL

stack_test <- GLMCaretTestPredictedProbability
stack_test$Logistic<-ifelse(stack_test$Attrited>0.30,1,0)
stack_test$Logistic<-as.character(stack_test$Logistic)
stack_test$Logistic<-as.factor(stack_test$Logistic)
stack_test$Attrited<-NULL
stack_test$Not.Attrited<-NULL

## Remove unrequired variables

rm(costs,df.cost.table,GLMCaretModel,GLMCaretTestPredictedProbability,GLMCaretTrainPredictedProbability,
   objControl,caretTrainPredictedClass,cutoff,i,msclaf.cost,n,P00,P01,P10,P11,predicted.y,tbl,youden.index)

#table(stack_train$Logistic)
#table(stack_test$Logistic)

## Modeling using Decision Trees - RPART

objControl <- trainControl(method = "repeatedcv", number = 2, repeats = 4) # Setting object control

set.seed(123)

grid <- expand.grid(cp=seq(0.0001,1,0.01)) # Defining extent of grid

library(rpart)

RPCaretModel <- train(train_data[,1:21],
                      train_data[,22], 
                      method = 'rpart',
                      trControl = objControl,
                      tuneGrid = grid) # Building model

## Check important variables

plot(RPCaretModel)

plot(varImp(RPCaretModel, scale = TRUE))

## Performance on Train Data

caretTrainPredictedClass <- predict(object = RPCaretModel, train_data[,1:21],
                                    type = 'prob') # Predict on Train Data

## Predicting on Test Data

caretTestPredictedClass <- predict(object = RPCaretModel, test_data[,1:21],
                                   type = 'prob') # Predict on Test Data

caretTrainPredictedClass$Rpart <- ifelse(caretTrainPredictedClass$`1`>0.5,1,0)
caretTestPredictedClass$Rpart <- ifelse(caretTestPredictedClass$`1`>0.5,1,0)

stack_train<-cbind(stack_train,caretTrainPredictedClass)
stack_test<-cbind(stack_test,caretTestPredictedClass)

stack_train$`0`<-NULL
stack_train$`1`<-NULL
stack_test$`0`<-NULL
stack_test$`1`<-NULL

## Removing unrequired variables

rm(caretTrainPredictedClass,caretTestPredictedClass,grid,objControl,RPCaretModel)

## Modeling using Decision Trees - C5.0

objControl <- trainControl(method = "repeatedcv", number = 2, repeats = 4) # Describe object control

grid <- expand.grid(.winnow=c(TRUE,FALSE),.trials=c(1,3,5,7,9,11,13,15,17),
                    .model="tree") # Describe grid search

library(C50)

set.seed(123)

C5CaretModel <- train(train_data[,1:21],
                      train_data[,22], 
                      method = 'C5.0',
                      trControl = objControl,
                      tuneGrid = grid) # Build model

## Check model features & important variables

plot(C5CaretModel)

plot(varImp(C5CaretModel, scale = TRUE))

## Predicting on Data

caretTrainPredictedClass <- predict(object = C5CaretModel, train_data[,1:21],
                                    type = 'prob') # Predict on Test Data

caretTestPredictedClass <- predict(object = C5CaretModel, test_data[,1:21],
                                   type = 'prob') # Predict on Test Data

caretTrainPredictedClass$C5 <- ifelse(caretTrainPredictedClass$`1`>0.5,1,0)
caretTestPredictedClass$C5 <- ifelse(caretTestPredictedClass$`1`>0.5,1,0)

stack_train<-cbind(stack_train,caretTrainPredictedClass)
stack_test<-cbind(stack_test,caretTestPredictedClass)

stack_train$`0`<-NULL
stack_train$`1`<-NULL
stack_test$`0`<-NULL
stack_test$`1`<-NULL

## Removing unrequired variables

rm(C5CaretModel,caretTestPredictedClass,caretTrainPredictedClass,grid,objControl)

## Modeling using Random Forest

library(randomForest)

objControl <- trainControl(method = "repeatedcv", number = 2, repeats = 4) # Describe object control

set.seed(123)

mtry <- tuneRF(train_data[-22],train_data$Flag, ntreeTry=500,stepFactor=1.5,improve=0.01, 
               trace=TRUE, plot=TRUE)

best.m <- mtry[mtry[, 2] == min(mtry[, 2]), 1]
print(mtry)
print(best.m)

grid <- expand.grid(.mtry=best.m)

RFCaretModel <- train(Flag~., 
                      data=train_data, 
                      method='rf', 
                      metric='Accuracy', 
                      tuneGrid=grid, 
                      trControl=objControl)

#plot(RFCaretModel)

plot(varImp(RFCaretModel, scale = TRUE))

## Predicting on Data

caretTrainPredictedClass <- predict(object = RFCaretModel, train_data[,1:21],
                                    type = 'prob') # Predict on Test Data

caretTestPredictedClass <- predict(object = RFCaretModel, test_data[,1:21],
                                   type = 'prob') # Predict on Test Data

caretTrainPredictedClass$RF <- ifelse(caretTrainPredictedClass$`1`>0.5,1,0)
caretTestPredictedClass$RF <- ifelse(caretTestPredictedClass$`1`>0.5,1,0)

stack_train<-cbind(stack_train,caretTrainPredictedClass)
stack_test<-cbind(stack_test,caretTestPredictedClass)

stack_train$`0`<-NULL
stack_train$`1`<-NULL
stack_test$`0`<-NULL
stack_test$`1`<-NULL

## Removing unrequired variables

rm(caretTestPredictedClass,caretTrainPredictedClass,grid,mtry,objControl,RFCaretModel,best.m)

## Trying SVM Linear

library(e1071)

objControl <- trainControl(method = "repeatedcv", number = 2, repeats = 4) # Describe object control

grid <- expand.grid(.C=c(10^-4, 10^-3, 10^-2, 10^-1, 10^1, 10^2, 10^3))

set.seed(123)

SVMLinCaretRough <- train(Flag~., 
                          data=train_dummy_data, 
                          method='svmLinear', 
                          metric='Accuracy', 
                          tuneGrid=grid, 
                          trControl=objControl)

SVMLinCaretRough$bestTune

rm(SVMLinCaretRough)

grid <- expand.grid(.C=c(0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1,2,3,4,5))

set.seed(123)

SVMLinCaretFine <- train(Flag~., 
                         data=train_dummy_data, 
                         method='svmLinear', 
                         metric='Accuracy', 
                         tuneGrid=grid, 
                         trControl=objControl)

SVMLinCaretFine

SVMCaretTrain <- predict(SVMLinCaretFine, train_dummy_data[,1:41], type='raw')
SVMCaretTest <- predict(SVMLinCaretFine, test_dummy_data[,1:41], type='raw')

SVMCaretTrain<- as.data.frame(SVMCaretTrain)
SVMCaretTest<- as.data.frame(SVMCaretTest)

SVMCaretTrain$SVMLin <- ifelse(SVMCaretTrain$SVMCaretTrain=='Attrited',1,0)
SVMCaretTest$SVMLin <- ifelse(SVMCaretTest$SVMCaretTest=='Attrited',1,0)

stack_train<-cbind(stack_train,SVMCaretTrain)
stack_test<-cbind(stack_test,SVMCaretTest)

stack_train$SVMCaretTrain <- NULL
stack_test$SVMCaretTest <- NULL

## Removing unrequired variables

rm(grid,objControl,SVMCaretTest,SVMCaretTrain,SVMLinCaretFine)

## Trying SVM Poly

objControl <- trainControl(method = "repeatedcv", number = 2, repeats = 4) # Describe object control

grid <- expand.grid(.C = c(10^-3, 10^-2.5, 10^-2.1, 10^-1.5, 10^-1.2, 10^-0.6), .degree = c(2, 3, 5), 
                    .scale = c(0.15, 0.25, 1))

set.seed(123)

SVMPolyCaret <- train(Flag~., 
                      data=train_dummy_data, 
                      method='svmPoly', 
                      metric='Accuracy', 
                      tuneGrid=grid, 
                      trControl=objControl)

SVMPolyCaret$bestTune

SVMPolyCaret

plot(SVMPolyCaret)

SVMCaretTrain <- predict(SVMPolyCaret, train_dummy_data[,1:41], type='raw')
SVMCaretTest <- predict(SVMPolyCaret, test_dummy_data[,1:41], type='raw')

SVMCaretTrain<- as.data.frame(SVMCaretTrain)
SVMCaretTest<- as.data.frame(SVMCaretTest)

SVMCaretTrain$SVMPoly <- ifelse(SVMCaretTrain$SVMCaretTrain=='Attrited',1,0)
SVMCaretTest$SVMPoly <- ifelse(SVMCaretTest$SVMCaretTest=='Attrited',1,0)

stack_train<-cbind(stack_train,SVMCaretTrain)
stack_test<-cbind(stack_test,SVMCaretTest)

stack_train$SVMCaretTrain <- NULL
stack_test$SVMCaretTest <- NULL

## Removing unrequired variables

rm(grid,objControl,SVMCaretTrain,SVMCaretTest,SVMPolyCaret)

## Trying ADA Boost

library(vegan)
library(ada)

objControl <- trainControl(method = "repeatedcv", number = 2, repeats = 4, verboseIter = T,
                           allowParallel = T) # Describe object control

grid <- expand.grid(iter = 150, nu = c(0.1,0.3,0.5,0.7), maxdepth = c(7,14,21,28))

set.seed(123)

AdaCaret <- train(Flag~., 
                  data=train_dummy_data, 
                  method='ada', 
                  metric='Accuracy', 
                  loss = "exponential", 
                  type = "discrete",
                  tuneGrid=grid, 
                  trControl=objControl)


AdaCaret$bestTune

AdaCaret

plot(AdaCaret)

AdaCaretTrain <- predict(AdaCaret, train_dummy_data[,1:41], type='raw')
AdaCaretTest <- predict(AdaCaret, test_dummy_data[,1:41], type='raw')

AdaCaretTrain<- as.data.frame(AdaCaretTrain)
AdaCaretTest<- as.data.frame(AdaCaretTest)

AdaCaretTrain$Ada <- ifelse(AdaCaretTrain$AdaCaretTrain=='Attrited',1,0)
AdaCaretTest$Ada <- ifelse(AdaCaretTest$AdaCaretTest=='Attrited',1,0)

stack_train<-cbind(stack_train,AdaCaretTrain)
stack_test<-cbind(stack_test,AdaCaretTest)

stack_train$AdaCaretTrain <- NULL
stack_test$AdaCaretTest <- NULL

## Removing unrequired variables

rm(AdaCaret,AdaCaretTest,AdaCaretTrain,grid,objControl,SVMLinCaretRough)

## Trying XGBoost

library(xgboost)

objControl <- trainControl(method = "repeatedcv", number = 2, repeats = 4, verboseIter = T,
                           allowParallel = T) # Describe object control

grid <- expand.grid(.nrounds = 20, .max_depth = c(2, 4, 6, 8), .eta = c(0.1, 0.3, 0.5),
                    .gamma = c(0.6, 0.5, 0.3, 0.1), .colsample_bytree = c(0.8, 0.6, 0.4),
                    .min_child_weight = 1, .subsample = c(0.5, 0.6, 0.9))

set.seed(123)

XGBCaret <- train(Flag~., 
                  data=train_std_data, 
                  method='xgbTree', 
                  metric='Accuracy',
                  tuneGrid=grid, 
                  trControl=objControl)

plot(XGBCaret)

XGBCaretTrain <- predict(XGBCaret, train_std_data[,1:21], type='raw')
XGBCaretTest <- predict(XGBCaret, test_std_data[,1:21], type='raw')

XGBCaretTrain<- as.data.frame(XGBCaretTrain)
XGBCaretTest<- as.data.frame(XGBCaretTest)

XGBCaretTrain$XGB <- ifelse(XGBCaretTrain$XGBCaretTrain=='Attrited',1,0)
XGBCaretTest$XGB <- ifelse(XGBCaretTest$XGBCaretTest=='Attrited',1,0)

stack_train<-cbind(stack_train,XGBCaretTrain)
stack_test<-cbind(stack_test,XGBCaretTest)

stack_train$XGBCaretTrain <- NULL
stack_test$XGBCaretTest <- NULL

## Removing unrequired variables

rm(grid,objControl,XGBCaret,XGBCaretTest,XGBCaretTrain)

## Adding dependent variables and modeling

Train_Flag <- as.data.frame(train_std_data$Flag)
Test_Flag <- as.data.frame(test_std_data$Flag)

stack_train <- cbind(stack_train,Train_Flag)
stack_test <- cbind(stack_test,Test_Flag)

names(stack_train)[names(stack_train)== "train_std_data$Flag"] <- "Flag"
names(stack_test)[names(stack_test)== "test_std_data$Flag"] <- "Flag"

rm(Train_Flag,Test_Flag)

stack_train$Logistic <- as.factor(as.character(stack_train$Logistic))
stack_train$Rpart <- as.factor(as.character(stack_train$Rpart))
stack_train$C5 <- as.factor(as.character(stack_train$C5))
stack_train$RF <- as.factor(as.character(stack_train$RF))
stack_train$SVMLin <- as.factor(as.character(stack_train$SVMLin))
stack_train$SVMPoly <- as.factor(as.character(stack_train$SVMPoly))
stack_train$Ada <- as.factor(as.character(stack_train$Ada))
stack_train$XGB <- as.factor(as.character(stack_train$XGB))

stack_test$Logistic <- as.factor(as.character(stack_test$Logistic))
stack_test$Rpart <- as.factor(as.character(stack_test$Rpart))
stack_test$C5 <- as.factor(as.character(stack_test$C5))
stack_test$RF <- as.factor(as.character(stack_test$RF))
stack_test$SVMLin <- as.factor(as.character(stack_test$SVMLin))
stack_test$SVMPoly <- as.factor(as.character(stack_test$SVMPoly))
stack_test$Ada <- as.factor(as.character(stack_test$Ada))
stack_test$XGB <- as.factor(as.character(stack_test$XGB))

objControl <- trainControl(method = "repeatedcv", number = 2, returnResamp = 'final', 
                           summaryFunction = twoClassSummary, repeats = 4,
                           classProbs = TRUE,
                           savePredictions = TRUE) # Describe object control


set.seed(123)

GLMCaretModel <- train(stack_train[,1:6],
                       stack_train[,7], 
                       method = 'glmStepAIC',
                       direction = "both",
                       trControl = objControl,
                       metric = "ROC",
                       verbose = FALSE) # Build model

## Model Evaluation

GLMCaretModel

plot(varImp(GLMCaretModel, scale = TRUE)) # Plotting important predictors

## Performance on Train Data

caretTrainPredictedClass <- predict(object = GLMCaretModel, stack_train[,1:6],
                                    type = 'raw') # Predict on Train Data

confusionMatrix(caretTrainPredictedClass,stack_train$Flag,positive = "Attrited") # Build conf matrix

## Deriving optimal cutoff value

# Creating empty vectors to store the results

msclaf.cost <- c()
youden.index <- c()
cutoff <- c()
P00 <- c() #correct classification of positive as positive
P11 <- c() #correct classification of negative as negative
P01 <- c() #misclassification of positive class to negative class
P10 <- c() #misclassification of negative class to positive class

costs = matrix(c(0,1,1.5, 0), ncol = 2) # Assigning cost of mis-classification

colnames(costs) = rownames(costs) = c("Not Attrited", "Attrited")

as.table(costs)

## Calculating probability on train data & mis - classification cost table

GLMCaretTrainPredictedProbability = predict(object = GLMCaretModel, 
                                            stack_train[,1:6], type = 'prob') # Predict on train data

GLMCaretTestPredictedProbability = predict(object = GLMCaretModel, 
                                           stack_test[,1:6], type = 'prob') # Predict on test data

n <- length(stack_train$Flag)

## The misclassification cost table is

for (i in seq(0.05, 1, .05)) {
  predicted.y = rep("NotAttrited", n)
  predicted.y[GLMCaretTrainPredictedProbability[1] > i] = "Attrited"
  tbl <- table(stack_train$Flag, predicted.y)
  if ( i <= 1) {
    #Classifying Attrited as Not Attrited
    P01[20*i] <- tbl[3]/(tbl[1] + tbl[3])
    
    #Classifying Attrited as Attrited
    P00[20*i] <- tbl[1]/(tbl[1] + tbl[3])
    
    #Classifying Not Attrited as Not Attrited
    P11[20*i] <- tbl[4]/(tbl[2] + tbl[4])
    
    #Classifying Not Attrited as Attrited
    P10[20*i] <- tbl[2]/(tbl[2] + tbl[4])
    
    cutoff[20*i] <- i
    msclaf.cost[20*i] <- P01[20*i]*costs[2] + P10[20*i]*costs[3]
    youden.index[20*i] <- P00[20*i] + P11[20*i] - 1
  }
}

df.cost.table <- cbind(cutoff,P10,P01,msclaf.cost, P11, P00, youden.index)

df.cost.table

GLMCaretTestPredictedProbability$Prediction <- ifelse(GLMCaretTestPredictedProbability$Attrited>0.25,
                                                      "Attrited","Not.Attrited")

write.csv(GLMCaretTestPredictedProbability,"Stack_Test.csv")