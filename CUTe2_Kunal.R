## Set Working Directory to Relevant Folder##

incgrp<-read.csv("train_data.csv",header = T) ## Read train data into variable incgrp

## Basic data analysis ##

str(incgrp) ## Checking data structure

summary(incgrp) ## Summarizing data

## Preliminary Observations & Changes ##

## 1 - Loan taken is integer. Can be classified as Factor

incgrp$loan_taken<-as.factor(incgrp$loan_taken)

## 2 - Tax Paid has 92.4% NAs. Imputation not recommended. Column can be removed.

rem<-c("tax_paid") ## Assigning columns to be removed to variable 'rem'

validcols <- setdiff(x=colnames(incgrp),y=rem) ## Getting list of valid columns

incgrptemp<-incgrp[,validcols] ##Removing column(s) in variable 'rem' and assigning to temp variable

incgrp<-incgrptemp ## Assigning temp values to original variable

rm(incgrptemp) ## Remove temp variable

rm(rem) ## Remove variable 'rem' since it is not required anymore

rm(validcols) ## Remove variable 'validcols' since it is not required anymore

## The variable index can be moved to rownames ##

rownames(incgrp)<-incgrp$index ## Assigning values in variable index to rownames

incgrp$index<-NULL ## Converting index to NULL & hence removing it

## Checking Data Again ##

str(incgrp)

summary(incgrp)

## Feature Engineering - Qualification ##

table(incgrp$qualification) ## Checking frequency by levels

## Order of education level - Preschool < 1st-4th < 5th-6th < 7th-8th < 9th < 10th < 11th < 12th 
##< HS-grad < Prof-school < Assoc-acdm < Assoc-voc < Some-college < Bachelors < Masters < Doctorate


table(incgrp$target,incgrp$qualification) ## Checking count of target value by qualification

## From the table above, it can be deduced that there is a lower proportion of High Income Grp
## people when education level < HS-Grad

## Therefore, combining everything before HS - Grad into one category "LeftStudy"

incgrptemp<-incgrp ## Assigning incgrp to temp variable incgrptemp

table(incgrptemp$qualification) ## Frequency table of all columns

incgrptemp$qualification<-as.character(incgrptemp$qualification) ## Changing to character

incgrptemp$qualification <- gsub("Preschool","LeftStudy",incgrptemp$qualification,fixed = TRUE)
incgrptemp$qualification <- gsub("10th","LeftStudy",incgrptemp$qualification,fixed = TRUE)
incgrptemp$qualification <- gsub("11th","LeftStudy",incgrptemp$qualification,fixed = TRUE)
incgrptemp$qualification <- gsub("12th","LeftStudy",incgrptemp$qualification,fixed = TRUE)
incgrptemp$qualification <- gsub("1st-4th","LeftStudy",incgrptemp$qualification,fixed = TRUE)
incgrptemp$qualification <- gsub("5th-6th","LeftStudy",incgrptemp$qualification,fixed = TRUE)
incgrptemp$qualification <- gsub("7th-8th","LeftStudy",incgrptemp$qualification,fixed = TRUE)
incgrptemp$qualification <- gsub("9th","LeftStudy",incgrptemp$qualification,fixed = TRUE)

table(incgrptemp$qualification,incgrptemp$target) ## Assoc-acdm & Assoc-voc have similar 0,1 proportion

incgrptemp$qualification<-gsub("Assoc-acdm","Associate",incgrptemp$qualification,fixed = TRUE)
incgrptemp$qualification<-gsub("Assoc-voc","Associate",incgrptemp$qualification,fixed = TRUE)

table(incgrptemp$qualification,incgrptemp$target)

incgrptemp$qualification<-as.factor(incgrptemp$qualification) ## Changing back to factor

incgrp<-incgrptemp ## Reassigning incgrptemp values to incgrp

rm(incgrptemp) ## Removing temp variable

## Feature Engineering Working Sector ##

table(incgrp$working_sector,incgrp$target) ## Frequency table of target by working sector

## Not worked and Without Pay categories can be merged since they show similar behavior

incgrptemp<-incgrp ## Creating temp variable with all data from incgrp

incgrptemp$working_sector<-as.character(incgrptemp$working_sector) ## Changing to character

incgrptemp$working_sector<-gsub("without_pay","not_worked",incgrptemp$working_sector,fixed = TRUE)

incgrptemp$working_sector<-as.factor(incgrptemp$working_sector) ## Changing back to factor

incgrp<-incgrptemp ## Reassigning incgrptemp values to incgrp

rm(incgrptemp) ## Removing temp variable

## Feature Engineering Marital Status ##

table(incgrp$marital_status,incgrp$target)

incgrptemp<-incgrp ## Assigning values of incgrp to incgrptemp

incgrptemp$marital_status<-as.character(incgrptemp$marital_status) ## Changing to character

## Married-civilian & Married-defence have similar target proportion

incgrptemp$marital_status<-gsub("Married-civilian","Married-resident",incgrptemp$marital_status,fixed = TRUE)
incgrptemp$marital_status<-gsub("Married-defence","Married-resident",incgrptemp$marital_status,fixed = TRUE)

## Separated & Widowed have similar target proportion so can be combined within separated

incgrptemp$marital_status<-gsub("Widowed","Separated",incgrptemp$marital_status,fixed = TRUE)

table(incgrptemp$marital_status,incgrptemp$target)

incgrptemp$marital_status<-as.factor(incgrptemp$marital_status) ## Changing back to factor

incgrp<-incgrptemp ## Reassigning incgrptemp values to incgrp

rm(incgrptemp) ## Removing temp variable incgrptemp

## Feature Engineering Occupation ##

table(incgrp$occupation,incgrp$target)

incgrptemp<-incgrp ## Assigning values to temp variable incgrptemp

incgrptemp$occupation<-as.character(incgrptemp$occupation) ## Changing to character

## cleaner, house servant & other have similar target proportion

incgrptemp$occupation<-gsub("cleaner","lowest",incgrptemp$occupation,fixed = TRUE)
incgrptemp$occupation<-gsub("house_servant","lowest",incgrptemp$occupation,fixed = TRUE)
incgrptemp$occupation<-gsub("other","lowest",incgrptemp$occupation,fixed = TRUE)

## clerical, defence, farming & inspector have similar target proportion

incgrptemp$occupation<-gsub("clerical","low",incgrptemp$occupation,fixed = TRUE)
incgrptemp$occupation<-gsub("defence","low",incgrptemp$occupation,fixed = TRUE)
incgrptemp$occupation<-gsub("farming","low",incgrptemp$occupation,fixed = TRUE)
incgrptemp$occupation<-gsub("inspector","low",incgrptemp$occupation,fixed = TRUE)

## sales can be renamed to lower mid to maintain consistency

incgrptemp$occupation<-gsub("sales","lower mid",incgrptemp$occupation,fixed = TRUE)

## repair & transport have similar target proportion

incgrptemp$occupation<-gsub("repair","mid",incgrptemp$occupation,fixed = TRUE)
incgrptemp$occupation<-gsub("transport","mid",incgrptemp$occupation,fixed = TRUE)

## guard & support have similar target proportion

incgrptemp$occupation<-gsub("guard","upper mid",incgrptemp$occupation,fixed = TRUE)
incgrptemp$occupation<-gsub("support","upper mid",incgrptemp$occupation,fixed = TRUE)

## managerial & specialty have similar target proportion

incgrptemp$occupation<-gsub("managerial","upper",incgrptemp$occupation,fixed = TRUE)
incgrptemp$occupation<-gsub("specialty","upper",incgrptemp$occupation,fixed = TRUE)

incgrptemp$occupation<-as.factor(incgrptemp$occupation) ## Changing back to factor

incgrp<-incgrptemp ## Reassigning values

rm(incgrptemp) ## Removing temp variable incgrptemp

## Feature Engineering for Relationship ##

table(incgrp$relationship,incgrp$target)
incgrptemp<-incgrp ## Assigning values to a temp variable
incgrptemp$relationship<-as.character(incgrptemp$relationship) ## Changing to character

## Husband & wife can be combined. Other can be combined based on target proportion

incgrptemp$relationship<-gsub("Husband","Spouse",incgrptemp$relationship,fixed = TRUE)
incgrptemp$relationship<-gsub("Wife","Spouse",incgrptemp$relationship,fixed = TRUE)
incgrptemp$relationship<-gsub("Not-in-family","Other",incgrptemp$relationship,fixed = TRUE)
incgrptemp$relationship<-gsub("Other-relative","Other",incgrptemp$relationship,fixed = TRUE)
incgrptemp$relationship<-gsub("Own-child","Other",incgrptemp$relationship,fixed = TRUE)
incgrptemp$relationship<-gsub("Unmarried","Other",incgrptemp$relationship,fixed = TRUE)

incgrptemp$relationship<-as.factor(incgrptemp$relationship) ## Changing back to factor

incgrp<-incgrptemp ## Reassigning values

rm(incgrptemp) ## Removing temp variable

## Feature Engineering Ethnicity ##

table(incgrp$ethnicity,incgrp$target)
incgrptemp<-incgrp ## Assigning values to a temp variable
incgrptemp$ethnicity<-as.character(incgrptemp$ethnicity) ## Converting to character values

## Amer - Indian - Eskimo, Black & Other can be combined based on target proportion

incgrptemp$ethnicity<-gsub("Amer-Indian-Eskimo","Bk",incgrptemp$ethnicity,fixed = TRUE)
incgrptemp$ethnicity<-gsub("Black","Bk",incgrptemp$ethnicity,fixed = TRUE)
incgrptemp$ethnicity<-gsub("Other","Bk",incgrptemp$ethnicity,fixed = TRUE)

## Asian-Pac-Islander and White can be combined based on target proportion

incgrptemp$ethnicity<-gsub("Asian-Pac-Islander","Wh",incgrptemp$ethnicity,fixed = TRUE)
incgrptemp$ethnicity<-gsub("White","Wh",incgrptemp$ethnicity,fixed = TRUE)

table(incgrptemp$ethnicity,incgrptemp$target)

incgrptemp$ethnicity<-as.factor(incgrptemp$ethnicity) ## Changing back to Factor

incgrp<-incgrptemp ## Reassigning values

rm(incgrptemp) ## Removing temp variable

## Feature Engineering Country ##

table(incgrp$country,incgrp$target)

incgrptemp<-incgrp ## Assigning value to a temp variable
incgrptemp$country<-as.character(incgrptemp$country) ## Converting to character values

## Very Low as per target proportion

incgrptemp$country<-gsub("Columbia","Very Low",incgrptemp$country,fixed = TRUE)
incgrptemp$country<-gsub("Dominican-Republic","Very Low",incgrptemp$country,fixed = TRUE)
incgrptemp$country<-gsub("El-Salvador","Very Low",incgrptemp$country,fixed = TRUE)
incgrptemp$country<-gsub("Guatemala","Very Low",incgrptemp$country,fixed = TRUE)
incgrptemp$country<-gsub("Haiti","Very Low",incgrptemp$country,fixed = TRUE)
incgrptemp$country<-gsub("Holand-Netherlands","Very Low",incgrptemp$country,fixed = TRUE)
incgrptemp$country<-gsub("Honduras","Very Low",incgrptemp$country,fixed = TRUE)
incgrptemp$country<-gsub("Jamaica","Very Low",incgrptemp$country,fixed = TRUE)
incgrptemp$country<-gsub("Laos","Very Low",incgrptemp$country,fixed = TRUE)
incgrptemp$country<-gsub("Mexico","Very Low",incgrptemp$country,fixed = TRUE)
incgrptemp$country<-gsub("Nicaragua","Very Low",incgrptemp$country,fixed = TRUE)
incgrptemp$country<-gsub("Outlying-US(Guam-USVI-etc)","Very Low",incgrptemp$country,fixed = TRUE)
incgrptemp$country<-gsub("Peru","Very Low",incgrptemp$country,fixed = TRUE)
incgrptemp$country<-gsub("Portugal","Very Low",incgrptemp$country,fixed = TRUE)
incgrptemp$country<-gsub("Puerto-Rico","Very Low",incgrptemp$country,fixed = TRUE)
incgrptemp$country<-gsub("Trinadad&Tobago","Very Low",incgrptemp$country,fixed = TRUE)
incgrptemp$country<-gsub("Vietnam","Very Low",incgrptemp$country,fixed = TRUE)

## Low as per target proportion

incgrptemp$country<-gsub("Ecuador","Low",incgrptemp$country,fixed = TRUE)
incgrptemp$country<-gsub("Hungary","Low",incgrptemp$country,fixed = TRUE)
incgrptemp$country<-gsub("Ireland","Low",incgrptemp$country,fixed = TRUE)
incgrptemp$country<-gsub("Poland","Low",incgrptemp$country,fixed = TRUE)
incgrptemp$country<-gsub("Scotland","Low",incgrptemp$country,fixed = TRUE)
incgrptemp$country<-gsub("South","Low",incgrptemp$country,fixed = TRUE)
incgrptemp$country<-gsub("Thailand","Low",incgrptemp$country,fixed = TRUE)

## Medium as per target proportion

incgrptemp$country<-gsub("China","Medium",incgrptemp$country,fixed = TRUE)
incgrptemp$country<-gsub("Cuba","Medium",incgrptemp$country,fixed = TRUE)
incgrptemp$country<-gsub("Greece","Medium",incgrptemp$country,fixed = TRUE)
incgrptemp$country<-gsub("United-States","Medium",incgrptemp$country,fixed = TRUE)

## High as per target proportion

incgrptemp$country<-gsub("Cambodia","High",incgrptemp$country,fixed = TRUE)
incgrptemp$country<-gsub("Canada","High",incgrptemp$country,fixed = TRUE)
incgrptemp$country<-gsub("England","High",incgrptemp$country,fixed = TRUE)
incgrptemp$country<-gsub("Germany","High",incgrptemp$country,fixed = TRUE)
incgrptemp$country<-gsub("Hong","High",incgrptemp$country,fixed = TRUE)
incgrptemp$country<-gsub("Italy","High",incgrptemp$country,fixed = TRUE)
incgrptemp$country<-gsub("Japan","High",incgrptemp$country,fixed = TRUE)
incgrptemp$country<-gsub("Philippines","High",incgrptemp$country,fixed = TRUE)

## Very High as per target proportion

incgrptemp$country<-gsub("France","Very High",incgrptemp$country,fixed = TRUE)
incgrptemp$country<-gsub("India","Very High",incgrptemp$country,fixed = TRUE)
incgrptemp$country<-gsub("Iran","Very High",incgrptemp$country,fixed = TRUE)
incgrptemp$country<-gsub("Taiwan","Very High",incgrptemp$country,fixed = TRUE)
incgrptemp$country<-gsub("Yugoslavia","Very High",incgrptemp$country,fixed = TRUE)

table(incgrptemp$country,incgrptemp$target)

incgrptemp$country<-as.factor(incgrptemp$country) ## Converting back to factor

incgrp<-incgrptemp ## Reassigning back all values

rm(incgrptemp) ## Removing temp variable

## Checking data again

str(incgrp)

summary(incgrp)

## Missing Value Imputation ##

sum(is.na(incgrp))

colSums(is.na(incgrp))

incgrptemp<-incgrp ## Assigning to temp variable

manyNAs(incgrptemp) ## Returns rows 31561, 31572

incgrptemp<-incgrptemp[-c(31561,31572),] ## Removing two rows

library(DMwR)

colSums(is.na(incgrptemp)) ## NA values in working_sector, occupation & country

incgrptemp =  centralImputation(data = incgrptemp)

colSums(is.na(incgrptemp)) ## NA values imputed

incgrp<-incgrptemp ## Reassigning values

rm(incgrptemp) ## Removing temp variable

## Train Validation Split ##

set.seed(123)

train_rows<-createDataPartition(incgrp$target,p = 0.7, list = F)

train_data<-incgrp[train_rows,]

validation_data<-incgrp[-train_rows,]

rm(train_rows) ## Remove the variable train_rows

rm(incgrp) ## Remove the variable incgrp since it is not required anymore

## Build a Model ##

log_reg<-glm(target~.,data=train_data,family = binomial)

summary(log_reg)

## Check for collinearity

library(car)

vif(log_reg)

## The highest adjusted GVIF is of years of education @ 5.92

rem<-c("years_of_education")

validcols <- setdiff(x=colnames(train_data),y=rem) ## Getting list of valid columns

traintemp<-train_data[,validcols] ##Removing column(s) in variable 'rem' and assigning to temp variable

train_data<-traintemp ## Assigning temp values to original variable

rm(traintemp) ## Remove temp variable

validationtemp<-validation_data[,validcols] ## Removing column(s) in variable 'rem'

validation_data<-validationtemp ## Assigning temp values to original variable

rm(validationtemp) ## Removing temp variable

rm(rem) ## Removing temp variable

rm(validcols) ## Removing temp variable

## Building model - 2nd iteration

log_reg2<-glm(target~.,data=train_data,family = binomial)

summary(log_reg2)

## Check for collinearity & removing variables

vif(log_reg2) ## Highest adjusted GVIF for relationship @ 5.32

rem<-c("relationship")

validcols <- setdiff(x=colnames(train_data),y=rem) ## Getting list of valid columns

traintemp<-train_data[,validcols] ##Removing column(s) in variable 'rem' and assigning to temp variable

train_data<-traintemp ## Assigning temp values to original variable

rm(traintemp) ## Remove temp variable

validationtemp<-validation_data[,validcols] ## Removing column(s) in variable 'rem'

validation_data<-validationtemp ## Assigning temp values to original variable

rm(validationtemp) ## Removing temp variable

rm(rem) ## Removing temp variable

rm(validcols) ## Removing temp variable

## Building model - 3rd iteration

log_reg3<-glm(target~.,data=train_data,family = binomial)

summary(log_reg3)

## Check for collinearity & removing variables

vif(log_reg3) ## All adjusted GVIF values look good

## Creating ROC plot on the 3rd iteration model ##

prob_train<-predict(log_reg3,type="response")

library(ROCR)

pred<-prediction(prob_train,train_data$target)

perf<-performance(pred,measure = "tpr", x.measure = "fpr")

plot(perf, col = rainbow(10), colorize = T, print.cutoffs.at = seq(0,1,0.05))

perf_auc<-performance(pred,measure = "auc")

auc<-perf_auc@y.values[[1]]

print(auc) ## AUC comes out to be 0.902

## Choosing cutoff point at 0.5 based on iteration & Predicting on Validation Data

prob_validation <- predict(log_reg3,validation_data,type = "response")

preds_validation <- ifelse(prob_validation>0.5,1,0)

## Confusion Matrix & Manual Computation

validation_data_labs<-validation_data$target

conf_matrix<-table(validation_data_labs,preds_validation)

print(conf_matrix)

## Specificity

specificity <- conf_matrix[1, 1]/sum(conf_matrix[1, ])

print(specificity) ## Specificity at 0.936

## Sensitivity

sensitivity <- conf_matrix[2, 2]/sum(conf_matrix[2, ])

print(sensitivity) ## Sensitivity at 0.583

## Accuracy

accuracy <- sum(diag(conf_matrix))/sum(conf_matrix)

print(accuracy) ## Accuracy at 0.8503 on validation

## Implementing the model on Test Data ##

test_data<-read.csv("test_data.csv",header = T)

## Removing variables

rem<-c("tax_paid","years_of_education","relationship")

validcols <- setdiff(x=colnames(test_data),y=rem) ## Getting list of valid columns

testdatatemp<-test_data[,validcols] ##Removing column(s) in variable 'rem' and assigning to temp variable

test_data<-testdatatemp ## Assigning temp values to original variable

rm(testdatatemp) ## Remove temp variable

rm(rem) ## Remove variable 'rem' since it is not required anymore

rm(validcols) ## Remove variable 'validcols' since it is not required anymore

## Converting loan taken to factor

test_data$loan_taken<-as.factor(test_data$loan_taken)

## Index can be moved to rownames

rownames(test_data)<-test_data$index ## Assigning values in variable index to rownames

test_data$index<-NULL ## Converting index to NULL & hence removing it

## Checking remainder data

str(test_data)

## Modifying remaining variables as applicable to reduce levels

## Feature Engineering Working Sector ##

testdatatemp<-test_data ## Creating temp variable with all data

testdatatemp$working_sector<-as.character(testdatatemp$working_sector) ## Changing to character

testdatatemp$working_sector<-gsub("without_pay","not_worked",testdatatemp$working_sector,fixed = TRUE)

testdatatemp$working_sector<-as.factor(testdatatemp$working_sector) ## Changing back to factor

test_data<-testdatatemp ## Reassigning

rm(testdatatemp) ## Removing temp variable

## Feature Engineering Qualification

testdatatemp<-test_data ## Assigning to temp variable

testdatatemp$qualification<-as.character(testdatatemp$qualification) ## Changing to character

testdatatemp$qualification <- gsub("Preschool","LeftStudy",testdatatemp$qualification,fixed = TRUE)
testdatatemp$qualification <- gsub("10th","LeftStudy",testdatatemp$qualification,fixed = TRUE)
testdatatemp$qualification <- gsub("11th","LeftStudy",testdatatemp$qualification,fixed = TRUE)
testdatatemp$qualification <- gsub("12th","LeftStudy",testdatatemp$qualification,fixed = TRUE)
testdatatemp$qualification <- gsub("1st-4th","LeftStudy",testdatatemp$qualification,fixed = TRUE)
testdatatemp$qualification <- gsub("5th-6th","LeftStudy",testdatatemp$qualification,fixed = TRUE)
testdatatemp$qualification <- gsub("7th-8th","LeftStudy",testdatatemp$qualification,fixed = TRUE)
testdatatemp$qualification <- gsub("9th","LeftStudy",testdatatemp$qualification,fixed = TRUE)

testdatatemp$qualification<-gsub("Assoc-acdm","Associate",testdatatemp$qualification,fixed = TRUE)
testdatatemp$qualification<-gsub("Assoc-voc","Associate",testdatatemp$qualification,fixed = TRUE)

testdatatemp$qualification<-as.factor(testdatatemp$qualification) ## Changing back to factor

test_data<-testdatatemp ## Reassigning

rm(testdatatemp) ## Removing temp variable

## Feature Engineering Marital Status ##

testdatatemp<-test_data ## Assigning to temp variable

testdatatemp$marital_status<-as.character(testdatatemp$marital_status) ## Changing to character

testdatatemp$marital_status<-gsub("Married-civilian","Married-resident",testdatatemp$marital_status,fixed = TRUE)
testdatatemp$marital_status<-gsub("Married-defence","Married-resident",testdatatemp$marital_status,fixed = TRUE)

testdatatemp$marital_status<-gsub("Widowed","Separated",testdatatemp$marital_status,fixed = TRUE)

testdatatemp$marital_status<-as.factor(testdatatemp$marital_status) ## Changing back to factor

test_data<-testdatatemp ## Reassigning

rm(testdatatemp) ## Removing temp variable

## Feature Engineering Ethnicity ##

testdatatemp<-test_data ## Assigning values to a temp variable
testdatatemp$ethnicity<-as.character(testdatatemp$ethnicity) ## Converting to character values

testdatatemp$ethnicity<-gsub("Amer-Indian-Eskimo","Bk",testdatatemp$ethnicity,fixed = TRUE)
testdatatemp$ethnicity<-gsub("Black","Bk",testdatatemp$ethnicity,fixed = TRUE)
testdatatemp$ethnicity<-gsub("Other","Bk",testdatatemp$ethnicity,fixed = TRUE)

testdatatemp$ethnicity<-gsub("Asian-Pac-Islander","Wh",testdatatemp$ethnicity,fixed = TRUE)
testdatatemp$ethnicity<-gsub("White","Wh",testdatatemp$ethnicity,fixed = TRUE)

testdatatemp$ethnicity<-as.factor(testdatatemp$ethnicity) ## Changing back to Factor

test_data<-testdatatemp ## Reassigning values

rm(testdatatemp) ## Removing temp variable

## Feature Engineering Country ##

testdatatemp<-test_data ## Assigning value to a temp variable
testdatatemp$country<-as.character(testdatatemp$country) ## Converting to character values

testdatatemp$country<-gsub("Columbia","Very Low",testdatatemp$country,fixed = TRUE)

testdatatemp$country<-gsub("Dominican-Republic","Very Low",testdatatemp$country,fixed = TRUE)
testdatatemp$country<-gsub("El-Salvador","Very Low",testdatatemp$country,fixed = TRUE)
testdatatemp$country<-gsub("Guatemala","Very Low",testdatatemp$country,fixed = TRUE)
testdatatemp$country<-gsub("Haiti","Very Low",testdatatemp$country,fixed = TRUE)
testdatatemp$country<-gsub("Holand-Netherlands","Very Low",testdatatemp$country,fixed = TRUE)
testdatatemp$country<-gsub("Honduras","Very Low",testdatatemp$country,fixed = TRUE)
testdatatemp$country<-gsub("Jamaica","Very Low",testdatatemp$country,fixed = TRUE)
testdatatemp$country<-gsub("Laos","Very Low",testdatatemp$country,fixed = TRUE)
testdatatemp$country<-gsub("Mexico","Very Low",testdatatemp$country,fixed = TRUE)
testdatatemp$country<-gsub("Nicaragua","Very Low",testdatatemp$country,fixed = TRUE)
testdatatemp$country<-gsub("Outlying-US(Guam-USVI-etc)","Very Low",testdatatemp$country,fixed = TRUE)
testdatatemp$country<-gsub("Peru","Very Low",testdatatemp$country,fixed = TRUE)
testdatatemp$country<-gsub("Portugal","Very Low",testdatatemp$country,fixed = TRUE)
testdatatemp$country<-gsub("Puerto-Rico","Very Low",testdatatemp$country,fixed = TRUE)
testdatatemp$country<-gsub("Trinadad&Tobago","Very Low",testdatatemp$country,fixed = TRUE)
testdatatemp$country<-gsub("Vietnam","Very Low",testdatatemp$country,fixed = TRUE)

testdatatemp$country<-gsub("Ecuador","Low",testdatatemp$country,fixed = TRUE)
testdatatemp$country<-gsub("Hungary","Low",testdatatemp$country,fixed = TRUE)
testdatatemp$country<-gsub("Ireland","Low",testdatatemp$country,fixed = TRUE)
testdatatemp$country<-gsub("Poland","Low",testdatatemp$country,fixed = TRUE)
testdatatemp$country<-gsub("Scotland","Low",testdatatemp$country,fixed = TRUE)
testdatatemp$country<-gsub("South","Low",testdatatemp$country,fixed = TRUE)
testdatatemp$country<-gsub("Thailand","Low",testdatatemp$country,fixed = TRUE)

testdatatemp$country<-gsub("China","Medium",testdatatemp$country,fixed = TRUE)
testdatatemp$country<-gsub("Cuba","Medium",testdatatemp$country,fixed = TRUE)
testdatatemp$country<-gsub("Greece","Medium",testdatatemp$country,fixed = TRUE)
testdatatemp$country<-gsub("United-States","Medium",testdatatemp$country,fixed = TRUE)

testdatatemp$country<-gsub("Cambodia","High",testdatatemp$country,fixed = TRUE)
testdatatemp$country<-gsub("Canada","High",testdatatemp$country,fixed = TRUE)
testdatatemp$country<-gsub("England","High",testdatatemp$country,fixed = TRUE)
testdatatemp$country<-gsub("Germany","High",testdatatemp$country,fixed = TRUE)
testdatatemp$country<-gsub("Hong","High",testdatatemp$country,fixed = TRUE)
testdatatemp$country<-gsub("Italy","High",testdatatemp$country,fixed = TRUE)
testdatatemp$country<-gsub("Japan","High",testdatatemp$country,fixed = TRUE)
testdatatemp$country<-gsub("Philippines","High",testdatatemp$country,fixed = TRUE)

testdatatemp$country<-gsub("France","Very High",testdatatemp$country,fixed = TRUE)
testdatatemp$country<-gsub("India","Very High",testdatatemp$country,fixed = TRUE)
testdatatemp$country<-gsub("Iran","Very High",testdatatemp$country,fixed = TRUE)
testdatatemp$country<-gsub("Taiwan","Very High",testdatatemp$country,fixed = TRUE)
testdatatemp$country<-gsub("Yugoslavia","Very High",testdatatemp$country,fixed = TRUE)

testdatatemp$country<-as.factor(testdatatemp$country) ## Converting back to factor

test_data<-testdatatemp ## Reassigning back all values

rm(testdatatemp) ## Removing temp variable

## Feature Engineering Occupation ##

testdatatemp<-test_data ## Assigning values to temp variable incgrptemp

testdatatemp$occupation<-as.character(testdatatemp$occupation) ## Changing to character

## cleaner, house servant & other have similar target proportion

testdatatemp$occupation<-gsub("cleaner","lowest",testdatatemp$occupation,fixed = TRUE)
testdatatemp$occupation<-gsub("house_servant","lowest",testdatatemp$occupation,fixed = TRUE)
testdatatemp$occupation<-gsub("other","lowest",testdatatemp$occupation,fixed = TRUE)

## clerical, defence, farming & inspector have similar target proportion

testdatatemp$occupation<-gsub("clerical","low",testdatatemp$occupation,fixed = TRUE)
testdatatemp$occupation<-gsub("defence","low",testdatatemp$occupation,fixed = TRUE)
testdatatemp$occupation<-gsub("farming","low",testdatatemp$occupation,fixed = TRUE)
testdatatemp$occupation<-gsub("inspector","low",testdatatemp$occupation,fixed = TRUE)

## sales can be renamed to lower mid to maintain consistency

testdatatemp$occupation<-gsub("sales","lower mid",testdatatemp$occupation,fixed = TRUE)

## repair & transport have similar target proportion

testdatatemp$occupation<-gsub("repair","mid",testdatatemp$occupation,fixed = TRUE)
testdatatemp$occupation<-gsub("transport","mid",testdatatemp$occupation,fixed = TRUE)

## guard & support have similar target proportion

testdatatemp$occupation<-gsub("guard","upper mid",testdatatemp$occupation,fixed = TRUE)
testdatatemp$occupation<-gsub("support","upper mid",testdatatemp$occupation,fixed = TRUE)

## managerial & specialty have similar target proportion

testdatatemp$occupation<-gsub("managerial","upper",testdatatemp$occupation,fixed = TRUE)
testdatatemp$occupation<-gsub("specialty","upper",testdatatemp$occupation,fixed = TRUE)

testdatatemp$occupation<-as.factor(testdatatemp$occupation) ## Changing back to factor

test_data<-testdatatemp ## Reassigning values

rm(testdatatemp) ## Removing temp variable incgrptemp


## Missing Value Imputation on Test ##

testdatatemp<-test_data ## Assigning to temp variable

colSums(is.na(testdatatemp))

testdatatemp<-centralImputation(data=testdatatemp)

test_data<-testdatatemp ## Reassigning

rm(testdatatemp) ## Removing Temp Variable

sum(is.na(test_data)) ## No null values remaining

## Prediction on Test Data ##

prob_test<-predict(log_reg3,test_data,type="response")

preds_test<-ifelse(prob_test>0.5,1,0)

## Write CSV

write.csv(preds_test,file="Kunal_Ray_TestPred.csv") ## Accuracy is 84.02%