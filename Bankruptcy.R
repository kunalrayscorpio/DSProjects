## Working Directory Set ##

bankruptcy<-read.csv("train.csv",header = T)

## Preliminary Changes in DataSet ##

str(bankruptcy)

bankruptcy$target<-as.factor(bankruptcy$target) ## Converting target to factor

summary(bankruptcy)

rownames(bankruptcy)<-bankruptcy$ID ## Moving ID to rownames since it is unique

bankruptcy$ID <- NULL ## Removing ID from list of variables

library(DMwR)

NAS<-manyNAs(bankruptcy) ## 177 rows with high number of NA columns found

temp<-bankruptcy ## Assigning to temp variable

temp<-temp[-NAS,] ## ManyNAs rows removed

bankruptcy<-temp ## Reassigning values

rm(temp,NAS) ## Removing variables no longer required

summary(bankruptcy) ## Check for NAs

## Preliminary Data Analysis & Missing Value Imputation##

rownames(bankruptcy)[is.na(bankruptcy$Attr4)]

rownames(bankruptcy)[is.na(bankruptcy$Attr12)]

rownames(bankruptcy)[is.na(bankruptcy$Attr33)]

rownames(bankruptcy)[is.na(bankruptcy$Attr40)]

rownames(bankruptcy)[is.na(bankruptcy$Attr46)] ## Has 1 more than others

rownames(bankruptcy)[is.na(bankruptcy$Attr63)]

## Attr 4, 12, 33, 40, 46, 63 have NAs for the same rows. Each has 'short term liability' in

## denominator. Could be NA due to zero value of 'short term liability'.

## Attr4 current assets/short - term liabilities

## Attr 12 gross profit/short - term liabilities

## Attr33	operating expenses / short-term liabilities

## Attr40	(current assets - inventory - receivables) / short-term liabilities

## Attr46	(current assets - inventory) / short-term liabilities

## Attr63	sales / short-term liabilities

## If short term liability for these 6 columns is very low, attribute values should be high

## Attr4

boxplot(bankruptcy$Attr4) ## Outliers identified

bankruptcy$Attr4<-ifelse(bankruptcy$Attr4>2000,2000,bankruptcy$Attr4) ## Ceiling assigned

bankruptcy$Attr4<-ifelse(is.na(bankruptcy$Attr4),2000,bankruptcy$Attr4) ## NA values imputed

## Attr 12

boxplot(bankruptcy$Attr12) ## Outliers not identified but values can be both +ve or -ve

## Attr18	gross profit / total assets also has gross profit in numerator & no NAs left

## So when Attr 18 is +ve, Attr 12 should hit max & vice versa as 'total assets' > 0 always

mi<-min(bankruptcy$Attr12[!is.na(bankruptcy$Attr12)])

bankruptcy$Attr12<-ifelse(bankruptcy$Attr18<0 & is.na(bankruptcy$Attr12),
                          mi,bankruptcy$Attr12)

ma<-max(bankruptcy$Attr12[!is.na(bankruptcy$Attr12)])

bankruptcy$Attr12<-ifelse(bankruptcy$Attr18>=0 & is.na(bankruptcy$Attr12),
                          ma,bankruptcy$Attr12)

rm(mi,ma) ## Removing temp variables

## Attr 33

boxplot(bankruptcy$Attr33) ## Outliers identified

bankruptcy$Attr33<-ifelse(bankruptcy$Attr33>10000,10000,bankruptcy$Attr33) ## Ceiling assigned

bankruptcy$Attr33<-ifelse(is.na(bankruptcy$Attr33),
                          10000,bankruptcy$Attr33) ## NA values imputed

## Attr 40

boxplot(bankruptcy$Attr40) ## Outliers identified

bankruptcy$Attr40<-ifelse(bankruptcy$Attr40>2500,2500,bankruptcy$Attr40) ## Ceiling assigned

bankruptcy$Attr40<-ifelse(is.na(bankruptcy$Attr40),
                          2500,bankruptcy$Attr40) ## NA values imputed

## Attr 46

boxplot(bankruptcy$Attr46) ## Outliers identified

## Has 1 more NA value than others, so imputing only 37 cells instead of 38

## by using Attr 63 as reference

bankruptcy$Attr46<-ifelse(bankruptcy$Attr46>2500,2500,bankruptcy$Attr46) ## Ceiling assigned

ma<-max(bankruptcy$Attr46[!is.na(bankruptcy$Attr46)])

bankruptcy$Attr46<-ifelse(is.na(bankruptcy$Attr63) & is.na(bankruptcy$Attr46),
                          ma,bankruptcy$Attr46)

rm(ma) ## Removing temp variable

## Attr 63

boxplot(bankruptcy$Attr63) ## Outliers identified

bankruptcy$Attr63<-ifelse(bankruptcy$Attr63>5000,5000,bankruptcy$Attr63) ## Ceiling assigned

bankruptcy$Attr63<-ifelse(is.na(bankruptcy$Attr63),
                          5000,bankruptcy$Attr63) ## NA values imputed

summary(bankruptcy) ## Check for NAs

rownames(bankruptcy)[is.na(bankruptcy$Attr8)]

rownames(bankruptcy)[is.na(bankruptcy$Attr16)]

rownames(bankruptcy)[is.na(bankruptcy$Attr17)]

rownames(bankruptcy)[is.na(bankruptcy$Attr26)]

rownames(bankruptcy)[is.na(bankruptcy$Attr34)]

rownames(bankruptcy)[is.na(bankruptcy$Attr50)]

## Attr 8, 16, 17, 26, 34, 50 have NAs for the same rows. Each has 'total liabilities' in

## denominator. Could be NA due to zero value of 'total liabilities'.

## Attr8	book value of equity / total liabilities

## Attr16	(gross profit + depreciation) / total liabilities

## Attr17	total assets / total liabilities

## Attr26	(net profit + depreciation) / total liabilities

## Attr34	operating expenses / total liabilities

## Attr50	current assets / total liabilities

## If 'total liabilities' for these 6 columns is very low, attribute values should be high

## Attr 8

boxplot(bankruptcy$Attr8) ## Outliers identified

bankruptcy$Attr8<-ifelse(bankruptcy$Attr8>10000,10000,bankruptcy$Attr8) ## Ceiling assigned

bankruptcy$Attr8<-ifelse(is.na(bankruptcy$Attr8),
                          10000,bankruptcy$Attr8) ## NA values imputed

## Attr 16

boxplot(bankruptcy$Attr16) ## No outliers

ma<-max(bankruptcy$Attr16[!is.na(bankruptcy$Attr16)])

bankruptcy$Attr16<-ifelse(is.na(bankruptcy$Attr16),
                         ma,bankruptcy$Attr16) ## NA values imputed

rm(ma) ## Temp variable removed

## Attr 17

boxplot(bankruptcy$Attr17) ## Outliers identified

bankruptcy$Attr17<-ifelse(bankruptcy$Attr17>5000,5000,bankruptcy$Attr17) ## Ceiling assigned

bankruptcy$Attr17<-ifelse(is.na(bankruptcy$Attr17),
                         5000,bankruptcy$Attr17) ## NA values imputed

## Attr 26

boxplot(bankruptcy$Attr26) ## Outliers identified

bankruptcy$Attr26<-ifelse(bankruptcy$Attr26>1000,1000,bankruptcy$Attr26) ## Ceiling assigned

bankruptcy$Attr26<-ifelse(is.na(bankruptcy$Attr26),
                          1000,bankruptcy$Attr26) ## NA values imputed

## Attr 34

boxplot(bankruptcy$Attr34) ## Outliers identified

bankruptcy$Attr34<-ifelse(bankruptcy$Attr34>10000,10000,bankruptcy$Attr34) ## Ceiling assigned

bankruptcy$Attr34<-ifelse(is.na(bankruptcy$Attr34),
                          10000,bankruptcy$Attr34) ## NA values imputed

## Attr 50

boxplot(bankruptcy$Attr50) ## Outliers identified

bankruptcy$Attr50<-ifelse(bankruptcy$Attr50>1500,1500,bankruptcy$Attr50) ## Ceiling assigned

bankruptcy$Attr50<-ifelse(is.na(bankruptcy$Attr50),
                          1500,bankruptcy$Attr50) ## NA values imputed

summary(bankruptcy) ## Check for NAs

rownames(bankruptcy)[is.na(bankruptcy$Attr28)]

rownames(bankruptcy)[is.na(bankruptcy$Attr53)]

rownames(bankruptcy)[is.na(bankruptcy$Attr54)]

rownames(bankruptcy)[is.na(bankruptcy$Attr64)]

## Attr 28, 53, 54, 64 have NAs for the same rows. Each has 'fixed assets' in

## denominator. Could be NA due to zero value of 'fixed assets'.

## Attr28	working capital / fixed assets

## Attr53	equity / fixed assets

## Attr54	constant capital / fixed assets

## Attr64	sales / fixed assets

## If 'fixed assets' for these 4 columns is very low, attribute values should be high

## Attr 28

boxplot(bankruptcy$Attr28) ## Outliers identified

bankruptcy$Attr28<-ifelse(bankruptcy$Attr28>10000,10000,bankruptcy$Attr28) ## Ceiling assigned

bankruptcy$Attr28<-ifelse(is.na(bankruptcy$Attr28),
                          10000,bankruptcy$Attr28) ## NA values imputed

## Attr 53

boxplot(bankruptcy$Attr53) ## Outliers identified

bankruptcy$Attr53<-ifelse(bankruptcy$Attr53>30000,30000,bankruptcy$Attr53) ## Ceiling assigned

bankruptcy$Attr53<-ifelse(is.na(bankruptcy$Attr53),
                          30000,bankruptcy$Attr53) ## NA values imputed

## Attr 54

boxplot(bankruptcy$Attr54) ## Outliers identified

bankruptcy$Attr54<-ifelse(bankruptcy$Attr54>30000,30000,bankruptcy$Attr54) ## Ceiling assigned

bankruptcy$Attr54<-ifelse(is.na(bankruptcy$Attr54),
                          30000,bankruptcy$Attr54) ## NA values imputed

## Attr 64

boxplot(bankruptcy$Attr64) ## Outliers identified

bankruptcy$Attr64<-ifelse(bankruptcy$Attr64>10000,10000,bankruptcy$Attr64) ## Ceiling assigned

bankruptcy$Attr64<-ifelse(is.na(bankruptcy$Attr64),
                          10000,bankruptcy$Attr64) ## NA values imputed

colSums(is.na(bankruptcy)) ## Check for NAs

rownames(bankruptcy)[is.na(bankruptcy$Attr45)]

rownames(bankruptcy)[is.na(bankruptcy$Attr60)]

## Attr 45, 60 have NAs for the same rows +/- 5. Each has 'inventory' in

## denominator. Could be NA due to zero value of 'inventory'.

## Attr45	net profit / inventory

## Attr60	sales / inventory

## Attr 60

boxplot(bankruptcy$Attr60) ## Outliers identified

bankruptcy$Attr60<-ifelse(bankruptcy$Attr60>1000000,
                          1000000,bankruptcy$Attr60) ## Ceiling assigned

bankruptcy$Attr60<-ifelse(is.na(bankruptcy$Attr45) & is.na(bankruptcy$Attr60),
                          1000000,bankruptcy$Attr60) ## NA values imputed

## Attr 45

boxplot(bankruptcy$Attr45) ## Outliers identified & values both +ve & -ve

bankruptcy$Attr45<-ifelse(bankruptcy$Attr45>200000,
                          200000,bankruptcy$Attr45) ## Ceiling assigned

bankruptcy$Attr45<-ifelse(bankruptcy$Attr45<-100000,
                          -100000,bankruptcy$Attr45) ## Floor assigned

## 'Net profit' occurs in the numerator in Attr 1 as well

## So using Attr 1 to assign +ve or -ve values to Attr 45

bankruptcy$Attr45<-ifelse(bankruptcy$Attr1<0 & is.na(bankruptcy$Attr45),
                          -100000,bankruptcy$Attr45) ## NA values imputed

bankruptcy$Attr45<-ifelse(bankruptcy$Attr1>=0 & is.na(bankruptcy$Attr45),
                          200000,bankruptcy$Attr45) ## NA values imputed

colSums(is.na(bankruptcy)) ## Check NA values

rownames(bankruptcy)[is.na(bankruptcy$Attr32)]

rownames(bankruptcy)[is.na(bankruptcy$Attr47)]

rownames(bankruptcy)[is.na(bankruptcy$Attr52)]

## Attr 32, 47 & 52 have NAs for the same rows +/- 5. Each has 'cost of products sold' in

## denominator. Could be NA due to zero value of 'cost of products sold'.

## Attr32	(current liabilities * 365) / cost of products sold

## Attr47	(inventory * 365) / cost of products sold

## Attr52	(short-term liabilities * 365) / cost of products sold)

## Attr 32

boxplot(bankruptcy$Attr32) ## Outliers identified

bankruptcy$Attr32<-ifelse(bankruptcy$Attr32>5000000,
                          5000000,bankruptcy$Attr32) ## Ceiling assigned

bankruptcy$Attr32<-ifelse(is.na(bankruptcy$Attr47) & is.na(bankruptcy$Attr32),
                          5000000,bankruptcy$Attr32) ## Majority NA values imputed

## Attr 52

boxplot(bankruptcy$Attr52) ## Outliers identified

bankruptcy$Attr52<-ifelse(bankruptcy$Attr52>20000,
                          20000,bankruptcy$Attr52) ## Ceiling assigned

bankruptcy$Attr52<-ifelse(is.na(bankruptcy$Attr47) & is.na(bankruptcy$Attr52),
                          20000,bankruptcy$Attr52) ## Majority NA values imputed

## Attr 47

boxplot(bankruptcy$Attr47) ## Outliers identified

bankruptcy$Attr47<-ifelse(bankruptcy$Attr47>1000000,
                          1000000,bankruptcy$Attr47) ## Ceiling assigned

bankruptcy$Attr47<-ifelse(is.na(bankruptcy$Attr47),
                          1000000,bankruptcy$Attr47) ## NA values imputed

colSums(is.na(bankruptcy)) ## Check NA values

## Remaining Attributes having NA values are unrelated & need to be handled individually

## Attr 21 sales (n) / sales (n-1) has 4870 NAs

## It is possible that this is the first year of operation so sales (n-1) is zero.

## Can do Central Imputation here

temp<-bankruptcy ## Assigning to temp variable

temp$Attr21[is.na(temp$Attr21)]<-mean(!is.na(temp$Attr21))

bankruptcy<-temp ## Re - assigning

rm(temp) ## Removing temp variable

## Attr37	(current assets - inventories) / long-term liabilities has 15955 NAs

## Attr 37 has 'long term liabilities' in denominator

## Attr59	long-term liabilities / equity has 'long term liabilities' in numerator

## Attr 59 value is zero for every NA value in Attr 37

## This means that NAs are being caused due to zero 'long term liabilities'

boxplot(bankruptcy$Attr37) ## Outliers identified

bankruptcy$Attr37<-ifelse(bankruptcy$Attr37>200000,
                          200000,bankruptcy$Attr37) ## Ceiling assigned

bankruptcy$Attr37<-ifelse(is.na(bankruptcy$Attr37),
                          200000,bankruptcy$Attr37) ## NA values imputed

colSums(is.na(bankruptcy)) ## Check for NAs

## Attr 61 sales / receivables has 60 NAs

## Attr 61 has 'receivables' in denominator

## Attr 44 Attr44	(receivables * 365) / sales has 'receivables' in numerator as product

## Attr 44 value is zero for every NA value in Attr 61

## This means that NAs are being caused due to zero 'receivables'

boxplot(bankruptcy$Attr61) ## Outliers identified

bankruptcy$Attr61<-ifelse(bankruptcy$Attr61>40000,
                          40000,bankruptcy$Attr61) ## Ceiling assigned

bankruptcy$Attr61<-ifelse(is.na(bankruptcy$Attr61),
                          40000,bankruptcy$Attr61) ## NA values imputed

## Attr41	total liabilities / ((profit on operating activities + depreciation) * (12/365))
## has 643 NAs

## This has profit on operating activities + depreciation in denominator

## Attr48	EBITDA (profit on operating activities - depreciation) / total assets

## &

## Attr49	EBITDA (profit on operating activities - depreciation) / sales

## have combination of profit on operating activities & depreciation in numerator

## It is observed that Attr 48 & Attr 49 are mostly zero when Attr 41 is null

## Therefore, it may be assumed that when Attr 41 is NA, its denominator tends to zero

boxplot(bankruptcy$Attr41) ## Outliers identified

bankruptcy$Attr41<-ifelse(bankruptcy$Attr41>25000,
                          25000,bankruptcy$Attr41) ## Ceiling assigned

bankruptcy$Attr41<-ifelse(is.na(bankruptcy$Attr41),
                          25000,bankruptcy$Attr41) ## NA values imputed

colSums(is.na(bankruptcy)) ## A few columns still have NA values

## Using Central Imputation to tackle remaining missing values

temp =  centralImputation(bankruptcy)

bankruptcy<-temp ## Re - assigning values

rm(temp) ## Removing temp variable

## Train - Validation Split

## Train Validation Split ##

library(caret)

set.seed(1234)

train_rows<-createDataPartition(bankruptcy$target,p = 0.8, list = F)

pre_train_data<-bankruptcy[train_rows,]

pre_validation_data<-bankruptcy[-train_rows,]

rm(train_rows) ## Remove the variable train_rows

## Standardization of Data ##

std_obj <- preProcess(x = pre_train_data[, !colnames(pre_train_data) %in% c("target")],
                      method = c("center", "scale"))

train_data <- predict(std_obj, pre_train_data)

validation_data <- predict(std_obj, pre_validation_data)

rm(pre_train_data,pre_validation_data) ## Removing temp variable

## Preparing Test Data

pre_test<-read.csv("test.csv",header = T)

rownames(pre_test)<-pre_test$ID

pre_test$ID<-NULL

## Preliminary Data Analysis & Missing Value Imputation##

## Attr4

pre_test$Attr4<-ifelse(pre_test$Attr4>2000,2000,pre_test$Attr4) ## Ceiling assigned

pre_test$Attr4<-ifelse(is.na(pre_test$Attr4),2000,pre_test$Attr4) ## NA values imputed

## Attr 12

mi<-min(pre_test$Attr12[!is.na(pre_test$Attr12)])

pre_test$Attr12<-ifelse(pre_test$Attr18<0 & is.na(pre_test$Attr12),
                          mi,pre_test$Attr12)

ma<-max(pre_test$Attr12[!is.na(pre_test$Attr12)])

pre_test$Attr12<-ifelse(pre_test$Attr18>=0 & is.na(pre_test$Attr12),
                          ma,pre_test$Attr12)

rm(mi,ma) ## Removing temp variables

## Attr 33

pre_test$Attr33<-ifelse(pre_test$Attr33>10000,10000,pre_test$Attr33) ## Ceiling assigned

pre_test$Attr33<-ifelse(is.na(pre_test$Attr33),
                          10000,pre_test$Attr33) ## NA values imputed

## Attr 40

pre_test$Attr40<-ifelse(pre_test$Attr40>2500,2500,pre_test$Attr40) ## Ceiling assigned

pre_test$Attr40<-ifelse(is.na(pre_test$Attr40),
                          2500,pre_test$Attr40) ## NA values imputed

## Attr 46

pre_test$Attr46<-ifelse(pre_test$Attr46>2500,2500,pre_test$Attr46) ## Ceiling assigned

ma<-max(pre_test$Attr46[!is.na(pre_test$Attr46)])

pre_test$Attr46<-ifelse(is.na(pre_test$Attr63) & is.na(pre_test$Attr46),
                          ma,pre_test$Attr46)

rm(ma) ## Removing temp variable

## Attr 63

pre_test$Attr63<-ifelse(pre_test$Attr63>5000,5000,pre_test$Attr63) ## Ceiling assigned

pre_test$Attr63<-ifelse(is.na(pre_test$Attr63),
                          5000,pre_test$Attr63) ## NA values imputed

## Attr 8

pre_test$Attr8<-ifelse(pre_test$Attr8>10000,10000,pre_test$Attr8) ## Ceiling assigned

pre_test$Attr8<-ifelse(is.na(pre_test$Attr8),
                         10000,pre_test$Attr8) ## NA values imputed

## Attr 16

ma<-max(pre_test$Attr16[!is.na(pre_test$Attr16)])

pre_test$Attr16<-ifelse(is.na(pre_test$Attr16),
                          ma,pre_test$Attr16) ## NA values imputed

rm(ma) ## Temp variable removed

## Attr 17

pre_test$Attr17<-ifelse(pre_test$Attr17>5000,5000,pre_test$Attr17) ## Ceiling assigned

pre_test$Attr17<-ifelse(is.na(pre_test$Attr17),
                          5000,pre_test$Attr17) ## NA values imputed

## Attr 26

pre_test$Attr26<-ifelse(pre_test$Attr26>1000,1000,pre_test$Attr26) ## Ceiling assigned

pre_test$Attr26<-ifelse(is.na(pre_test$Attr26),
                          1000,pre_test$Attr26) ## NA values imputed

## Attr 34

pre_test$Attr34<-ifelse(pre_test$Attr34>10000,10000,pre_test$Attr34) ## Ceiling assigned

pre_test$Attr34<-ifelse(is.na(pre_test$Attr34),
                          10000,pre_test$Attr34) ## NA values imputed

## Attr 50

pre_test$Attr50<-ifelse(pre_test$Attr50>1500,1500,pre_test$Attr50) ## Ceiling assigned

pre_test$Attr50<-ifelse(is.na(pre_test$Attr50),
                          1500,pre_test$Attr50) ## NA values imputed

## Attr 28

pre_test$Attr28<-ifelse(pre_test$Attr28>10000,10000,pre_test$Attr28) ## Ceiling assigned

pre_test$Attr28<-ifelse(is.na(pre_test$Attr28),
                          10000,pre_test$Attr28) ## NA values imputed

## Attr 53

pre_test$Attr53<-ifelse(pre_test$Attr53>30000,30000,pre_test$Attr53) ## Ceiling assigned

pre_test$Attr53<-ifelse(is.na(pre_test$Attr53),
                          30000,pre_test$Attr53) ## NA values imputed

## Attr 54

pre_test$Attr54<-ifelse(pre_test$Attr54>30000,30000,pre_test$Attr54) ## Ceiling assigned

pre_test$Attr54<-ifelse(is.na(pre_test$Attr54),
                          30000,pre_test$Attr54) ## NA values imputed

## Attr 64

pre_test$Attr64<-ifelse(pre_test$Attr64>10000,10000,pre_test$Attr64) ## Ceiling assigned

pre_test$Attr64<-ifelse(is.na(pre_test$Attr64),
                          10000,pre_test$Attr64) ## NA values imputed

## Attr 60

pre_test$Attr60<-ifelse(pre_test$Attr60>1000000,
                          1000000,pre_test$Attr60) ## Ceiling assigned

pre_test$Attr60<-ifelse(is.na(pre_test$Attr45) & is.na(bankruptcy$Attr60),
                          1000000,pre_test$Attr60) ## NA values imputed

## Attr 45

pre_test$Attr45<-ifelse(pre_test$Attr45>200000,
                          200000,pre_test$Attr45) ## Ceiling assigned

pre_test$Attr45<-ifelse(pre_test$Attr45<-100000,
                          -100000,pre_test$Attr45) ## Floor assigned

pre_test$Attr45<-ifelse(pre_test$Attr1<0 & is.na(pre_test$Attr45),
                          -100000,pre_test$Attr45) ## NA values imputed

pre_test$Attr45<-ifelse(pre_test$Attr1>=0 & is.na(pre_test$Attr45),
                          200000,pre_test$Attr45) ## NA values imputed

## Attr 32

pre_test$Attr32<-ifelse(pre_test$Attr32>5000000,
                          5000000,pre_test$Attr32) ## Ceiling assigned

pre_test$Attr32<-ifelse(is.na(pre_test$Attr47) & is.na(pre_test$Attr32),
                          5000000,pre_test$Attr32) ## Majority NA values imputed

## Attr 52

pre_test$Attr52<-ifelse(pre_test$Attr52>20000,
                          20000,pre_test$Attr52) ## Ceiling assigned

pre_test$Attr52<-ifelse(is.na(pre_test$Attr47) & is.na(pre_test$Attr52),
                          20000,pre_test$Attr52) ## Majority NA values imputed

## Attr 47

pre_test$Attr47<-ifelse(pre_test$Attr47>1000000,
                          1000000,pre_test$Attr47) ## Ceiling assigned

pre_test$Attr47<-ifelse(is.na(pre_test$Attr47),
                          1000000,pre_test$Attr47) ## NA values imputed

temp<-pre_test ## Assigning to temp variable

temp$Attr21[is.na(temp$Attr21)]<-mean(!is.na(temp$Attr21))

pre_test<-temp ## Re - assigning

rm(temp) ## Removing temp variable

## Attr 37

pre_test$Attr37<-ifelse(pre_test$Attr37>200000,
                          200000,pre_test$Attr37) ## Ceiling assigned

pre_test$Attr37<-ifelse(is.na(pre_test$Attr37),
                          200000,pre_test$Attr37) ## NA values imputed

## Attr 61

pre_test$Attr61<-ifelse(pre_test$Attr61>40000,
                          40000,pre_test$Attr61) ## Ceiling assigned

pre_test$Attr61<-ifelse(is.na(pre_test$Attr61),
                          40000,pre_test$Attr61) ## NA values imputed

## Attr 41

pre_test$Attr41<-ifelse(pre_test$Attr41>25000,
                          25000,pre_test$Attr41) ## Ceiling assigned

pre_test$Attr41<-ifelse(is.na(pre_test$Attr41),
                          25000,pre_test$Attr41) ## NA values imputed

temp =  centralImputation(pre_test)

pre_test<-temp ## Re - assigning values

rm(temp) ## Removing temp variable

## Standardization Test Data

test_data <- predict(std_obj, pre_test)

rm(pre_test) ## Remove temp variable

## Logistic Regression - Iteration 1

log_reg1<-glm(target~.,data = train_data,family = binomial)

## Warning Message #1: algorithm did not converge
## Warning Message #2: fitted probabilities numerically 0 or 1 occured

## Logistic Regression - Iteration 2 increase maxit to 50

log_reg2<-glm(target~.,data = train_data,family = binomial,control = list(maxit = 300))

## Warning Message 1 no longer observed - algorithm converges

## Warning Message 2 fitted probabilities numerically 0 or 1 occured is still not resolved

summary(log_reg2) ## Every variable shows up as significant

## Creating an ROC Plot ##

prob_train <- predict(log_reg2, type = "response")

library(ROCR)

pred <- prediction(prob_train, train_data$target)

perf <- performance(pred, measure="tpr", x.measure="fpr")

plot(perf, col=rainbow(10), colorize=T, print.cutoffs.at=seq(0,1,0.05))

perf_auc <- performance(pred, measure="auc")

auc <- perf_auc@y.values[[1]]

auc ## AUC comes out to be only 0.5044

## Dropping Logistic Regression & Moving on to Decision Trees CART

rm(auc,log_reg1,log_reg2,perf,perf_auc,pred,prob_train)

library(rpart)

rpart_tree <- rpart(target ~ . , data = train_data, method="class")

rpart_tree$variable.importance

## Model Evaluation

preds_rpart <- predict(rpart_tree, validation_data, type="class")

validation_data_labs <- validation_data$target

conf_matrix <- table(validation_data_labs, preds_rpart)

conf_matrix

## Precision = TP/(TP+FP)

## Recall = TP/(TP+FN)


prec<-conf_matrix[2,2]/sum(conf_matrix[,2])

rec<-conf_matrix[2,2]/sum(conf_matrix[2,])

F1<-(2*prec*rec)/(prec+rec)

F1 ## Comes out to be 0.53

printcp(rpart_tree)

plotcp(rpart_tree)

rpart_tree1 <- rpart(target ~ ., train_data, method = "class", 
                     control = rpart.control(cp = 0.0001))

plotcp(rpart_tree1)

## Tuning cp

rpart_tree2 <- rpart(target ~ ., train_data, method = "class",
                     control = rpart.control(cp = 0.0028))

plotcp(rpart_tree2)

preds_rpart <- predict(rpart_tree2, validation_data, type="class")

validation_data_labs <- validation_data$target

conf_matrix <- table(validation_data_labs, preds_rpart)

conf_matrix

prec<-conf_matrix[2,2]/sum(conf_matrix[,2])

rec<-conf_matrix[2,2]/sum(conf_matrix[2,])

F1<-(2*prec*rec)/(prec+rec)

F1 ## F1 Score comes out to be 0.5069

## Prediction on Test Data

pred_test <- predict(rpart_tree2, test_data, type="class")

## Write CSV

write.csv(pred_test,file="KR_RF.csv") ## F1 Score of 53.07% on Test

rm(conf_matrix,F1,prec,pred_test,preds_rpart,rec,rpart_tree,rpart_tree1,
   rpart_tree2,validation_data_labs) ## Removing unrequired variables

## Trying Decision Trees C5.0

library(C50)

c5_tree <- C5.0(target ~ . , train_data)

c5_rules <- C5.0(target ~ . , train_data, rules = T)

C5imp(c5_tree, metric = "usage")

summary(c5_rules)

# Extract and store important variables obtained from the random forest model
dt_Imp_Attr = data.frame(c5_tree$usage)
dt_Imp_Attr = data.frame(row.names(dt_Imp_Attr),dt_Imp_Attr[,1])
colnames(rf_Imp_Attr) = c('Attributes', 'Importance')
rf_Imp_Attr = rf_Imp_Attr[order(rf_Imp_Attr$Importance, decreasing = TRUE),]

dt_Imp_Attr

plot(c5_tree)

preds <- predict(c5_tree, validation_data)

validation_data_labs <- validation_data$target

conf_matrix <- table(validation_data_labs, preds)

conf_matrix

prec<-conf_matrix[2,2]/sum(conf_matrix[,2])

rec<-conf_matrix[2,2]/sum(conf_matrix[2,])

F1<-(2*prec*rec)/(prec+rec)

F1 ## F1 Score comes out to be 0.547

rm(c5_rules,c5_tree,conf_matrix,F1,prec,preds,rec,
   validation_data_labs,dt_Imp_Attr) ## Remove temp

## Applying on Test Data ##



## Applying Random Forest

rm(conf_matrix,F1,prec,preds_rpart,rec,rpart_tree,rpart_tree1,rpart_tree2,
   validation_data_labs,pred_test)

# Build the classification model using randomForest

library(randomForest)

set.seed(1234)

model = randomForest(target ~ ., data=train_data, 
                     keep.forest=TRUE, ntree=300)

# Print and understand the model
print(model)

# Predict on Train data 
pred_Train = predict(model, 
                     train_data[,setdiff(names(train_data), "target")],
                     type="response", 
                     norm.votes=TRUE)

# Build confusion matrix and find accuracy   
cm_Train = table("actual"= train_data$target, "predicted" = pred_Train);
cm_Train

## Precision = TP/(TP+FP)

## Recall = TP/(TP+FN)

prec<-cm_Train[2,2]/sum(cm_Train[,2])

rec<-cm_Train[2,2]/sum(cm_Train[2,])

F1<-(2*prec*rec)/(prec+rec)

F1 ## F1 Score is 0.99 on train data

# Predicton Validation Data
pred_validation = predict(model, validation_data[,setdiff(names(validation_data),
                                                          "target")],
                          type="response", 
                          norm.votes=TRUE)

# Build confusion matrix and find accuracy   
cm_validation = table("actual"= validation_data$target, "predicted" = pred_validation);
cm_validation

## Precision = TP/(TP+FP)

## Recall = TP/(TP+FN)

prec<-cm_validation[2,2]/sum(cm_validation[,2])

rec<-cm_validation[2,2]/sum(cm_validation[2,])

F1<-(2*prec*rec)/(prec+rec)

F1 ## 0.36 on validation data

#Select mtry value with minimum out of bag(OOB) error.
mtry <- tuneRF(train_data[-65],train_data$target, ntreeTry=300,
               stepFactor=1.5,improve=0.01, trace=TRUE, plot=TRUE)
best.m <- mtry[mtry[, 2] == min(mtry[, 2]), 1]
print(mtry)
print(best.m)

#Build Model with best mtry again - 
set.seed(71)
rf <- randomForest(target~.,data=train_data, mtry=best.m, importance=TRUE,
                   ntree=300,replace = T)
print(rf)

# Predict on Train data 
pred_Train = predict(rf, 
                     train_data[,setdiff(names(train_data), "target")],
                     type="response", 
                     norm.votes=TRUE)

# Build confusion matrix and find accuracy   
cm_Train = table("actual"= train_data$target, "predicted" = pred_Train);

cm_Train

## Precision = TP/(TP+FP)

## Recall = TP/(TP+FN)

prec<-cm_Train[2,2]/sum(cm_Train[,2])

rec<-cm_Train[2,2]/sum(cm_Train[2,])

F1<-(2*prec*rec)/(prec+rec)

F1 ## F1 score as 1

# Predicton Validation Data
pred_validation = predict(rf, validation_data[,setdiff(names(validation_data),
                                                       "target")],
                          type="response", 
                          norm.votes=TRUE)

# Build confusion matrix and find accuracy   
cm_validation = table("actual"=validation_data$target, "predicted"=pred_validation);

cm_validation

## Precision = TP/(TP+FP)

## Recall = TP/(TP+FN)

prec<-cm_validation[2,2]/sum(cm_validation[,2])

rec<-cm_validation[2,2]/sum(cm_validation[2,])

F1<-(2*prec*rec)/(prec+rec)

F1 ## F1 Score of 0.524 is observed

## Prediction on Test Data ##

pred_test<-predict(rf, test_data[,setdiff(names(test_data),
                                                "target")],
                   type="response", 
                   norm.votes=TRUE)

## Write CSV

write.csv(pred_test,file="KR_RF.csv") ## 54.19% score on Test Data

## Trying ADABoost

library(vegan)
library(ada)

rm(mtry,best.m,cm_Train,cm_validation,F1,prec,model,pred_Train,
   pred_validation,rec,rf,pred_test)
## Temp variables removed

model<-ada(x=train_data[-65],train_data$target,iter = 100,
           loss = "exponential",type = "discrete", nu = 1)

model

summary(model)

pred_Train  =  predict(model, train_data[-65])

cm_Train = table(train_data$target, pred_Train)

cm_Train

## Precision = TP/(TP+FP)

## Recall = TP/(TP+FN)

prec<-cm_Train[2,2]/sum(cm_Train[,2])

rec<-cm_Train[2,2]/sum(cm_Train[2,])

F1<-(2*prec*rec)/(prec+rec)

F1 ## F1 score as 0.71

pred_validation = predict(model, validation_data[-65])

cm_validation <- table(validation_data$target,pred_validation)

## Precision = TP/(TP+FP)

## Recall = TP/(TP+FN)

prec<-cm_validation[2,2]/sum(cm_validation[,2])

rec<-cm_validation[2,2]/sum(cm_validation[2,])

F1<-(2*prec*rec)/(prec+rec)

F1 ## F1 score as 0.61

## Prediction on Test Data ##

pred_test = predict(model, test_data)

## Write CSV

write.csv(pred_test,file="Team6_ADA.csv") ## F1 Score on Test only 49.56%

rm(cm_Train,cm_validation,F1,model,prec,pred_Train,
   pred_validation,rec,pred_test) ## Remove temp

## Trying XGBoost

library(xgboost)

train_matrix <- xgb.DMatrix(data = as.matrix(train_data[, !(names(train_data) %in% c("target"))]), 
                            label = as.matrix(train_data[, names(train_data) %in% "target"]))

validation_matrix <- xgb.DMatrix(data = as.matrix(validation_data[, !(names(validation_data) %in% c("target"))]), 
                           label = as.matrix(validation_data[, names(validation_data) %in% "target"]))

test_matrix<- xgb.DMatrix(data = as.matrix(test_data))

xgb_model_basic <- xgboost(data = train_matrix, max.depth = 6, eta = 1, nthread = 2, 
                           nround = 500, objective = "binary:logistic", 
                           verbose = 1, early_stopping_rounds = 10)

basic_preds <- predict(xgb_model_basic, validation_matrix) ## On validation

basic_preds_train<- predict(xgb_model_basic, train_matrix) ## On train

basic_preds_labels_train<-ifelse(basic_preds_train < 0.5, 0, 1) ## Assign labels on train

basic_preds_labels <- ifelse(basic_preds < 0.5, 0, 1) ## Assign labels on validation

basic_preds_test<- predict(xgb_model_basic,test_matrix) ## On test

basic_preds_labels_test<-ifelse(basic_preds_test < 0.5, 0, 1) ## Assign labels on test

cm_Train = table(train_data$target, basic_preds_labels_train)

cm_Train

## Precision = TP/(TP+FP)

## Recall = TP/(TP+FN)

prec<-cm_Train[2,2]/sum(cm_Train[,2])

rec<-cm_Train[2,2]/sum(cm_Train[2,])

F1<-(2*prec*rec)/(prec+rec)

F1 ## F1 score as 0.99

cm_validation <- table(validation_data$target,basic_preds_labels)

## Precision = TP/(TP+FP)

## Recall = TP/(TP+FN)

prec<-cm_validation[2,2]/sum(cm_validation[,2])

rec<-cm_validation[2,2]/sum(cm_validation[2,])

F1<-(2*prec*rec)/(prec+rec)

F1 ## F1 score as 0.66

params_list <- list("objective" = "binary:logitraw",
                    "eta" = 0.1,
                    "early_stopping_rounds" = 10,
                    "max_depth" = 4,
                    "gamma" = 0.5,
                    "colsample_bytree" = 0.5,
                    "subsample" = 0.65,
                    "eval_metric" = "auc",
                    "silent" = 1)

xgb_model_with_params <- xgboost(data = train_matrix, params = params_list, 
                                 nrounds = 500, early_stopping_rounds = 20)

basic_params_preds <- predict(xgb_model_with_params, validation_matrix)

basic_params_preds_labels <- ifelse(basic_params_preds < 0.5, 0, 1)

cm_validation <- table(validation_data$target,basic_params_preds_labels)

## Precision = TP/(TP+FP)

## Recall = TP/(TP+FN)

prec<-cm_validation[2,2]/sum(cm_validation[,2])

rec<-cm_validation[2,2]/sum(cm_validation[2,])

F1<-(2*prec*rec)/(prec+rec)

F1 ## F1 score as 0.66

## Using Caret Package

sampling_strategy <- trainControl(method = "repeatedcv", number = 5, repeats = 2, 
                                  verboseIter = F, allowParallel = T)

param_grid <- expand.grid(.nrounds = 20, .max_depth = c(2, 4, 6), .eta = c(0.1, 0.3),
                          .gamma = c(0.6, 0.5, 0.3), .colsample_bytree = c(0.6, 0.4),
                          .min_child_weight = 1, .subsample = c(0.5, 0.6, 0.9))

xgb_tuned_model <- train(x = train_data[ , !(names(train_data) %in% c("target"))], 
                         y = train_data[ , names(train_data) %in% c("target")], 
                         method = "xgbTree",
                         trControl = sampling_strategy,
                         tuneGrid = param_grid)

xgb_tuned_model$bestTune

plot(xgb_tuned_model)

tuned_params_preds <- predict(xgb_tuned_model, test_data)

tuned_params_validation <- predict(xgb_tuned_model, validation_data)

cm_validation <- table(validation_data$target,tuned_params_validation)

## Precision = TP/(TP+FP)

## Recall = TP/(TP+FN)

prec<-cm_validation[2,2]/sum(cm_validation[,2])

rec<-cm_validation[2,2]/sum(cm_validation[2,])

F1<-(2*prec*rec)/(prec+rec)

F1 ## F1 score as 0.56

## Check on Test

write.csv(tuned_params_preds,file="Team6_XGB.csv") ## 50.35% F1 Score on Test
