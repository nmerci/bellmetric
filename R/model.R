if(!require("xgboost"))
{
  install.packages("xgboost")
  library("xgboost")
}

if(!require("data.table"))
{
  install.packages("data.table")
  library("data.table")
}

if(!require("Matrix"))
{
  install.packages("Matrix")
  library("Matrix")
}

session_data <- fread("data/session_data.csv")

#format data
#remove ID's
session_data[, session_id:=NULL]
session_data[, visitor_id:=NULL]

#replace NA's
session_data$city[is.na(session_data$city)] <- "Other"
session_data$first_source[is.na(session_data$first_source)] <- "0"

#remove outliers
daily_checkouts <- session_data[, .(checkouts=sum(is_checkout_page)), by="year_day"]
session_data <- session_data[!(session_data$year_day %in% 
                                 daily_checkouts$year_day[daily_checkouts$checkouts < 50])]
session_data <- session_data[session_data$year_day != 331] #Black Friday

#remove sessions with the only event
session_data <- session_data[session_data$n_events > 1]

#create train model matrix
train_data <- sparse.model.matrix(~ . -is_checkout_page -1, session_data)
train_data <- train_data[1:2000000, ]


#train model
xgb_model <- xgboost(data=train_data, label=session_data$is_checkout_page[1:2000000], 
                     eta=0.1, nrounds=10, objective="binary:logistic", eval_metric="auc")








