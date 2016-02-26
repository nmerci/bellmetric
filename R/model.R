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

#read train data
train_data <- fread("data/train_data.csv")

#create model matrix
train_matrix <- sparse.model.matrix(~ . -is_checkout -n_sessions -n_events -n_checkouts -events_per_session -1, train_data)

#train model
xgb_model <- xgboost(data=train_matrix, label=train_data$is_checkout, 
                     eta=0.1, nrounds=500, objective="binary:logistic", eval_metric="auc")

#save model to file
xgb.save(xgb_model, "R/xgb_model")
