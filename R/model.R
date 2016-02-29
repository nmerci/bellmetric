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

#train model without history
train_matrix <- sparse.model.matrix(~ . -1 -is_checkout -n_sessions -n_checkouts -events_per_session, train_data)
xgb_model <- xgboost(data=train_matrix, label=train_data$is_checkout, 
                     eta=0.1, nrounds=50, objective="binary:logistic", eval_metric="auc")

#save model to file
xgb.save(xgb_model, "R/models/model_without_history.xgb")

#train model with history
train_matrix <- sparse.model.matrix(~ . -1 -is_checkout, train_data)

xgb_model <- xgboost(data=train_matrix, label=train_data$is_checkout, 
                     eta=0.1, nrounds=50, objective="binary:logistic", eval_metric="auc")

#save model to file
xgb.save(xgb_model, "R/models/model_with_history.xgb")
