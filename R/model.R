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

#convert to factor
session_data[, hour:=as.character(hour)]
session_data[, month_day:=as.character(month_day)]
session_data[, first_source:=as.character(first_source)]
session_data[, first_page:=as.character(first_page)]

#replace NA's
session_data$city[is.na(session_data$city)] <- "Other"
session_data$first_source[is.na(session_data$first_source)] <- "0"

#remove outliers
daily_checkouts <- session_data[, .(checkouts=sum(is_checkout_page)), by="year_day"]
session_data <- session_data[!(session_data$year_day %in% 
                                 daily_checkouts$year_day[daily_checkouts$checkouts < 50])]
session_data <- session_data[session_data$year_day != 331] #Black Friday

#create train model matrix
train_data <- sparse.model.matrix(~ . -is_checkout_page -1, session_data)
train_data <- train_data[1:2000000, ]


#train model
xgb_model <- xgboost(data=train_data, label=session_data$is_checkout_page[1:2000000], 
                     eta=0.1, nrounds=10, objective="binary:logistic", eval_metric="auc")





#decide how to convert input vector in sparse model format
#parse user agent column

#save model to binary file
xgb.save(xgb_model, "R/xgb_model")

######################

#prediction
#load model
xgb_model <- xgb.load("R/xgb_model")

#load events_distribution
events_distribution <- fread("data/events_distribution.csv")

#load history data
#load local session history


test_data <- session_data[2000001:nrow(session_data)]
test_vec <- as.data.frame(test_data[1, ])

x <- matrix(0, 1, length(col_names_mm), dimnames=list(1, col_names_mm))
x[, paste0("country", test_vec$country)] <- 1
x[, paste0("city", test_vec$city)] <- 1
x[, paste0("hour", test_vec$hour)] <- 1
x[, paste0("week_day", test_vec$week_day)] <- 1
x[, paste0("month_day", test_vec$month_day)] <- 1
x[, paste0("year_day")] <- test_vec$year_day
x[, paste0("os", test_vec$os)] <- 1
x[, paste0("os_version", test_vec$os_version)] <- 1
x[, paste0("device", test_vec$device)] <- 1
x[, paste0("h_session_number")] <- test_vec$h_session_number
x[, paste0("h_events_number")] <- test_vec$h_events_number
x[, paste0("h_checkout_number")] <- test_vec$h_checkout_number
x[, paste0("h_mean_session_time")] <- test_vec$h_mean_session_time
x[, paste0("first_source", test_vec$first_source)] <- 1
x[, paste0("first_page", test_vec$first_page)] <- 1

x <- do.call("rbind", rep(list(x), nrow(events_distribution)))
x <- as.data.frame(x)

x$n_events <- events_distribution$n_events








