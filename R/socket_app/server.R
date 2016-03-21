if(!require("data.table")) {install.packages("data.table"); library("data.table")}
if(!require("lubridate")) {install.packages("lubridate"); library("lubridate")}
if(!require("Matrix")) {install.packages("Matrix"); library("Matrix")}
if(!require("xgboost")) {install.packages("xgboost"); library("xgboost")}

server <- function()
{
  print("Starting server...")
  PORT <- 6011
  
  #set locale for weekday
  Sys.setlocale("LC_TIME", "C")
  
  print("Loading geoip data...")
  cities <- fread(input="data/cities.csv")
  cities[is.na(cities)] <- "Other"
  
  major_cc <- readLines("data/cc.txt")
  major_cloc <- readLines("data/cloc.txt")
  
  print("Loading visitors' history data...")
  visitors_history <- fread("data/visitors_history.csv")
  setkey(visitors_history, visitor_id)
  
  print("Loading train data...")
  train_data <- fread("data/train_data.csv")
  train_data[, is_checkout:=NULL]
  
  print("Loading events distribution...")
  events_distribution <- fread("data/events_distribution.csv")
  events_distribution <- events_distribution$probability
  MAX_N_EVENTS <- length(events_distribution)
  
  print("Loading threshold values...")
  thresholds <- fread("data/thresholds.csv")
  
  print("Loading xgboost models...")
  xgb_model_without_history <- xgb.load("R/models/model_without_history.xgb")
  xgb_model_with_history <- xgb.load("R/models/model_with_history.xgb")
  
  local_session_history <- matrix(data=numeric(), nrow=0, ncol=2 + MAX_N_EVENTS)
  colnames(local_session_history) <- c("with_history", "n_past_events", 1:MAX_N_EVENTS)

  while (TRUE)
  {
    print(paste0("Listening port ", PORT, "..."))
    con <- socketConnection(host = "localhost", port = PORT, blocking = TRUE, 
                            server = TRUE, open = "r+")
    
    #raw_data <- '69739898,120838210,2015-12-19 15:21:19.560241,16631985,,"Mozilla/5.0 (iPad; CPU OS 9_2 like Mac OS X) AppleWebKit/601.1.46 (KHTML, like Gecko) Version/9.0 Mobile/13C75 Safari/601.1",,DK,59'
    
    raw_data <- readLines(con, 1)
    print(paste("New event:\n", raw_data))
    
    print("Formating data...")
    raw_data <- paste(toString(c("visitor_id", "session_id", "time", "page_id", "source_id", 
                                 "user_agent", "is_checkout_page", "cc", "cloc")), 
                      "\n", raw_data)
    
    raw_data <- as.data.table(read.csv(text=raw_data))
    
    current_session <- matrix(c(0, rep(0, 1 + MAX_N_EVENTS)), 1)
    colnames(current_session) <- c("with_history", "n_past_events", 1:MAX_N_EVENTS)
    rownames(current_session) <- as.character(raw_data$session_id)
    
    #add event to local session history
    if(raw_data$session_id %in% rownames(local_session_history) == FALSE)
    {
      print("Processing time features...")
      raw_data[, time:=substr(raw_data$time, 1, 19)]
      raw_data[, time:=as.numeric(strptime(raw_data$time, format="%Y-%m-%d %H:%M:%S"))]
      
      ptime <- as.POSIXct(raw_data$time, tz="UTC", origin="1970-01-01")
      raw_data[, hour:=as.character(format(ptime, format="%H"))]
      raw_data[, week_day:=as.character(format(ptime, format="%a"))]
      raw_data[, month_day:=as.character(format(ptime, format="%d"))]
      raw_data[, year_day:=as.numeric(format(ptime, format="%j")) + 
                           (as.numeric(format(ptime, format="%Y")) - 2015) * 365] #adjust year day
      
      raw_data[, time:=NULL]
      
      print("Processing geoip data...")
      raw_data$cloc <- cities[raw_data$cloc]
      if(is.na(raw_data$cloc) == TRUE | raw_data$cloc %in% major_cloc == FALSE)
        raw_data$cloc <- "Other"
      
      if(raw_data$cc %in% major_cc == FALSE)
        raw_data$cc <- "Other"

      print("Processing user agent...")
      raw_data[, c("os", "device"):=list("Other", "Other")]
      
      raw_data$device[grep("Linux|Android|Mobile|BB10|iPhone|iPod|iPad", raw_data$user_agent)] <- "Mobile"
      raw_data$device[grep("Windows|compatible|Macintosh", raw_data$user_agent)] <- "Desktop"
      
      raw_data$os[grep("Linux|Android", raw_data$user_agent)] <- "Android"
      raw_data$os[grep("iPhone|iPod|iPad|Macintosh", raw_data$user_agent)] <- "Mac_iOS"
      raw_data$os[grep("Windows|compatible", raw_data$user_agent)] <- "Windows"
      
      print("Cleaning data...")
      raw_data[, c("session_id", "source_id", "page_id", "user_agent", "is_checkout_page") :=
                 list(NULL, NULL, NULL, NULL, NULL)]
      
      print("Processing visitor's history...")
      if(raw_data$visitor_id %in% visitors_history$visitor_id)
      {
        current_session[, 1] <- 1 #with visitor's history
        raw_data <- merge(raw_data, visitors_history[visitors_history$visitor_id == raw_data$visitor_id], by="visitor_id")
      }
      
      raw_data[, visitor_id:=NULL]
      
      
      print("Building model matrix...")
      local_train_data <- train_data
      if(current_session[, 1] == 0)
      {
        local_train_data[, c("n_sessions", "n_checkouts", "events_per_session"):=list(NULL, NULL, NULL)]
      }
      
      raw_data <- cbind(raw_data, n_events=1:MAX_N_EVENTS)
      raw_data <- rbind(raw_data, local_train_data)
      
      mm <- sparse.model.matrix(~. -1, raw_data)[1:MAX_N_EVENTS, ]
      
      print("Predicting...")
      current_session[, 2] <- 1 #n_past_events
      pred <- numeric()
      if(current_session[, 1] == 0) #without history
      {
        pred <- predict(xgb_model_with_history, mm)
      } else
      {
        pred <- predict(xgb_model_without_history, mm)
      }
      
      print("Calculating probability of checkout...")
      for(i in 1:(MAX_N_EVENTS - 1))
      {
        ed <- events_distribution[i:MAX_N_EVENTS] / sum(events_distribution[i:MAX_N_EVENTS])
        ed <- ed[2:length(ed)]
        
        current_session[, 2 + 1 + i] <- as.numeric(pred[(1 + i):MAX_N_EVENTS] %*% ed)
      }
      
      local_session_history <- rbind(local_session_history, current_session)
    } else #load session from history
    {
      current_session[as.character(raw_data$session_id), ] <- local_session_history[as.character(raw_data$session_id), ]
    }
    
    print("Writing response...")
    response <- paste("Prob. to checkout:", 
                      current_session[, 2 + 1 + current_session[, "n_past_events"]],
                      "Suggested threshold:")
    if(current_session[, "with_history"] == 0)
    {
      response <- paste(response, thresholds$without_history[current_session[, "n_past_events"]])
    } else
    {
      response <- paste(response, thresholds$with_history[current_session[, "n_past_events"]])
    }
    
    print("Updating local session history...")
    if(current_session[, "n_past_events"] < MAX_N_EVENTS - 1)
    {
      current_session[, "n_past_events"] <- current_session[, "n_past_events"] + 1
    } else
    {
      print("Warning: Reached maximum number of events per session")
    }
    
    local_session_history[rownames(current_session), ] <- current_session

    #send predicted value to the socket
    writeLines(as.character(response), con)
    
    print("Finished!")
    close(con)
  }
}

server()