if(!require("data.table"))
{
  install.packages("data.table")
  library("data.table")
}

#load lubridate package
if(!require("lubridate"))
{
  install.packages("lubridate")
  library("lubridate")
}

if(!require("Matrix"))
{
  install.packages("Matrix")
  library("Matrix")
}

if(!require("xgboost"))
{
  install.packages("xgboost")
  library("xgboost")
}

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
  train_data[, c("is_checkout", "n_sessions", "n_checkouts", "events_per_session"):=list(NULL, NULL, NULL, NULL)]
  
  print("Loading events distribution...")
  events_distribution <- fread("data/events_distribution.csv")
  events_distribution <- events_distribution$probability
  MAX_N_EVENTS <- length(events_distribution)
  
  print("Loading xgboost models...")
  xgb_model_without_history <- xgb.load("R/models/model_without_history.xgb")
  xgb_model_with_history <- xgb.load("R/models/model_with_history.xgb")
  
  local_session_history <- matrix(data=numeric(), nrow=0, ncol=2 + MAX_N_EVENTS)
  colnames(local_session_history) <- c("n_past_events", "is_checkout", 1:MAX_N_EVENTS)

  while (TRUE)
  {
    #@check for stability
    print(paste("Listening port", PORT, "..."))
    con <- socketConnection(host = "localhost", port = PORT, blocking = TRUE, 
                            server = TRUE, open = "r+")
    
    raw_data <- readLines(con, 1)
    print(paste("New event:\n", raw_data))
    
    print("Formating data...")
    raw_data <- paste(toString(c("visitor_id", "session_id", "time", "page_id", "source_id", 
                                 "user_agent", "is_checkout_page", "cc", "cloc")), 
                      "\n", raw_data)
    
    raw_data <- as.data.table(read.csv(text=raw_data))

    response <- numeric()
    
    if(is.na(raw_data$is_checkout_page) == TRUE)
    {
      if(raw_data$session_id %in% rownames(local_session_history) == TRUE)
      {
        if(local_session_history[as.character(raw_data$session_id), "is_checkout"] == 0)
        {
          #increment number of past events
          if(local_session_history[as.character(raw_data$session_id), "n_past_events"] < (MAX_N_EVENTS - 1))
          {
            local_session_history[as.character(raw_data$session_id), "n_past_events"] <- 
              local_session_history[as.character(raw_data$session_id), "n_past_events"] + 1
            
            #adjust events distribution for current number of clicks
            n_past_events <- local_session_history[as.character(raw_data$session_id), "n_past_events"]
            
            current_distribution <- events_distribution[n_past_events:MAX_N_EVENTS] / 
              sum(events_distribution[n_past_events:MAX_N_EVENTS])
            
            #here one value from the beginnig is skipped in order to include probability 
            #this event will be the last
            response <- as.numeric(local_session_history[as.character(raw_data$session_id), 
                                                         (2 + n_past_events + 1):ncol(local_session_history)] %*%
                                     current_distribution[2:length(current_distribution)])
          } else
          {
            #@this mght be improved
            print("Reached maximum number of events per session")
            response <- 0.5 * local_session_history[as.character(raw_data$session_id), 2 + MAX_N_EVENTS]
          }
        } else #there has already been checkout
        {
          response <- "There has already been checkout in this session"
        }
      } else #there is no entry in local_session_history
      {
        print("Processing time features...")
        raw_data[, time:=substr(raw_data$time, 1, 19)]
        raw_data[, time:=as.numeric(strptime(raw_data$time, format="%Y-%m-%d %H:%M:%S"))]
        
        ptime <- as.POSIXct(raw_data$time, tz="UTC", origin="1970-01-01")
        raw_data[, hour:=as.character(format(ptime, format="%H"))]
        raw_data[, week_day:=as.character(format(ptime, format="%a"))]
        raw_data[, month_day:=as.character(format(ptime, format="%d"))]
        raw_data[, year_day:=as.numeric(format(ptime, format="%j"))] #adjust year day
        raw_data[, time:=NULL]
        
        print("Processing geoip data...")
        raw_data$cloc <- cities[raw_data$cloc]
        if(is.na(raw_data$cloc))
          raw_data$cloc <- "Other"
        
        if(!(raw_data$cc %in% major_cc))
          raw_data$cc <- "Other"
        
        if(!(raw_data$cloc %in% major_cloc))
          raw_data$cloc <- "Other"
        
        print("Processing user agent...")
        raw_data[, c("os", "device"):=list("Other", "Other")]
        
        raw_data$device[grep("Linux|Android|Mobile|BB10|iPhone|iPod|iPad", raw_data$user_agent)] <- "Mobile"
        raw_data$device[grep("Windows|compatible|Macintosh", raw_data$user_agent)] <- "Desktop"
        
        raw_data$os[grep("Linux|Android", raw_data$user_agent)] <- "Android"
        raw_data$os[grep("iPhone|iPod|iPad|Macintosh", raw_data$user_agent)] <- "Mac_iOS"
        raw_data$os[grep("Windows|compatible", raw_data$user_agent)] <- "Windows"
        
        current_session <- raw_data$session_id
        current_visitor <- raw_data$visitor_id
        
        print("Cleaning data...")
        raw_data[, session_id:=NULL]
        raw_data[, visitor_id:=NULL]
        raw_data[, source_id:=NULL]
        raw_data[, page_id:=NULL]
        raw_data[, user_agent:=NULL]
        raw_data[, is_checkout_page:=NULL]
        
        print("Building model matrix...")
        raw_data <- cbind(raw_data, n_events=1:MAX_N_EVENTS)
        raw_data <- rbind(raw_data, train_data)
        
        mm <- sparse.model.matrix(~. -1, raw_data)[1:MAX_N_EVENTS, ]
        pred <- numeric()
        
        if(current_visitor %in% visitors_history$visitor_id)
        {
          #@this might be improved
          print("Processing visitors' history...")
          mm <- cBind(mm, Matrix(as.matrix(
                  rBind(visitors_history[visitor_id == current_visitor])[rep(1, MAX_N_EVENTS)])))
          
          print("Predicting...")
          pred <- predict(xgb_model_with_history, mm)
        } else
        {
          print("Predicting...")
          pred <- predict(xgb_model_without_history, mm)
        }
        
        response <- as.numeric(pred[2:length(pred)] %*% events_distribution[2:length(pred)])
        
        local_session_history <- rbind(local_session_history, c(1, 0, pred))
        rownames(local_session_history)[nrow(local_session_history)] <- current_session
      }
    } else #current event contains checkout
    {
      if(raw_data$session_id %in% rownames(local_session_history) == TRUE)
      {
        local_session_history[as.character(raw_data$session_id), "is_checkout"] <- 1
      } else
      {
        local_session_history <- rbind(local_session_history, c(1, 1, rep(0, MAX_N_EVENTS)))
        rownames(local_session_history)[nrow(local_session_history)] <- as.character(raw_data$session_id)
      }
      
      response <- "There has already been checkout in this session"
    }
    
    #send predicted value to the socket
    writeLines(as.character(response), con)
    
    print("Finished!")
    close(con)
  }
}

server()