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
  MAX_N_EVENTS <- nrow(events_distribution)
  
  print("Loading xgboost models...")
  xgb_model_without_history <- xgb.load("R/models/model_without_history.xgb")
  xgb_model_with_history <- xgb.load("R/models/model_with_history.xgb")
  
  local_session_history <- matrix(data=numeric(), nrow=0, ncol=3 + MAX_N_EVENTS)
  colnames(local_session_history) <- c("session_id", "n_past_events", "is_checkout", 1:MAX_N_EVENTS)

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
    
    #save current session
    current_session <- raw_data$session_id
    response <- numeric()
    
    if(is.na(raw_data$is_checkout_page) == TRUE)
    {
      if(is.element(current_session, local_session_history[, "session_id"]) == TRUE)
      {
        if(local_session_history[local_session_history[, "session_id"] == current_session, "is_checkout"] == FALSE)
        {
          #predict based on session history
          #...
        } else
        {
          response <- "There has already been checkout in this session"
        }
      } else
      {
        #predict using xgboost model
        #...
      }
    } else
    {
      if(is.element(current_session, local_session_history[, "session_id"]) == TRUE)
      {
        #update existing entry in local session history
        #...
      } else
      {
        #create new entry in local session history
        #...
      }
      
      response <- "There has already been checkout in this session"
    }
    
    #send predicted value to the socket
    writeLines(as.character(response), con)
    
    print("Finished!")
    close(con)
    
    
    
    #if not checkout do prediction
    if(is.na(raw_data$is_checkout_page))
    {
      #@this might be improved
      if(is.element(current_session, local_session_history[, "session_id"]))
      {
        #get number of events
        n_past_events <- local_session_history[local_session_history[, "session_id"] == current_session, 
                                               "n_past_events"]
        
        #increment number of events
        if(n_past_events < MAX_N_EVENTS)
        {
          local_session_history[local_session_history[, "session_id"] == current_session, 
                                "n_past_events"] <- n_past_events + 1
        }
        
        #adjust probability distribution
        current_distribution <- events_distribution$probability
        current_distribution[1:n_past_events] <- 0
        current_distribution <- current_distribution / sum(current_distribution)
        current_distribution[n_past_events + 1] <- 0
        
        #write result
        response <- local_session_history[local_session_history[, "session_id"] == current_session, 
                                          (2 + n_past_events + 1):ncol(local_session_history)] %*% 
          current_distribution[n_past_events:MAX_N_EVENTS]
      } else
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
        
        print("Processing city...")
        #@check names for cc 
        #...
        raw_data$cloc <- cities[raw_data$cloc]
        if(is.na(raw_data$cloc))
          raw_data$cloc <- "Other"
        
        print("Processing user agent...")
        raw_data[, c("os", "device"):=list("Other", "Other")]
        
        raw_data$device[grep("Linux|Android|Mobile|BB10|iPhone|iPod|iPad", raw_data$user_agent)] <- "Mobile"
        raw_data$device[grep("Windows|compatible|Macintosh", raw_data$user_agent)] <- "Desktop"
        
        raw_data$os[grep("Linux|Android", raw_data$user_agent)] <- "Android"
        raw_data$os[grep("iPhone|iPod|iPad|Macintosh", raw_data$user_agent)] <- "Mac_iOS"
        raw_data$os[grep("Windows|compatible", raw_data$user_agent)] <- "Windows"
        
        print("Processing visitor's history...")
        history_data <- visitors_history[visitor_id == raw_data$visitor_id]
        
        if(complete.cases(history_data) == FALSE)
        {
          #@use another predictive model in this case instead
          raw_data[, c("n_sessions", "n_checkouts", "events_per_session"):=list(0, 0, 0)]
        } else
        {
          raw_data <- merge(raw_data, history_data, by="visitor_id")
        }
        
        #remove redundant columns
        print("Cleaning data...")
        raw_data[, visitor_id:=NULL]
        raw_data[, session_id:=NULL]
        raw_data[, source_id:=NULL]
        raw_data[, page_id:=NULL]
        raw_data[, user_agent:=NULL]
        raw_data[, is_checkout_page:=NULL]
        
        #build model matrix
        #@this should be optimized
        print("Building model matrix")
        raw_data <- cbind(raw_data, n_events=events_distribution$n_events)
        raw_data <- rbind(raw_data, train_data)
        
        mm <- sparse.model.matrix(~. -1, raw_data)
        
        #predict
        print("Predicting...")
        pred <- predict(xgb_model, mm[1:nrow(events_distribution), ])
        
        response <- pred[2:length(pred)] %*% events_distribution$probability[2:length(pred)]
        
        #add entry in local session history
        local_session_history <- rbind(local_session_history, c(current_session, 1, pred))
      }
    } else
    {
      #set is_checkout in local session history
    }
    
    writeLines(as.character(response), con)
    
    print("Finished!")
    close(con)
  }
}

server()