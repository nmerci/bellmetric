server <- function()
{
  print("Starting server...")
  PORT <- 6011
  
  #set locale for weekday
  Sys.setlocale("LC_TIME", "C")
  
  
  print("Loading geoip data...")
  geoip_data <- fread(input="data/geoip_city_location.csv")
  cities <- rep(NA, max(geoip_data$loc_id))
  cities[geoip_data$loc_id] <- as.character(geoip_data$city)
  cities[59] <- "Copenhagen"
  cities[3] <- "Bern"
  cities[162] <- "Oslo"
  cities[190] <- "Stockholm"
  cities[cities == ""] <- NA
  cities[is.na(cities)] <- "Other"
  
  print("Loading visitors' history data...")
  visitors_history <- fread("data/visitors_history.csv")
  setkey(visitors_history, visitor_id)
  
  print("Loading session data...")
  train_data <- fread("data/train_data.csv")
  train_data[, is_checkout_page:=NULL]
  
  print("Loading events distribution...")
  events_distribution <- fread("data/events_distribution.csv")
  
  print("Loading xgboost model...")
  xgb_model <- xgb.load("R/xgb_model")
  
  local_session_history <- matrix(numeric(), 0, 2 + nrow(events_distribution))
  colnames(local_session_history) <- c("session_id", "n_past_events", events_distribution$n_events)

  while (TRUE)
  {
    print(paste("Listening port", PORT, "..."))
    con <- socketConnection(host = "localhost", port = PORT, blocking = TRUE, 
                            server = TRUE, open = "r+")
    
    raw_data <- readLines(con, 1)
    print(paste("New event:", raw_data))
    
    raw_data <- paste(toString(c("visitor_id", "session_id", "time", "page_id", "source_id", 
                                 "user_agent", "is_checkout_page", "cc", "cloc")), 
                      "\n", raw_data)
    
    raw_data <- as.data.table(read.csv(text=raw_data))
    
    #save current session
    current_session <- raw_data$session_id
    response <- 0
    
    if(is.element(current_session, local_session_history[, "session_id"]))
    {
      #get number of events
      n_past_events <- local_session_history[local_session_history[, "session_id"] == current_session, 
                                             "n_past_events"]
      
      #increment number of events
      local_session_history[local_session_history[, "session_id"] == current_session, 
                            "n_past_events"] <- n_past_events + 1
      
      #adjust probability distribution
      current_distribution <- events_distribution$probability
      current_distribution[1:n_past_events] <- 0
      current_distribution <- current_distribution / sum(current_distribution)
      current_distribution[n_past_events + 1] <- 0
      
      #write result
      response <- local_session_history[local_session_history[, "session_id"] == current_session, 
                                        3:ncol(local_session_history)] %*% current_distribution
    } else
    {
      print("Process time features...")
      raw_data[, time:=substr(raw_data$time, 1, 19)]
      raw_data[, time:=as.numeric(strptime(raw_data$time, format="%Y-%m-%d %H:%M:%S"))]
      
      ptime <- as.POSIXct(raw_data$time, tz="UTC", origin="1970-01-01")
      raw_data[, hour:=as.character(format(ptime, format="%H"))]
      raw_data[, week_day:=as.character(format(ptime, format="%a"))]
      raw_data[, month_day:=as.character(format(ptime, format="%d"))]
      raw_data[, year_day:=as.numeric(format(ptime, format="%j"))]
      raw_data[, time:=NULL]
      
      print("Process city...")
      raw_data$cloc <- cities[raw_data$cloc]
      
      #rename columns
      setnames(raw_data, "cc", "country")
      setnames(raw_data, "cloc", "city")
      
      print("Process visitor's history...")
      history_data <- visitors_history[visitor_id == raw_data$visitor_id]
      
      if(complete.cases(history_data) == FALSE)
      {
        raw_data[, h_session_number:=0]
        raw_data[, h_events_number:=0]
        raw_data[, h_checkout_number:=0]
        raw_data[, h_mean_session_time:=0]
      } else
      {
        raw_data <- merge(raw_data, history_data, by="visitor_id")
      }
      
      #parse user agent
      #...
      
      #remove redundant columns
      raw_data[, visitor_id:=NULL]
      raw_data[, session_id:=NULL]
      raw_data[, source_id:=NULL]
      raw_data[, page_id:=NULL]
      raw_data[, user_agent:=NULL]
      raw_data[, is_checkout_page:=NULL]
      
      #build model matrix
      raw_data <- cbind(raw_data, n_events=events_distribution$n_events)
      raw_data <- rbind(raw_data, train_data)
      
      mm <- sparse.model.matrix(~. -1, raw_data)
      
      #predict
      pred <- predict(xgb_model, mm[1:nrow(events_distribution), ])
      
      #add entry in local session history
      local_session_history <- rbind(local_session_history, c(current_session, 1, pred))
        
      response <- pred[2:length(pred)] %*% events_distribution$probability[2:length(pred)]
    }
    
    writeLines(response, con)
    
    print("Finished!")
    close(con)
  }
}

server()