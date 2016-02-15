

server <- function()
{
  print("Starting server...")
  
  #set locale for weekday
  Sys.setlocale("LC_TIME", "C")
  
  print("Loading geoip data...")
  geoip_data <- fread(input="data/geoip_city_location.csv")
  cities <- rep(NA, max(geoip_data$loc_id))
  cities[geoip_data$loc_id] <- as.character(geoip_data$cities)
  cities[59] <- "Copenhagen"
  cities[3] <- "Bern"
  cities[162] <- "Oslo"
  cities[190] <- "Stockholm"
  cities[cities == ""] <- NA
  cities[is.na(cities)] <- "Other"
  
  print("Loading visitors' history data...")
  visitors_history <- fread("data/visitors_history.csv")
  
  print("Loading session data...")
  session_data <- fread("data/session_data.csv")
  session_data[, session_id:=NULL]
  session_data[, visitor_id:=NULL]
  session_data[, is_checkout_page:=NULL]
  
  print("Loading events distribution...")
  events_distribution <- fread("data/events_distribution.csv")
  

  
  #load everything
  #...
  
  
  while (TRUE)
  {
    PORT <- 6011
    print(paste("Listening", PORT, "..."))
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
    
    print("Process time features...")
    raw_data[, time:=substr(raw_data$time, 1, 19)]
    raw_data[, time:=as.numeric(fast_strptime(raw_data$time, format="%Y-%m-%d %H:%M:%S"))]
    
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
    
    print("Process page and source ID...")
    
    if(is.na(raw_data$source_id) == TRUE)
      raw_data$source_id <- "0"
      
    setnames(raw_data, "source_id", "first_source")
    setnames(raw_data, "page_id", "first_page")
    
    print("Process visitor's history...")
    history_data <- visitors_history[raw_data$visitor_id]
    
    if(complete.cases(history_data) == FALSE)
    {
      raw_data[, h_session_number:=0]
      raw_data[, h_events_number:=0]
      raw_data[, h_checkout_number:=0]
      raw_data[, h_mean_session_time:=0]
    }
    
    if(nrow(history_data) > 0)
    {
      merge(raw_data, history_data, by="visitor_id")
    }
    
    #parse user agent
    #...
    raw_data[, user_agent:=NULL]
    
    #remove redundant columns
    raw_data[, visitor_id:=NULL]
    raw_data[, session_id:=NULL]
    raw_data[, is_checkout_page:=NULL]

    raw_data <- cbind(raw_data, n_events=events_distribution$n_events)
    raw_data <- rbind(raw_data, session_data)
    
    mm <- sparse.model.matrix(~. -1, raw_data)
    mm[1:nrow(events_distribution), ]
    
    #TODO: finish this part
    #Return visitor history!!!
    
    writeLines(response, con)
    
    close(con)
  }
}

server()