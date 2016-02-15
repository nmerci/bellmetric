#load data.table package
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

#set locale for weekday
Sys.setlocale("LC_TIME", "C")

#read data
df <- fread(input="data/wa_data.csv")

#fix response variable
df$is_checkout_page <- factor(df$is_checkout_page,levels=c("","t"))
levels(df$is_checkout_page) <- c(0,1)
df[, is_checkout_page:=as.numeric(as.character(df$is_checkout_page))]

#convert time in POSIX format
df[, time:=substr(df$time, 1, 19)]
df[, time:=as.numeric(fast_strptime(df$time, format="%Y-%m-%d %H:%M:%S"))]
setorder(df, time)

#slice data and sort by session/time
click_data <- df[, .(session_id, time, page_id, source_id, is_checkout_page)]
setkey(click_data, session_id, time)

#save click_data in CSV format
write.csv(click_data, file="data/click_data.csv", row.names=F)

#remove duplicated data
session_data <- unique(df[, .(session_id, visitor_id, user_agent, cc, cloc)], by="session_id")
session_data <- merge(click_data[, .(start_time=min(time), is_checkout_page=max(is_checkout_page)), by="session_id"],
                      session_data, by="session_id")

#add time features
ptime <- as.POSIXct(session_data$start_time, tz="UTC", origin="1970-01-01")
session_data[, hour:=as.character(format(ptime, format="%H"))]
session_data[, week_day:=as.character(format(ptime, format="%a"))]
session_data[, month_day:=as.character(format(ptime, format="%d"))]
session_data[, year_day:=as.numeric(format(ptime, format="%j"))]
session_data[, start_time:=NULL]

#parse geoip data
geoip_data <- fread(input="data/geoip_city_location.csv")
cities <- rep(NA, max(geoip_data$loc_id))
cities[geoip_data$loc_id] <- as.character(geoip_data$city)
cities[cities == ""] <- NA

#fix some cities codes
session_data$cloc[session_data$cloc == 59] <- 705419 #Copenhagen
session_data$cloc[session_data$cloc == 3] <- 24907 #Bern
session_data$cloc[session_data$cloc == 162] <- 16563 #Oslo
session_data$cloc[session_data$cloc == 190] <- 14355 #Stockholm

#map cities code to cities name
session_data[, cloc:=cities[session_data$cloc]]

#rename variables
colnames(session_data)[colnames(session_data) %in% c("cc", "cloc")] <- c("country", "city")

#add first source and page id's
first_source_page <- unique(click_data[, .(session_id, first_source=as.character(source_id), 
                                                       first_page=as.character(page_id))], 
                            by="session_id")

session_data <- merge(session_data, first_source_page, by="session_id")

#add number of events per session
n_events <- click_data[, .(n_events=length(is_checkout_page)), by="session_id"]
session_data <- merge(session_data, n_events, by="session_id")

#parse user agent column
#this is done in python code
#TODO: implement it here or use rPython



session_data[, user_agent:=NULL]

#create visitors' history features
#this is done in python code

#save session_data in CSV format
write.csv(session_data, file="data/session_data.csv", row.names=F)

#calculate distribution for number of events
events_distribution <- session_data[, .(probability=length(is_checkout_page) / nrow(session_data)), 
                                    by="n_events"]
setkey(events_distribution, n_events)

#save events_distribution in CSV format
write.csv(events_distribution, file="data/events_distribution.csv", row.names=F)

#concatenate pages
pages_sequence <- click_data[, .(page=list(c(page_id))), by="session_id"]
pages_sequence$page <- lapply(pages_sequence$page, append, values=0)

#save pages_sequence in CSV format
write.csv(unlist(pages_sequence$page), file="data/pages.csv", row.names=F)
