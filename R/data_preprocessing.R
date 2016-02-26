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
df$is_checkout_page[df$is_checkout_page == "t"] <- "1"
df$is_checkout_page[df$is_checkout_page == ""] <- "0"

#convert time in POSIX format
df[, time:=substr(df$time, 1, 19)]
df[, time:=as.numeric(fast_strptime(df$time, format="%Y-%m-%d %H:%M:%S"))]

#slice data and sort by session/time
click_data <- df[, .(session_id, time, page_id, source_id, is_checkout_page)]
setkey(click_data, session_id, time)

#save click_data in CSV format
write.csv(click_data, file="data/click_data.csv", row.names=F)

#remove duplicated data
session_data <- unique(df[, .(session_id, visitor_id, user_agent, cc, cloc)], by="session_id")
session_data <- merge(click_data[, .(start_time=min(time), is_checkout=max(is_checkout_page)), by="session_id"],
                      session_data, by="session_id")

#add time features
ptime <- as.POSIXct(session_data$start_time, tz="UTC", origin="1970-01-01")

session_data[, hour:=as.character(format(ptime, format="%H"))]
session_data[, week_day:=as.character(format(ptime, format="%a"))]
session_data[, month_day:=as.character(format(ptime, format="%d"))]
session_data[, year_day:=as.numeric(format(ptime, format="%j"))]
session_data[, start_time:=NULL]

#parse geoip data
cities <- fread(input="data/cities.csv")
session_data[, cloc:=cities$city[session_data$cloc]]

#add number of events per session
aggregated_events <- click_data[, .(n_events=list(is_checkout_page)), by="session_id"]
session_data[, n_events:=sapply(aggregated_events$n_events, 
                                function(x) {
                                  result <- match("1", x, NA)
                                  if(is.na(result)) length(x) else result})]

#parse user agent column
session_data[, c("os", "device"):=list(NA, NA)]

session_data$device[grep("Linux|Android|Mobile|BB10|iPhone|iPod|iPad", session_data$user_agent)] <- "Mobile"
session_data$device[grep("Windows|compatible|Macintosh", session_data$user_agent)] <- "Desktop"

session_data$os[grep("Linux|Android", session_data$user_agent)] <- "Android"
session_data$os[grep("iPhone|iPod|iPad|Macintosh", session_data$user_agent)] <- "Mac_iOS"
session_data$os[grep("Windows|compatible", session_data$user_agent)] <- "Windows"

session_data[, user_agent:=NULL]

#save session_data in CSV format
write.csv(session_data, file="data/session_data.csv", row.names=F)

#create visitors' history features
visitors_history <- session_data[, .(n_sessions=length(session_id), 
                                     n_checkouts=sum(is_checkout == "1"),
                                     events_per_session=trunc(median(n_events))),
                                 by="visitor_id"]

#save visitors_history in CSV format
write.csv(visitors_history, file="data/visitors_history.csv", row.names=F)

#calculate distribution for number of events
events_distribution <- session_data[, .(probability=length(is_checkout) / nrow(session_data)), 
                                    by="n_events"]
setkey(events_distribution, n_events)

#save events_distribution in CSV format
write.csv(events_distribution, file="data/events_distribution.csv", row.names=F)

#create train data
train_data <- session_data

#add visitors' history
train_data <- merge(session_data, visitors_history, by="visitor_id")

#remove redundant columns
train_data[, session_id:=NULL]
train_data[, visitor_id:=NULL]

#replace NA's
train_data$cloc[is.na(train_data$cloc)] <- "Other"
train_data$os[is.na(train_data$os)] <- "Other"
train_data$device[is.na(train_data$device)] <- "Other"

#remove outliers
daily_checkouts <- train_data[, .(checkouts=sum(is_checkout == "1")), by="year_day"]
train_data <- train_data[!(train_data$year_day %in% daily_checkouts$year_day[daily_checkouts$checkouts < 50])]
train_data <- train_data[train_data$year_day != 331] #Black Friday

#remove sessions with the only event
train_data <- train_data[train_data$n_events > 1]

#save train_data in CSV format
write.csv(train_data, file="data/train_data.csv", row.names=F)

#concatenate pages
pages_sequence <- click_data[, .(page=list(c(page_id))), by="session_id"]
pages_sequence$page <- lapply(pages_sequence$page, append, values=0)

#save pages_sequence in CSV format
write.csv(unlist(pages_sequence$page), file="data/pages.csv", row.names=F)
