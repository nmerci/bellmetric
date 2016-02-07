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
city <- rep(NA, max(geoip_data$loc_id))
city[geoip_data$loc_id] <- as.character(geoip_data$city)
city[city == ""] <- NA

#fix some city codes
session_data$cloc[session_data$cloc == 59] <- 705419 #Copenhagen
session_data$cloc[session_data$cloc == 3] <- 24907 #Bern
session_data$cloc[session_data$cloc == 162] <- 16563 #Oslo
session_data$cloc[session_data$cloc == 190] <- 14355 #Stockholm

#map city code to city name
session_data[, cloc:=city[session_data$cloc]]

#rename variables
colnames(session_data)[colnames(session_data) %in% c("cc", "cloc")] <- c("country", "city")

#parse user agent column
#this is done in python code

#create visitors' history features
#this is done in python code

#prepare train data
train_data <- session_data

#remove redundant columns
train_data[, session_id:=NULL]
train_data[, visitor_id:=NULL]
train_data[, user_agent:=NULL]

#replace NA's
train_data$city[is.na(train_data$city)] <- "Other"

#remove outliers
daily_checkouts <- train_data[, .(checkouts=sum(is_checkout_page)), by="year_day"]
train_data <- train_data[!(train_data$year_day %in% 
                             daily_checkouts$year_day[daily_checkouts$checkouts < 50])] #50 is hardcode
train_data <- train_data[train_data$year_day != 331] #Black Friday

#save session_data in CSV format
write.csv(session_data, file="data/session_data.csv", row.names=F)

#save click_data in CSV format
write.csv(click_data, file="data/click_data.csv", row.names=F)

#save train_data in CSV format
write.csv(train_data, file="data/train_data.csv", row.names=F)
