#load data.table package
if(!require("data.table"))
{
    install.packages("data.table")
    library("data.table")
}

#load jsonlite package
if(!require("jsonlite"))
{
  install.packages("jsonlite")
  library("jsonlite")
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
df[, time:=as.numeric(format(fast_strptime(df$time, format="%Y-%m-%d %H:%M:%S"), format="%s"))]
setorder(df, time)

#slice data and sort by session/time
click_data <- df[, .(session_id, time, page_id, source_id, is_checkout_page)]
setkey(click_data, session_id, time)



#remove duplicated data
session_data <- unique(df[, .(session_id, visitor_id, user_agent, cc, cloc)], by="session_id")

#save click_data in CSV format
write.csv(click_data, file="data/click_data.csv", row.names=F)

#save session_data in CSV format
write.csv(session_data, file="data/session_data.csv", row.names=F)

#commented due to low performance
# #aggregate data by session
# aggregated_data <- df[, .(time=list(time),
#                           page_id=list(page_id),
#                           source_id=list(source_id),
#                           is_checkout_page=list(is_checkout_page)), by=session_id]
# 
# #save aggregated_data in JSON format
# write(toJSON(aggregated_data), file="data/aggregated_data.json")


