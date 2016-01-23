#load data.table package
if(!require("data.table"))
{
    install.packages("data.table")
    library("data.table")
}

#read data
df <- fread(input="data/wa_data.csv")

#fix response variable
df$is_checkout_page <- factor(df$is_checkout_page,levels=c("","t"))
levels(df$is_checkout_page) <- c(0,1)
df[, is_checkout_page:=as.numeric(as.character(df$is_checkout_page))]

#remove milliseconds
df[, time:=substr(df$time, 1, 19)]

#read time in POSIX format
ptime <- strptime(df$time, format="%F %T")

#time zone adjustion should be added
#...

#extract time features
df[, hour:=as.numeric(format(ptime, "%H"))]
df[, day_of_week:=as.numeric(format(ptime, "%w"))]
df[, day_of_month:=as.numeric(format(ptime, "%d"))]
df[, month:=as.numeric(format(ptime, "%m"))]

#sort by time
df[, seconds_since_epoch:=as.numeric(format(ptime, format="%s"))]
setorder(df, seconds_since_epoch)
df[, time:=NULL]

#extract user agent features
#...

#save processed data to file
write.csv(df, file="data/preprocessed_data.csv", row.names=F)
