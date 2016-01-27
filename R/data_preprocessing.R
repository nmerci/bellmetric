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

#convert time in POSIX format
df[, time:=substr(df$time, 1, 19)]
df[, time:=as.numeric(format(strptime(df$time, format="%F %T"), format="%s"))]
setorder(df, time)

#aggregate data by session
df <- merge(x=df[, .(time=list(time),
                     page_id=list(page_id),
                     source_id=list(source_id),
                     is_checkout_page=list(is_checkout_page)), by=session_id],
            y=unique(df[, .(session_id, visitor_id, user_agent, cc, cloc)], by="session_id"),
            by="session_id")

#convert to string for writing in file
#!this should be rewriten in a smart way
df[, time:=as.character(time)]
df[, page_id:=as.character(page_id)]
df[, source_id:=as.character(source_id)]
df[, is_checkout_page:=as.character(is_checkout_page)]

#save processed data to file
write.csv(df, file="data/preprocessed_data1.csv", row.names=F)
