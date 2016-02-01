#load data.table package
if(!require("data.table"))
{
  install.packages("data.table")
  library("data.table")
}

#load ggplot2 package
if(!require("ggplot2"))
{
  install.packages("ggplot2")
  library("ggplot2")
}

#read data
session_data <- fread(input="data/session_data.csv")

#clean data
daily_checkouts <- session_data[, .(checkouts=sum(is_checkout_page)), by="year_day"]
session_data <- session_data[!(session_data$year_day %in% 
                               daily_checkouts$year_day[daily_checkouts$checkouts < 50])]
session_data <- session_data[session_data$year_day != 331] #Black Friday

#year trend
continuous_session_data <- session_data[session_data$year_day > 120]
daily_checkouts <- continuous_session_data[, .(checkouts=sum(is_checkout_page)), by="year_day"]

ggplot(data=daily_checkouts, aes(year_day, checkouts)) + geom_point() + geom_smooth(method="lm") +
  ggtitle("Checkouts in 2015 year") + xlab("") + 
  scale_x_continuous(breaks=c(121,unique(continuous_session_data$year_day[continuous_session_data$month_day == 1]), 366),
                     labels=c("May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec", "Jan"))

#ggsave(filename="figures/checkouts_per_year.jpeg")

#hourly rate
hourly_checkout <- session_data[, .(hour, is_checkout_page)]
hourly_checkout$hour[hourly_checkout$hour < 4] <- hourly_checkout$hour[hourly_checkout$hour < 4] + 24

ggplot(data=hourly_checkout) + 
  geom_density(aes(x=hour, fill="r"), adjust=4, alpha=0.2) +
  geom_density(aes(x=hour, fill="b"), adjust=4, alpha=0.2, data=subset(hourly_checkout, is_checkout_page == 1)) +
  scale_x_continuous(breaks=1:9 * 3, labels=c(1:7*3, 0, 3)) + 
  ggtitle("Hourly checkout rate") + xlab("Hour") +
  scale_fill_manual(name="", values=c("r"="purple", "b"="yellow"), labels=c("r"="total", "b"="checkout"))

ggsave(filename="figures/hourly_checkout_rate.jpeg")

#month day rate
month_day_checkout <- session_data[, .(month_day, is_checkout_page)]

ggplot(data=month_day_checkout) + 
  geom_density(aes(x=month_day, fill="r"), adjust=2.3, alpha=0.2) +
  geom_density(aes(x=month_day, fill="b"), adjust=1.3, alpha=0.2, data=subset(month_day_checkout, is_checkout_page == 1)) +
  scale_x_continuous(breaks=1:10 * 3) + 
  ggtitle("Month day checkout rate") + xlab("Month day") +
  scale_fill_manual(name="", values=c("r"="purple", "b"="yellow"), labels=c("r"="total", "b"="checkout"))

ggsave(filename="figures/month_day_checkout_rate.jpeg")
