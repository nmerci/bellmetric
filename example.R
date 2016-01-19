#read data from file
if(!require("data.table"))
{
    install.packages("data.table")
    library("data.table")
}

df <- fread(input="data/wa_data.csv")
