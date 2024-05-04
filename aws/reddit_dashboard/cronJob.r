#cron_add(command=cron_rscript("/srv/shiny-server/reddit_dashboard/cronJob.r"), frequency="daily",at="04:00",id='reddit-qs-1')
# Rscript /srv/shiny-server/reddit_dashboard/cronJob.r


require(dplyr)
require(aws.s3)
require(readr)
require(data.table)
require(paws)
require(DBI)
require(lubridate)

readRenviron("/srv/shiny-server/reddit_dashboard/.Renviron")

# Get labeled data from S3
dfBucket <- get_bucket_df('aws-emr-studio-533267180383-ap-southeast-1', 'output/')
listFile_path <- dfBucket$Key
matches <- sapply(listFile_path, function(x) grepl("\\.csv$", x))
csv_path <- listFile_path[matches][1]
dfRaw <- fread(text = rawToChar(get_object(object = paste0("s3://aws-emr-studio-533267180383-ap-southeast-1/",csv_path), show_progress = TRUE)))  

# Clean data
dfRaw_cleaned <- dfRaw[!is.na(prediction), ]
dfRaw_cleaned <- dfRaw_cleaned[grepl("^2024", dfRaw_cleaned$timestamp), ]
dfRaw_cleaned$cmid_length <- nchar(as.character(dfRaw_cleaned$comment_id))
dfRaw_cleaned$dt_length <- nchar(as.character(dfRaw_cleaned$date))
dfRaw_cleaned <- dfRaw_cleaned %>% filter( cmid_length==7 & dt_length==10)
dfRaw_cleaned$timestamp <- as.POSIXct(dfRaw_cleaned$timestamp, format = "%Y-%m-%d %H:%M:%OS")
dfRaw_cleaned$timestamp_local <- dfRaw_cleaned$timestamp + 8 * 60 * 60
dfRaw_cleaned$date_local <- as.Date(format(dfRaw_cleaned$timestamp_local, "%Y-%m-%d"))
dfRaw_cleaned$hour_local <- format(dfRaw_cleaned$timestamp_local, "%H")
dfRaw_cleaned_final <- dfRaw_cleaned %>% select(-timestamp,-body,-submission,-date,-cmid_length,-dt_length)
dfRaw_cleaned_final$timestamp_local <- as.character(dfRaw_cleaned_final$timestamp_local)

saveRDS(dfRaw_cleaned_final, file = "/srv/shiny-server/reddit_dashboard/labeled_data_final.rds")


