if (Sys.info()["sysname"] == "Linux") {
  setwd("/home/nbb2479/Taxi/") 
}
if (Sys.info()["sysname"] == "Windows") {
    setwd("C:/Users/Natasha Bhatia/Git/Sample Analysis/")  
}

#
library("tidyverse")

set.seed(1)

########################
#Date vars & week number
########################
to_date <- function(dayvar) {
  dayvar_full <- dayvar[is.na(dayvar)==FALSE]
  
  year <- as.numeric(substr(dayvar_full, 1, 4))
  month <- as.numeric(substr(dayvar_full, 5, 6))
  day <- as.numeric(substr(dayvar_full, 7, 8))
  
  full <- as.Date(paste0(year, "-", month, "-", day), optional=TRUE) 
  
  all <- as.Date(rep(NA, length(dayvar)), optional=TRUE)
  
  all[is.na(dayvar)==FALSE] <- full
  
  all
}

orderweekdays <- c("Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday")

week_num <- function(dayvar2) {
  as.numeric(paste0(format(to_date(dayvar2), '%Y'),
                    format(to_date(dayvar2), '%W')))
}

########################
#Helper functions
########################

dummy_maker <- function(df_0) {
  df_0 %>%
    mutate(treated = locationid %in% treatmentloc,
           preperiod_flag = (start_mon_day >= preperiod[1] & start_mon_day <= preperiod[2]),
           postperiod_flag = (start_mon_day >= postperiod[1] & start_mon_day <= postperiod[2]),
           studyperiod_flag = (start_mon_day >= preperiod[1] & start_mon_day <= postperiod[2]))
}

to_proper <- function(char_var) {
  temp <- gsub("(?<=\\b)([a-z])", "\\U\\1", tolower(char_var), perl=TRUE)
  temp <- gsub(" A ", " a ", temp)
  temp <- gsub(" Is ", " is ", temp)
  gsub(" In ", " in ", temp)
}

NAtoZero <- function(na_var) {
  ifelse(is.na(na_var), 0, na_var)
}

`%notin%` <- Negate(`%in%`)

filter_func <- function(df_arg2) {
  df_arg2 %>%
    filter( (Manhattanpu == TRUE & Manhattando == TRUE) |
              (Manhattanpu == TRUE & Airportdo == TRUE) | 
              (Manhattando == TRUE & Airportpu == TRUE))
}