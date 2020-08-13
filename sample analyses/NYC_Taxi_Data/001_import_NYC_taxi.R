#clean up the environment
rm(list=ls())

#different home directory, depend on whether I'm using
#the University Linux Cluster or my laptop
if (Sys.info()["sysname"] == "Linux") {
  source("/home/nbb2479/Taxi/taxi/header.R")}
if (Sys.info()["sysname"] == "Windows") {
  source("C:/Users/Natasha Bhatia/Git/Sample Analysis/sample analyses/NYC_Taxi_Data/header.R")}

library(sp)
library(rgdal)

################################################
# -- IMPORT DATA -- 
#1) Set directories. Outer loop over the yearly files. Load
#   taxi zones.
#2) Read in the Data. There are extra commas at the end
#of the files, so I had to ignore headers and
#   manually name columns myself. Save.
#3) Add in weekday, date information
#4) Stack the data
# -- AGGREGATE LOCATION IN PRE-JULY 2016 DATA -- 
#Data prior to July 2016 recorded pickup and dropoff location as longitude/latitude
#Data afterwards recorded aggregated location at the 
#pickup and dropoff taxi zone ID (similar to a neighborhood) level only.
#Aggregate the long/lat data from pre-July 2016 up to taxi zone
#5) Import taxi zone shapefile. Map longitude/latitude to taxi zones
#6) Merge computed taxi zones onto taxi data
#7) Filter yellow taxi data to drop rows with data quality issues.
#   Save yellow taxi data as RDS. Create a sample dataset to test code on and save that.
#8) Check whether computing taxi zones worked well. Is the number of
#   trips with missing pickup/dropoff taxi zone substantially higher in
#   early months (with imputed data) vs. later months (with raw taxi zone included)
# -- AGGREGATE DATA TO DAILY COUNTS -- 
#9) Import taxi zones file. Create a key of pu-do borough
#10) Aggregate yellow taxi data up to daily counts of total trips, AM rush trips,
#   PM rush trips, and weekend trips.
################################################

########################
#Step 1
########################
import_path <- "Data/raw/New York"
import_path <- "https://s3.amazonaws.com/nyc-tlc/trip+data"

########################
#Step 2
########################
t <- paste0("0", c(1:12))
filename <- 
  paste0("yellow_tripdata_", sort(rep(c(2016:2019), 12)), "-", substr(t, nchar(t)-1,3), ".csv")

yellow_list <- list()
sampyellow_list <- list()

for (i in c(1:length(filename))) {
print(paste0("start", " ", filename[i]))
  
#import data
datatemp<-read.csv(
  file=paste0(import_path, "/", filename[i]),
  skip=2,
  stringsAsFactors = FALSE,
  
  #If you would like to run this code, it is useful to use the  nrows option
  #to only import 1000 rows of each month's data and avoid overloading
  #your machine
  nrows=1000,
  
  header=FALSE)
#####
#column headers
#####
#remove extra commas
column_names<- 
  read.csv(
    file=paste0(import_path, "/", filename[i]),
    nrows=1, header=FALSE) %>%
  unlist() %>% 
  as.vector() %>% 
  tolower() %>%
  {gsub(" ", "", .)}

if( dim(datatemp)[2] > length(column_names) ) {
  column_names<-c(column_names, paste0("VVV", c(1:(dim(datatemp)[2] - length(column_names)))))
}

#add names
names(datatemp)<-column_names

########################
#Step 3
########################
datatemp2 <-
  datatemp %>%
  mutate(datetime_pu = as.POSIXct(tpep_pickup_datetime, tz = "EST"),
         datetime_do = as.POSIXct(tpep_dropoff_datetime, tz = "EST")) %>%
  mutate(weekday_pu = weekdays(datetime_pu),
         date_pu = as.Date(datetime_pu), 
         trip_length_min = 
           difftime(tpep_dropoff_datetime, tpep_pickup_datetime, units="mins") 
         ) %>%
  select(-starts_with("VVV"))

yellow_list[[i]] <- datatemp2

print(paste0("done", " ", filename[i]))
}

########################
#Step 4
########################

yellow_taxi <- bind_rows(yellow_list)

#clean up memory-expensive items in environment
rm(yellow_list, datatemp, datatemp2)

########################
#Step 5
########################
#import taxi zone shapefile
taxizones <- readOGR(dsn='data/maps_taxi_zones', layer='taxi_zones' )

#add row number to dataframe
#to make it seamless to merge back on taxi zones (in case
#things get sorted strangely)
yellow_taxi$merge_var <- c(1:nrow(yellow_taxi))

#calculate the taxi zones for each 
#longitude/latitude in the data
for (loctype in c("pickup", "dropoff")) {
  print(loctype)
  
  #clean dataset of coordinates in the taxi data
  #formatted the way SpatialPoints likes it
  coords <- data.frame(
    longitude = yellow_taxi[, paste0(loctype, "_longitude")], 
    latitude = yellow_taxi[, paste0(loctype, "_latitude")],
    merge_var = yellow_taxi$merge_var)
  
  #remove missings
  coords_select <- coords %>%
    filter(is.na(longitude)==FALSE)
  
  #project longitude/latitude in taxi data
  set_ll_warn(TRUE)
  points <- SpatialPoints(coords_select[,-3])
  proj4string(points) <- "+proj=longlat ++ellps=WGS84"
  
  #project taxi zone shapefile coordinates
  taxizones2 <- spTransform(taxizones, proj4string(points))
    
  #which taxi zone is a given coordinate pair located in?
  coords_select$computed <- over(points,taxizones2)$LocationID
  
  #save outputted taxi zones as "pickups" or "dropoffs" for merging later
  assign(value=coords_select, x=loctype)
  
}

################################
#Step 6
################################

#First merge together the computed taxi zone data.
#This data is smaller (since it's only for Jan-June 2016), so
#it'll avoid two huge merges in the next step.
all_zones <- pickup %>%
  rename(pulocationid_computed=computed) %>%
  left_join(dropoff, by="merge_var") %>%
  rename(dolocationid_computed=computed) %>%
  select(merge_var, pulocationid_computed, dolocationid_computed)

yellow_taxi <- yellow_taxi %>%
  left_join(all_zones, by="merge_var") %>%
  #create a flag for a computed (vs. raw data) taxi zone
  #that'll be helpful to catch bugs
  mutate(
    computed_taxi_zone = !(is.na(pickup_longitude)), 
    pulocationid = ifelse(computed_taxi_zone==1, 
                          pulocationid_computed,
                          pulocationid),
    dolocationid = ifelse(computed_taxi_zone==1,
                          dolocationid_computed,
                          dolocationid)) %>%
  select(-merge_var, -pulocationid_computed, -dolocationid_computed)


################################
#Step 7
################################
#standardize names
names(yellow_taxi) <- tolower(names(yellow_taxi))

yellow_taxi2 <- yellow_taxi %>%
  #drop zones as above
  filter(pulocationid %in% taxizones$LocationID &
           dolocationid %in% taxizones$LocationID &
           #drop trips with 0 fare
           fare_amount > 0 & 
           #drop trips with payment type = No charge, dispute, known, or voided
           payment_type %in% c(1:2)) 
#clean up memory
rm(yellow_taxi)

saveRDS(object = yellow_taxi2, 
        file="data/NewYork_Yellow0116_1219.RDS")

samp_yellow_taxi <- sample_n(
  yellow_taxi,
  size=1000*length(filename),
  replace=FALSE
  )

saveRDS(object = samp_yellow_taxi, 
        file="data/samp_NewYork_Yellow0116_1219.RDS")

################################
#Step 8
################################

#check to see whether I have an abnormal amount of missings
#in the months where I imputed taxi zone.
#I do have more but substantially more.
yellow_taxi2 %>%
  mutate(month = format(datetime_pu, "%Y-%m")) %>%
  group_by(month) %>%
  summarise(missing=sum(is.na(pulocationid) | is.na(dolocationid) | 
                          #Taxi zones 264 and 265 are "Unknown" in the 
                          #July 2016 and later files
                          pulocationid > 263 | dolocationid > 263),
            count=n()) %>%
  mutate(perc_missing = missing/count)

########################
#Step 9
########################

taxizones <- read.csv("data/taxi_zone_lookup.csv", stringsAsFactors = FALSE) %>%
  #drop Governor's Island/Ellis Island/Liberty Island since they aren't allowed
  #to have cars on them. Assume GPS error. Drop Newwark trips.
  filter(!(LocationID %in% c(103:105))  & 
           Borough %notin% c("Unknown", "EWR")) %>%
  mutate(Borough = ifelse(LocationID %in% c(132, 138), "Airport", as.character(Borough))) %>%
  select(LocationID, Borough)

#key of pickup and dropoff boroughs, to merge onto data later.
od_borough <- expand.grid(pulocationid = 
                            taxizones$LocationID, 
                          dolocationid = 
                            taxizones$LocationID) %>%
  left_join(taxizones %>% rename(puborough = Borough), by=c("pulocationid" = "LocationID")) %>%
  left_join(taxizones %>% rename(doborough = Borough), by=c("dolocationid" = "LocationID"))

########################
#Step 10
########################
daily_data <- yellow_taxi2 %>%
  mutate(trip_start_hour = as.numeric(format(datetime_pu, "%H"))) %>%
  #AM rush hour: trips between 6-9 AM on a non-holiday weekday
  #PM rush hour: trips between 4-7 PM on a non-holiday weekday
  #weekend: trips on a Saturday or Sunday
  mutate(AM_trip = 
           (weekday_pu %notin% c("Saturday", "Sunday") & 
              (trip_start_hour %in% c(6:9)) & 
              isHoliday(datetime_pu) == FALSE),
         PM_trip = 
           (weekday_pu %notin% c("Saturday", "Sunday") & 
              (trip_start_hour %in% c(16:19)) & 
              isHoliday(datetime_pu) == FALSE),
         weekend_trip = weekday_pu %in% c("Saturday", "Sunday")) %>%
  group_by(date_pu, pulocationid, dolocationid) %>%
  summarise(
    total_trips = n(),
    total_trips_AM = sum(AM_trip),
    total_trips_PM = sum(PM_trip),
    total_trips_weekend = sum(weekend_trip)
  ) %>%
  ungroup %>%
  #fill in day-pickup location-dropoff location for days that had no
  #trips between a certain location pair
  complete(date_pu,
           nesting(pulocationid, dolocationid)) %>%
  #NA's to zeros for days that had no trips
  mutate_at(vars(contains("total")), .funs = NAtoZero) %>%
  left_join(od_borough, by=c("pulocationid", "dolocationid"))


saveRDS(object = daily_data, 
        file="data/yellow_daily.RDS")
