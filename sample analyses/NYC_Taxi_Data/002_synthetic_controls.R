#clean up the environment
rm(list=ls())

#different home directory, depend on whether I'm using
#the University Linux Cluster or my laptop
if (Sys.info()["sysname"] == "Linux") {
  source("/home/nbb2479/Taxi/taxi/header.R")}
if (Sys.info()["sysname"] == "Windows") {
  source("C:/Users/Natasha Bhatia/Git/Sample Analysis/sample analyses/NYC_Taxi_Data/header.R")}

library("Rsolnp")
library("foreach")
library("doParallel")
################################################
# 1) Set preperiod, postperiod, out of sample period,
#    treated/route/excluded locations. 
# 2) Load data. Filter to only Manhattan zones.
#    create a pre/post period dummy. Split Uber data
#    into private and pooled sets.
#    Create a vector that holds all pre-period days.
# 3) If a subsample iteration, subsample
#     subsample.size.set of days from pre-period.
################################################

########################
#Step 1
########################

preperiod <- c("2016-07-01", "2016-11-14")
postperiod <- c("2017-01-14", "2017-05-31")
outofsampleperiod <- c("2016-06-01", "2016-06-31")
treatmentloc <- c(140, 262, 141, 263) 
treatedroute <- c(237, 163, 230, 100, 164, 186, 234, 144, 231)
excludedloc <- c(75, 236, 161, 162, 114, 113, 211)

subsample.size.set <- .8

treatedroute <- expand.grid(
  tr = treatmentloc,
  ro = treatedroute
) %>%
  mutate(route_id = paste0(tr, "_", ro),
         route_id2 = paste0(ro, "_", tr)) %>%
  select(route_id, route_id2) %>%
  unlist %>% unname
  
########################
#Step 2
########################
yellow <- 
  readRDS("data/yellow_daily.RDS") %>%
    ungroup %>%
    #break up filters into multiple steps for
    #readability
    #filter to only within Manhattan trips
    filter(Manhattanpu == TRUE & Manhattando == TRUE &
    #drop excluded zones (near Trump tower or contain
    #part time Q line stations)
          pulocationid %notin% excludedloc & 
          dolocationid %notin% excludedloc & 
    #filter to after June
          date_pu >= outofsampleperiod[1] &
          date_pu <= postperiod[2]) %>%
    #drop zones that contain full time Q line stations,
    #unless they're a treated route
    filter( (route_id %in% treatedroute) | 
            (pulocationid %notin% routeloc &
             dolocationid %notin% routeloc)) %>%
    #drop zones that contain treated zones line stations,
    #unless they're a treated route
    filter( route_id %in% treatedroute |
            (pulocationid %notin% treatmentloc & 
             dolocationid %notin% treatmentloc)) %>%
    mutate(pre = (date_pu >= preperiod[1] & 
                          date_pu <= preperiod[2]),
           post = (date_pu >= postperiod[1] & 
                           date_pu <= postperiod[2])) %>%
    mutate( route_id = 
              ifelse(route_id %in% treatedroute,
                     "treated", route_id) ) %>%
    group_by(route_id) %>%
    #filter to control units that have at least 50%
    #of days with non-zero trips 
    mutate( median = quantile(tot_trips[pre==TRUE], 0.5)) %>%
    filter(route_id == "treated" | median > 0) %>%
    #average treated route volume
    group_by(date_pu, pre, post, route_id) %>%
    summarise(tot_trips = mean(tot_trips)) %>%
    ungroup %>%
    spread(key=route_id, value=tot_trips) 

preperiod_days <- sort(
  unique(yellow$date_pu[yellow$pre==TRUE]))
postperiod_out_days <- sort(
  unique(yellow$date_pu
         [yellow$post==TRUE | 
             yellow$date_pu <= outofsampleperiod[2] ]))

########################
#Step 3
########################
synthetic_controls <- 
  function(df_arg, 
    significance.level=0.05,
    subsample=FALSE,
    subsample.size.ratio=subsample.size.set,
    starting.values = NA,
    namesuffix=subsetroutes) {

subsample.size <- 
  round(length(preperiod_days)*subsample.size.ratio, 0)

if (subsample==TRUE) {
  select_preperiod <- 
    base::sample(preperiod_days,
      size=subsample.size,
      replace=FALSE) %>% 
    sort()
  select_preperiod <- 
    data.frame(date_pu=sort(select_preperiod))
} else if (subsample==FALSE) {
  select_preperiod=
    data.frame(date_pu=preperiod_days)
}

 
########################
#Step 4
########################

#Get dataset. Create 
df <- get(df_arg) %>%
  #subsampling pre-period days
  filter(date_pu %in% select_preperiod$date_pu | 
           date_pu %in% postperiod_out_days)

df_treat <- df %>%
  select(date_pu, pre, post, treated) %>%
  rename(dv = treated) %>%
  ungroup
  
df_pre_treat <- df_treat %>%
  filter(pre == TRUE)  %>%
  arrange(date_pu)

df_cont <- df %>%
  select(-treated)

df_pre_cont_matrix <- df_cont %>%
  #subsampling days
  filter(date_pu %in% select_preperiod$date_pu) %>%
  arrange(date_pu) %>%
  select(-date_pu, -pre, -post) %>%
  as.matrix 
 
df_post_cont_matrix <- df_cont %>%
  filter(post==TRUE) %>%
  select(-date_pu, -pre, -post) %>%
  as.matrix

########################
#Step 4
########################

obj_func <- function(beta, treated_data) {
  
  beta_m<-as.matrix(beta)
  
  diff <- treated_data - df_pre_cont_matrix %*% beta 
  
  sum((diff)^2) }
  

########################
#Step 5
########################

treated_data0 <- df_pre_treat %>%
  select(dv) %>%
  unlist()

num.betas <- dim(df_pre_cont_matrix)[2]

#equality constrained
eq.const <- function(parsum, treated_data) {
      sum(parsum)
    }
lower.bound <- rep(0, num.betas)

#starting values
if (is.na(starting.values)) {
  starting.values0 <- rep(1, num.betas)/num.betas
} else {
  starting.values0 <- starting.values
}
 
results <-
  solnp(pars=starting.values0,
        fun=obj_func,
        eqfun=eq.const, 
        eqB=1,
        LB=lower.bound,
        treated_data=treated_data0,
        control=list(trace=0))

if(subsample==FALSE) {print(df_arg)}

########################
#Step 6
########################
predicted <- as.matrix(df_cont[,-c(1:3)]) %*% 
  as.matrix(results$par)
counterfactual <- predicted[df_cont$post==TRUE]
actual <- df_treat %>% 
  filter(post) %>% 
  select(dv) %>% 
  unlist
delta_t <- actual-counterfactual
treatment_effect <- mean(delta_t)
sigma <- 
  sqrt(
    sum((delta_t - treatment_effect)^2)*
      (1/length(delta_t)))

########################
#Step 7
########################

outofsample_actual <- df_treat$dv[
  df_treat$date_pu >= outofsampleperiod[1] &
  df_treat$date_pu <= outofsampleperiod[2]
]

outofsample_predict <- predicted[
  df_cont$date_pu >= outofsampleperiod[1] &
  df_cont$date_pu <= outofsampleperiod[2]]

########################
#Step 8
########################

#for export
treatment_effects<-
  data.frame(
    postperiod_actual=mean(actual),
    postperiod_predict=mean(counterfactual),
    treatment_eff=treatment_effect,
    sigma=sigma,
    outofsample_actual=mean(outofsample_actual),
    outofsample_predict=mean(outofsample_predict),
    outofsample_diff=sqrt(mean((outofsample_actual-outofsample_predict)^2))
  )

#for export
collect_predict <- data.frame(
  date_pu = unique(df$date_pu),
  predicted = predicted
)

#for export
collect_pars <- data.frame(
  control = names(df_cont)[-c(1:3)],
  par = results$par
)

########################
#Step 9
########################

name<-paste0(df_arg)

if (subsample==FALSE) {

write.csv(x=treatment_effects, 
          file=paste0("data/synthetic_control_results/", name, ".csv"))

write.csv(x=collect_pars, 
          file=paste0("data/synthetic_control_results/", name, ".csv"))

write.csv(x=collect_predict, 
          file=paste0("data/synthetic_control_results/", name, "_pr.csv"))

write.csv(x=df_treat %>% select(date_pu, dv),
          file=paste0("data/synthetic_control_results/", name, "_act.csv"))

assign(name, list(treatment_effects, collect_pars, collect_predict))
}

########################
#Step 9
########################
list(pars=collect_pars,
     estimates=treatment_effects,
     X=df_post_cont_matrix,
     num.preperiods=length(treated_data0),
     time=results$elasped)

}

########################
#Step 12
########################

for (suffix in c("")) { #}, "AM", "PM")) {
  for (df in c("via", "uberpool", "uberprivate", "yellow")) {
    print(timestamp())
    assign(x = paste0(df, suffix, "_results"), 
           val = synthetic_controls(paste0(df, suffix), 
                                    subsample=FALSE),
           envir = .GlobalEnv)
    print(timestamp())
  }
}

########################
#Step 13
########################
num.subsamples <- 5000

for (dfname in c("yellow")) {
  timestamp()
  
  synthetic_controls <- synthetic_controls
  results <- get(paste0(dfname, "_results"))
  T.1 <- results$num.preperiods
  T.2 <- nrow(results$X)
  num.treated <- ncol(results$par)-1
  m <- subsample.size.set
  
  
  #################################
  #Parallelize subsampling
  #################################
  print("start subsample")
  cl <- makeCluster(6) #not to overload your computer
  registerDoParallel(cl)
  
  subsample.results <-
    foreach(m=1:num.subsamples, 
            .packages = c("tidyverse", "Rsolnp"),
            .combine=rbind,
            .export=ls()) %dopar% {
    
    random.normals <- matrix(
      data=rnorm(T.2*num.treated, 
                 mean=0,
                 sd=1),
      ncol=num.treated) %>%
      colSums()/sqrt(T.2)
    
    subsample <- synthetic_controls(
      dfname, 
      subsample=TRUE,
      starting.values = results$pars$par)$pars
    
    subsample.inner.results <- -(sqrt(T.2/T.2))*(1/T.2)*sqrt(m)*
      colSums(results$X%*%as.matrix( (subsample-results$pars)[,-1])) +
      results$estimates$sigma * random.normals
    
    subsample.inner.results
  }
  names(subsample.results) <- colnames(results$pars)[-1]
  stopCluster(cl)

########################
#Step 14
########################  
  
  CI <- as.data.frame(matrix(data=NA,
                             nrow=22, 
                             ncol=num.treated))
  names(CI) <- names(results$par)[-1]
  
  iter <- 1
  for (alpha in seq(from=0, to=0.5, by=0.05)) {
  rank <- c(
    max(floor(num.subsamples*alpha/2), 1),
    ceiling(num.subsamples*(1-alpha/2)))
  
  #calculate CI's from subsample
  for (i in 1:num.treated) {
    CI[c(iter, iter+1),i] <- sort(subsample.results[,i])[rank]
    CI[iter,i] <- results$estimates$treatment_eff[i] - (1/sqrt(T.2))*CI[iter,i]
    CI[iter+1,i] <- results$estimates$treatment_eff[i] - (1/sqrt(T.2))*CI[iter+1,i]
    
    
  }
  iter <- iter +2
  }
  CI <- cbind(alpha=sort(rep(seq(from=0, to=0.5, by=0.05), 2)),
              CI)
  final.CI.results <-
    list(CI=CI, all_subsamples=subsample.results)

assign(paste0(dfname, "CI"), 
       final.CI.results)

write.csv(x=final.CI.results$CI, 
          file=paste0("Export/044_Results/", 
                      "CI",
                      subsetroutes,
                      dfname,
                      final.CI.results$name,"_",
                      subsample.size.set, "_",
                      num.subsamples,
                      ".csv"))
timestamp()  
}

