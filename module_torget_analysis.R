# ---- Data processing module ----


# ==== Since we are using the learned squishing parameters we do not need to change this
# ==== method (min, max) are determined by the squishing parameters.
normDataFrame <- function(df){
  local_min <- min(df)
  local_max <- max(df)
  if(local_max != local_min){
    df <-apply(df,2,function(x)(x-local_min)/(as.double(local_max-local_min)))
  }
  return(df)
}


performAggregation <- function(pattern, kpi, rawData, newColName){
  tmp <- rawData[ ,grep(pattern, colnames(rawData), perl=T)]
  tmp <- tmp[ ,grep("ant",colnames(tmp), invert=T)]
  tmp <- tmp[ ,grep("bin",colnames(tmp), invert=T)]
  if(kpi){tmp <- tmp[ ,grep("kpi",colnames(tmp), invert=T)]}
  tmp[is.na(tmp)] <-0
  sum_tmp <- data.frame(mycolname = rowSums(tmp, na.rm=T))
  colnames(sum_tmp) <- newColName
  rm(tmp)
  return(sum_tmp)
}


setDatePatterns <- function(qt, yr){
  # qt = quarter [1,4]. Integer.
  # yr = year [# years after 2000] (e.g. 2015 -> yr = 15). Integer.
  if (qt==1){
    q1 <- paste("_q2",as.character(yr-1),sep="")
    q2 <- paste("_q3",as.character(yr-1),sep="")
    qc <- paste("_q4",as.character(yr-1),sep="") # Kittens check in "qc" quarter (c~'check')
    qn <- paste("_q1",as.character(yr),sep="")   # current quarter (n~'now')
  }
  else if(qt==2){
    q1 <- paste("_q3",as.character(yr-1),sep="")
    q2 <- paste("_q4",as.character(yr-1),sep="")
    qc <- paste("_q1",as.character(yr),sep="") 
    qn <- paste("_q2",as.character(yr),sep="")  
  }
  else if(qt==3){
    q1 <- paste("_q4",as.character(yr-1),sep="")
    q2 <- paste("_q1",as.character(yr),sep="")
    qc <- paste("_q2",as.character(yr),sep="") 
    qn <- paste("_q3",as.character(yr),sep="") 
  }
  else if(qt==4){
    q1 <- paste("_q1",as.character(yr),sep="")
    q2 <- paste("_q2",as.character(yr),sep="")
    qc <- paste("_q3",as.character(yr),sep="") 
    qn <- paste("_q4",as.character(yr),sep="") 
  }
  return(list(q1,q2,qc,qn))
}


processIncomingData <- function(verbose, data, ureg, time){
  
  if (verbose) cat("Preprocessing data for quarter =",time[1],"and year =",time[2],"\n")
  
  data <- merge(data, ureg, by = "LoginId", all.x = TRUE)
  data <- data[data$LoginId > 1000000, ] # Remove "weird" records where loginid does not make sense
  data$Flag[is.na(data$Flag)] <- 0 # If NA we keep (should be very few records)
  
  if (verbose) cat("==== Checking merge results ==== \n")
  
  # Now remove the users that have been flagged
  if (verbose) cat("Rows before removal of unregular business =",nrow(data),"\n")
  
  data <- subset(data, data$Flag == 0)
  
  if (verbose) cat("Rows after removal of unregular business =",nrow(data),"\n")
  if (verbose) cat("==== End checking merge results ==== \n")
  
  pat <-setDatePatterns(time[1],time[2])
  
  # Get LoginId column as well
  logid <- data.frame( "loginid" = data$LoginId )
  
  if (verbose) cat("==== Performing aggregation ==== \n")
  pattern <- paste("(?=.*",pat[[1]],")(?=.*kjøpsadferd)",sep="")
  sum_kjop_q2 <- performAggregation(pattern, "T", data, "sum_kjop_q2")
  pattern <- paste("(?=.*",pat[[2]],")(?=.*kjøpsadferd)",sep="")
  sum_kjop_q3 <- performAggregation(pattern, "T", data, "sum_kjop_q3")
  pattern <- paste("(?=.*",pat[[3]],")(?=.*kjøpsadferd)",sep="")
  sum_kjop_q4 <- performAggregation(pattern, "T", data, "sum_kjop_q4")
  
  pattern <- paste("(?=.*",pat[[1]],")(?=.*publisering)",sep="")
  sum_publ_q2 <- performAggregation(pattern, "F", data, "sum_publ_q2")
  pattern <- paste("(?=.*",pat[[2]],")(?=.*publisering)",sep="")
  sum_publ_q3 <- performAggregation(pattern, "F", data, "sum_publ_q3")
  pattern <- paste("(?=.*",pat[[3]],")(?=.*publisering)",sep="")
  sum_publ_q4 <- performAggregation(pattern, "F", data, "sum_publ_q4")
  
  if (verbose) cat("Checking activity in time stamp:", pat[[3]], "\n")
  activity <- data.frame("total" = rowSums(cbind(sum_publ_q4, sum_kjop_q4)))
  activity[activity == 0] <- -1 
  hits <- nrow(subset(activity, total==-1))
  if (hits == dim(activity)[1]){
    stop("Something probably went wrong.\nAll users have zero activity.")
  }
  
  presentPattern <- paste("(?=.*",pat[[4]],")(?=.*kjøpsadferd)",sep="")
  sum_kjop_q1 <- performAggregation(presentPattern, "T", data, "sum_kjop_q1")
  presentPattern <- paste("(?=.*",pat[[4]],")(?=.*publisering)",sep="")
  sum_publ_q1 <- performAggregation(presentPattern, "T", data, "sum_publ_q1")
  
  sum_publ_fullyear <- data.frame("cum_publ" = rowSums(cbind(sum_publ_q2, sum_publ_q3, sum_publ_q4, sum_publ_q1)))
  sum_kjop_fullyear <- data.frame("cum_kjop" = rowSums(cbind(sum_kjop_q2, sum_kjop_q3, sum_kjop_q4, sum_kjop_q1)))
  
  if (verbose) cat("==== Checking for kittens ==== \n")
  kittens <- subset(cbind.data.frame(logid, sum_kjop_fullyear, sum_publ_fullyear, activity), total==-1)
  kittens <- data.frame(loginid = kittens$loginid, cum_kjop = kittens$cum_kjop, cum_publ = kittens$cum_publ, cid = -1)
  
  dataContainer <- subset(cbind.data.frame(logid, sum_kjop_fullyear, sum_publ_fullyear, activity), total != -1)

  sum_kjop_fullyear <- data.frame("cum_kjop" = dataContainer$cum_kjop)
  sum_publ_fullyear <- data.frame("cum_publ" = dataContainer$cum_publ)
  logid <- data.frame("loginid" = dataContainer$loginid)
  
  rm(dataContainer)
  
  #if (verbose) cat("==== Calculating quantiles ==== \n")
  #limKjop <- quantile(sum_kjop_fullyear$cum_kjop, probs=c(0.80,0.95,0.975), na.rm=T, names=F)
  #limPubl <- quantile(sum_publ_fullyear$cum_publ, probs=c(0.80,0.95,0.99), na.rm=T, names=F)
  #if (verbose) cat("==== 97.5% cutoff value in kjop is:",limKjop[3]," ====\n")
  #if (verbose) cat("==== 99% cutoff value in publ is:",limPubl[3]," ====\n")
  
  learnedCutoffKjop <- 165. # learned parameter! Do not change in future analyses!
  sum_kjop_fullyear[sum_kjop_fullyear > learnedCutoffKjop] <- learnedCutoffKjop
  
  learnedCutoffPubl <- 60. # learned parameter! Do not change in future analyses!
  sum_publ_fullyear[sum_publ_fullyear > learnedCutoffPubl] <- learnedCutoffPubl
  
  # Using learned max and min values from the training of the model when normalizing
  norm_sum_kjop_fullyear <- data.frame( "norm_sum_kjop_fullyear" = normDataFrame(sum_kjop_fullyear))
  norm_sum_publ_fullyear <- data.frame( "norm_sum_publ_fullyear" = normDataFrame(sum_publ_fullyear))
  
  XX <-data.frame(cbind(norm_sum_publ_fullyear, norm_sum_kjop_fullyear))
  
  if (verbose) cat("==== Returning from function ==== \n")
  return(list(kittens, logid, XX))
}