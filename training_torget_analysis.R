# Need this library in order to use svm
require(kernlab)

# If TRUE: print out stuff. FALSE: stay quiet during runtime
verbose <- as.logical(1) 

# Assumed input to code
this_quarter <- 1
this_year <- 15 

dataDir <- "H:\\Felles\\FINN Innsikt\\Forretningsanalyse\\Christian\\Torget Clustering Fullt år\\"
targetFile <- "20150428_Full_syntaks_CB fullyear simplified it4_A_full.csv"
dataFile <- paste(dataDir, targetFile, sep="")

t0 <- proc.time()
data <- read.csv(dataFile, sep=";", encoding = "UTF-8") # data.frame (UTF-8)
dt <- proc.time() - t0
if(verbose) cat("Read time =",dt[3]/60,"\n")
colnames(data)[1] = "LoginId" 

# ------------------------------------------------
# ---- Handle irregular business at the start ----
# ------------------------------------------------

#sourceDir <- "C:\\Users\\magaxels\\Desktop\\"
#sourceFile <- "torget_segmentering_UnregBusiness.csv"
#uregDataFile <- paste(sourceDir, sourceFile, sep="")
#ureg <- read.csv(uregDataFile, sep=",", encoding = "UTF-8", na.strings ="null")
#colnames(ureg) <- c("LoginId", "uregFlag")
#ureg$uregFlag[is.na(ureg$uregFlag)] <- 0
#ureg$uregFlag[ureg$uregFlag > 0] <- 1
# Left outer join data with ureg: Will fill missing rows in uregFlag column with NA
#data <- merge(data, ureg, by = "LoginId", all.x = TRUE)
#data$uregFlag[is.na(data$uregFlag)] <- 0
# Now remove the users that have been flagged
#if (verbose) cat("Rows before removal of unregular business =",nrow(data))
#data <- subset(data, data$uregFlag == 0)
#if (verbose) cat("Rows after removal of unregular business =",nrow(data))
#rm(ureg)

# End: Handling of irregular business

# Get the data labels in the column "ClusterNumber"
y_cb <- data.frame("cid" = data$ClusterNumber)


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
pat <-setDatePatterns(this_quarter,this_year)


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


# --- First ---
pattern <- paste("(?=.*",pat[[1]],")(?=.*kjøpsadferd)",sep="")
sum_kjop_q2 <- performAggregation(pattern, "T", data, "sum_kjop_q2")
# --- Second ---
pattern <- paste("(?=.*",pat[[2]],")(?=.*kjøpsadferd)",sep="")
sum_kjop_q3 <- performAggregation(pattern, "T", data, "sum_kjop_q3")
# --- Third ---
pattern <- paste("(?=.*",pat[[3]],")(?=.*kjøpsadferd)",sep="")
sum_kjop_q4 <- performAggregation(pattern, "T", data, "sum_kjop_q4")

# --- First ---
pattern <- paste("(?=.*",pat[[1]],")(?=.*publisering)",sep="")
sum_publ_q2 <- performAggregation(pattern, "F", data, "sum_publ_q2")
# --- Second ---
pattern <- paste("(?=.*",pat[[2]],")(?=.*publisering)",sep="")
sum_publ_q3 <- performAggregation(pattern, "F", data, "sum_publ_q3")
# --- Third ---
pattern <- paste("(?=.*",pat[[3]],")(?=.*publisering)",sep="")
sum_publ_q4 <- performAggregation(pattern, "F", data, "sum_publ_q4")


# Check for users with null acitivity during previous quarter and flag them!
activity <- data.frame("total" = rowSums(cbind(sum_publ_q4, sum_kjop_q4)))
activity[activity == 0] <- -1 # Flag kittens

hits <- nrow(subset(activity, total==-1))
if (hits == dim(activity)[1]){
  stop("Something probably went wrong.\nAll users have zero activity: Check your settings.")
}

presentPattern <- paste("(?=.*",pat[[4]],")(?=.*kjøpsadferd)",sep="")
sum_kjop_q1 <- performAggregation(presentPattern, "T", data, "sum_kjop_q1")
presentPattern <- paste("(?=.*",pat[[4]],")(?=.*publisering)",sep="")
sum_publ_q1 <- performAggregation(presentPattern, "T", data, "sum_publ_q1")

# NB: The Q1 code stamp is interpreted to mean "the present quarter"
# because of symmetry in naming conventions.

# Cumulative sum over a full year 
sum_publ_fullyear <- data.frame("cum_publ" = rowSums(cbind(sum_publ_q2, sum_publ_q3, sum_publ_q4, sum_publ_q1)))
sum_kjop_fullyear <- data.frame("cum_kjop" = rowSums(cbind(sum_kjop_q2, sum_kjop_q3, sum_kjop_q4, sum_kjop_q1)))

# Map kittens cluster label to "-1"
kittens <- subset(cbind.data.frame(sum_kjop_fullyear, sum_publ_fullyear, activity, y_cb), total==-1)
kittens <- data.frame(cum_kjop = kittens$cum_kjop, cum_publ = kittens$cum_publ, cid = -1)

dataContainer <- subset(cbind.data.frame(sum_kjop_fullyear, sum_publ_fullyear, activity, y_cb), total!=-1)

# Unreg business users have already been removed at the start of the script
# (Safety net: remove any "left over" cids = 22 from y_cb, so we avoid training on these.)
dataContainer <- subset(dataContainer, cid != 22)


sum_kjop_fullyear <- data.frame("cum_kjop" = dataContainer$cum_kjop)
sum_publ_fullyear <- data.frame("cum_publ" = dataContainer$cum_publ)
y_cb <- data.frame("cid" = dataContainer$cid) # cluster labels here
# NB: All of these variables are free from kittens! the *_fullyear data frames
# contain results aggregated over a full year now. 
rm(dataContainer)


limKjop <- quantile(sum_kjop_fullyear$cum_kjop, probs=c(0.80,0.95,0.975), na.rm=T, names=F)
limPubl <- quantile(sum_publ_fullyear$cum_publ, probs=c(0.80,0.95,0.99), na.rm=T, names=F)
if(verbose) cat("==== 97.5% cutoff value in kjop is:",limKjop[3]," ====\n")
if(verbose) cat("==== 99% cutoff value in publ is:",limPubl[3]," ====\n")

# Squish at 97.5% (kjop) and 99% (publ)
cutoff <- limKjop[3]
sum_kjop_fullyear[sum_kjop_fullyear > cutoff] <- cutoff 
cutoff <- limPubl[3]
sum_publ_fullyear[sum_publ_fullyear > cutoff] <- cutoff

# Re-normalization
normDataFrame <- function(df){
  local_min <- min(df)
  local_max <- max(df)
  cat("local min and max are:",local_min, local_max,"\n")
  if(local_max != local_min){
    df <-apply(df,2,function(x)(x-local_min)/(as.double(local_max-local_min)))
  }
  return(df)
}
norm_sum_kjop_fullyear <- data.frame( "norm_sum_kjop_fullyear" = normDataFrame(sum_kjop_fullyear))
norm_sum_publ_fullyear <- data.frame( "norm_sum_publ_fullyear" = normDataFrame(sum_publ_fullyear))

# Use mapping from CB
mapIDtoGroupID <- function(x){
  for(i in 1:length(x)){
    id <- x[i];
    temp <- 0;
    if(id==1|id==5) temp <- 1;
    if(id==2|id==9|id==10|id==15) temp <- 2;
    if(id==3|id==6|id==7|id==8|id==12|id==13|id==14|id==16|id==17|id==19|id==20) temp <- 3;
    if(id==4|id==11|id==18) temp <- 4;
    if(id==21) temp <- 5;
    if(id==22) temp <- 6;
    if(temp==0){
      x[i] <- -1;
    } 
    x[i] <- temp;
  }
  return(x);
}
clusters <- data.frame(apply(y_cb, 2, function(x) mapIDtoGroupID(x)))

if (any(clusters == -1)) stop("Stopping. Some clusters were not properly assigned.")
if (any(clusters ==  6)) stop("Stopping. Found irregular business users.")

# Hold processed data
XX <-data.frame(cbind(norm_sum_publ_fullyear, norm_sum_kjop_fullyear))
yy <- data.frame("cid" = clusters)

# Collect data again in order to make train, cv, test split
procData <- data.frame(cbind(XX,yy))

# Method that partitions the data
train_cross_test_split <- function(x, partition){
  stopifnot( sum(partition) == 1, length(partition) == 3) # Check that partitions are correct
  x <- x[sample(nrow(x)),] # shuffle data randomly
  num_data <- floor(nrow(x) * partition)
  while (sum(num_data) < nrow(x)){ 
    rand <- sample.int(3,1)
    num_data[rand] <- num_data[rand]+1
  }
  if(verbose) cat("Sum of points in split:",sum(num_data),"\n")
  if(verbose) cat(" Number of data points:",nrow(x),"\n")
  train <- data.frame(x[1:num_data[1],])
  off1 <- num_data[1]+1
  cross <- data.frame(x[off1:(off1+num_data[2]-1),])
  off2 <- (off1+num_data[2])
  test <- data.frame(x[off2:nrow(x),])
  return(list("train" = train, "cross" = cross, "test" = test))
}

# [train, cv, test] (fractions must add to one!)
dataset <-train_cross_test_split(procData, c(0.8,0.1,0.1))
train <- as.data.frame(dataset[["train"]])
cross <- as.data.frame(dataset[["cross"]])
test <- as.data.frame(dataset[["test"]])
rm(dataset)
stopifnot( sum(length(train[,1])+length(cross[,1])+length(test[,1])-nrow(procData)) == 0 )


# --------------------------------------------------------------------------------
# Support Vector Machine (ksvm from kernlab library) that estimates optimal value
# for the sigma variable that belongs to the Gaussian kernel. 
# --------------------------------------------------------------------------------
num <- nrow(train)
nsamp <- 1:num

t0 <- proc.time()
svp <- ksvm(cid~., data=train[nsamp,], type='C-svc', kernel='rbf', kpar="automatic", C=1)
t1 <- proc.time()

if(verbose) cat("Elapsed time =",(t1-t0)[3]/60,"minutes \n")
svp.pred.train <- predict(svp, train[nsamp,], type="response")
if(verbose) cat("Training accuracy =",mean(svp.pred.train==train[nsamp,3]))
svp.pred.test <- predict(svp, test, type="response")
if(verbose) cat("Test accuracy =",mean(svp.pred.test==test[,3])) 


# Should be retrained using ALL the available data (split was performed
# in order to estimate the out-of-sample error.)
if(verbose) cat("==== Retraining model using all available data ==== \n")
t0 <- proc.time()
svp_prod <- ksvm(cid~., data=procData, type='C-svc', kernel='rbf', kpar="automatic", C=1)
t1 <- proc.time()
save(svp_prod, file="C:\\Users\\magaxels\\Desktop\\svp_prod_trained_final.data")
if(verbose) cat("Elapsed training time =",(t1-t0)[3]/60,"minutes \n")
if(verbose) cat("==== Production model contained in the \"svp_prod\" object ==== \n")




rm(procData)