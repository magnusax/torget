require(kernlab) # to be able to call predict

# Set year and quarter to analyze: ([1-4],[YEAR-2000])
quarterYear <- c(2,15) 

torgetDir <- "C:\\Users\\magaxels\\Desktop\\torget_segmentering\\"
forretningsanalyseDir <- "H:\\Felles\\FINN Innsikt\\Forretningsanalyse\\Magnus\\Torget Clustering 2015Q2\\"

setwd(paste(torgetDir, "torget analysis", sep=""))
source("module_torget_analysis.r")

dataFile <- paste(forretningsanalyseDir, "kjøp og salgsadferd pr brukerid og hovedkategori q314 tom q215.csv", sep="")
irDataFile <- paste(torgetDir, "source data\\", "uregBusinessQuery_allyrs.csv", sep="")

cat("==== Reading torget data ==== \n")
t0 <- proc.time()
data <- read.csv(dataFile, sep=",", encoding = "UTF-8")
cat("==== Read time =",(proc.time() - t0)[3]/60,"minutes ==== \n")
colnames(data)[1] <- "LoginId"

cat(" ==== Reading file containing flags for irregular business users ==== \n")
ureg <- read.csv(irDataFile, sep=",", encoding = "UTF-8", na.strings ="null")
colnames(ureg) <- c("LoginId", "Flag")
ureg$Flag <- as.numeric(as.factor(ureg$Flag)) # Normal users == 2, ureg BUsiness users == 1
ureg$Flag[ureg$Flag == 2] <- 0
ureg$Flag[ureg$Flag == 1] <- 1

verbose <- 1
result  <- processIncomingData(verbose, data, ureg, quarterYear)
kittens <- result[[1]]
loginid <- result[[2]]
X       <- result[[3]]

productionFile <- "svp_prod_trained_final.data"
cat("==== Loading machine learning model ==== \n")
load(productionFile)
cat("==== Model loaded. Object svp_prod created ==== \n")

cat("==== Predicting labels on new data using trained SVM model ==== \n")
labels <- predict(svp_prod, X, type="response")

cat("==== Done. Saving data to file ==== \n")
kittenFile          <- paste(torgetDir, "torgetAnalysisKittens_2015q2_corrected.csv", sep="")
productionOutputFile <- paste(torgetDir, "torgetAnalysisOutput_2015q2_corrected.csv", sep="")
output <- data.frame("login_id" = loginid, "predict_cid" = labels)
write.table(output, file = productionOutputFile, sep = ",", col.names = colnames(output))
write.table(kittens, file = kittenFile, sep = ",", col.names = colnames(kittens))
cat("==== End of script. Check outputs ==== \n")





