library(wordVectors)
library(h2o)
library(magrittr)
library(tm)

data<-"C:\\Users\\HariKrishna.Majety\\Documents\\TeamInd\\ProjData\\codes\\h2oCodes\\"
setwd(data)
text<-readLines("cleanedInput3.txt")
docs<-Corpus(VectorSource(text))
outputConn<-file("cleanedInput4.txt",open="w+")
outputConn2<-file("cleanedInput5.txt",open="w+")

#docs[[1]][1]
docs<-tm_map(docs,removeWords,stopwords2)
docs2<-tm_map(docs,removeWords,stopwords("english"))
#docs[[1]][1]

i<-1
while(i<=length(docs))
{
  writeLines(docs[[i]][[1]],outputConn)
  writeLines(docs2[[i]][[1]],outputConn2)
  i<-i+1
}
close(outputConn)
close(outputConn2)
# model = train_word2vec("cleanedInput.txt",output="cleanedInput.bin",threads = 3,vectors = 3,window=12)
# model2 = train_word2vec("cleanedInput2.txt",output="cleanedInput2.bin",threads = 3,vectors = 100,window=12)
model3 <- train_word2vec("cleanedInput4.txt",output="cleanedInput4.bin",threads = 3,vectors = 100,window=12,force = TRUE)
model5<- train_word2vec("cleanedInput5.txt",output="cleanedInput5.bin",threads = 3,vectors =100,window=12,force = TRUE)

phraseToVector<-function(inputText,vectorModel)
{
  inputVector<-strsplit(inputText," ")[[1]]
  #print(inputVector)
  n<-1
  outputVector<-0
  while(n<=length(inputVector))
  {
    #print(inputVector[n])
    #print(vectorModel[[as.numeric(inputVector[n])]])
    #print(vectorModel[[inputVector[n]]])
    #print("The matrix is here")
    #print(cbind(outputVector,as.vector(vectorModel[[inputVector[n]]])))
    outputVector<-rowSums(cbind(outputVector,as.vector(vectorModel[[inputVector[n]]])),na.rm = TRUE)
    #print("The Final matrix")
    #print(outputVector)
    n<-n+1
  }
  return(outputVector)
}

inpdata<-read.csv("actualData/sitVer.csv",stringsAsFactors = FALSE)
rowCount<-nrow(inpdata)
dataMatrix<-matrix(0,nrow = rowCount,ncol = 100)


i<-1
while(i<=rowCount)
{
  dataMatrix[i,]<-phraseToVector(inpdata[i,2],model5)
  print(dataMatrix[i,])
  i<-i+1
}
#onlyMatrix<-data.frame(dataMatrix)
finalFrame<-data.frame(class=inpdata[,1],dataMatrix)

localH2O = h2o.init(ip = 'localhost', port = 54321)
h2o.clusterInfo()

finalh2oFrame<-as.h2o(finalFrame)
splitCriteria<-0.75
finalh2oFrameSplit<-h2o.splitFrame(finalh2oFrame,ratios = c(splitCriteria,1-splitCriteria-0.01))
situationModel<- h2o.gbm(x = 2:101, y = 1, training_frame = finalh2oFrame,ntrees = 50,min_rows = 2)
situationPredictions<-h2o.predict(situationModel,finalh2oFrame)

getMatchPercentage<-function(vector1,vector2)
{
  one<-length(vector1)
  two<-length(vector2)
  if(one!=two)
  {
    stop("Vectors are of different lengths")
  }
  i<-1
  count<-0
  while(i<=one)
  {
    if(vector1[i]==vector2[i])
    {
      count<-count+1
    }
    i<-i+1
  }
  return(count*100/one)
}
getMatchPercentage(as.vector(situationPredictions[,1]),as.vector(finalFrame[,1]))
as.vector(situationPredictions[,1])
as.vector(finalFrame[,1])

situationDeepLearningModel<- h2o.deeplearning(x = 2:101, y = 1, training_frame = finalh2oFrame)
situationDeepLearningPredictions<-h2o.predict(situationDeepLearningModel,finalh2oFrame)
getMatchPercentage(as.vector(situationDeepLearningPredictions[,1]),as.vector(finalFrame[,1]))

splitCriteria<-0.97
finalh2oFrameSplit2<-h2o.splitFrame(finalh2oFrame,ratios = c(splitCriteria,1-splitCriteria-0.01))

situationModel2<- h2o.gbm(x = 2:101, y = 1, training_frame = finalh2oFrameSplit2[[1]],ntrees = 50,min_rows = 2)
situationPredictions2<-h2o.predict(situationModel2,finalh2oFrameSplit2[[2]])
getMatchPercentage(as.vector(situationPredictions2[,1]),as.vector(finalh2oFrameSplit2[[2]][,1]))

getPredictions<-function(inputText,WordVecModel,predictionModel)
{
  phraseVector<-phraseToVector(inputText,WordVecModel)
  phraseVectorMatrix<-matrix(phraseVector,nrow = 1,ncol=length(phraseVector))
  phraseVectorDataFrame<-data.frame(class="Others",phraseVectorMatrix)
  #print(phraseVectorDataFrame)
  phraseVectorh2oFrame<-as.h2o(phraseVectorDataFrame)
  #print(phraseVectorh2oFrame)
  prediction<-h2o.predict(predictionModel,phraseVectorh2oFrame)
  return(prediction)
}
getPredictions("Marketing Finance team of a small retailer would like to organize sales campaigns across his stores",model5,situationModel)
