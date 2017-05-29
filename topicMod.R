library(tm)
library(topicmodels)
library(dplyr)
library(stringr)

wd<-"C:\\Users\\hariKrishna.majety\\Documents\\Products\\Project 1"
wd2<- "C:\\Users\\hariKrishna.majety\\Documents\\Products\\Project 1\\inputData"
setwd(wd)

docs<-Corpus(DirSource(wd2))
dtm<-DocumentTermMatrix(docs)

inputText<-read.csv("cleanedSitGapQues4.txt",stringsAsFactors = FALSE)
inp1<-inputText%>%select(situation)



# 
# inpSplit<-strsplit(inp1$situation," ")
# 
# #unique(strsplit(c("THis is a sentence", "THis is second sentence")," ")[[1]])
# #lapply(inpSplit,strsplit())
# 
# #b<-c("THis is a sentence is is of", "THis is second sentence")
# #sapply(d,unique)
# 
# uniqueVec<-sapply(inpSplit,unique)

wordCount = function(sentence,word){
  
  #print(sentence)
  #print(word)
  #print("is the word \n")
  splitList<-strsplit(sentence," ")
  #print(splitList)
  splitedVectorString <- unlist(splitList)
  #print(splitedVectorString)
  counter<-1
  count<-c()
  while(counter<=length(word))
  {
    count[counter] <- sum(word[counter] == splitedVectorString)
    counter<-counter+1
  }
  return(count)

}

taggedCSVtoDTM<-function(inputCSV,columnNumber=c(1))
{
  #columnNumber=c(3,4,5)
  inputCSVModified<-inputCSV%>%select(columnNumber)
  #splitCSV<-strsplit(inputCSV1[[1]]," ")
  #uniqueVec<-sapply(splitCSV,unique)
  #print(inputCSVModified[1,])
  if(length(columnNumber)!=1)
  {
    print("\nblock 1\n")
    rowCombinedCSV<-apply(inputCSVModified,MARGIN = 1,paste,collapse= " ")
  }  else
  {
    print("\n block 2\n")
    rowCombinedCSV<-inputCSVModified
  }
  #print("\n heres something\n")
  #print(rowCombinedCSV[1])
  #print("\n end of row Combined\n")
  CSVAsString<-paste(rowCombinedCSV,collapse = " ")
  #print(CSVAsString)
  combinedCSVArray<-strsplit(CSVAsString," ")
  uniqueVec<-unique(combinedCSVArray[[1]])
  cat(length(uniqueVec), " is the length of the unique Vec\n")
  dtm<-sapply(uniqueVec,wordCount,sentence=rowCombinedCSV[1])
  #cat(dtm,"\n")
  i<-2
  while(i<=length(rowCombinedCSV))
  {
    dtm2<-sapply(uniqueVec,wordCount,sentence=rowCombinedCSV[i])
    #cat("\n",dtm2,"\n")
    dtm<-rbind(dtm,dtm2)
    #cat(dtm,"\n")
    i<-i+1
  }
  return(dtm);
}

dtm2<-taggedCSVtoDTM(inputText,c(3,4,5))

burnin <- 4000
iter <- 2000
thin <- 500
seed <-list(2003,5,63,100001,765)
nstart <- 5
best <- TRUE

# docs<-Corpus(DirSource(wd))
# dtm<-DocumentTermMatrix(docs)

inspect(dtm2[1:5, 1:20])

#Number of topics
k <- 9

#Run LDA using Gibbs sampling
ldaOut <- LDA(dtm2,k, method="Gibbs", control=list(nstart=nstart, seed = seed, best=best, burnin = burnin, iter = iter, thin=thin))

#write out results
#docs to topics
ldaOut.topics <- as.matrix(topics(ldaOut))

#top 6 terms in each topic
ldaOut.terms <- as.matrix(terms(ldaOut,10))
