library(tm)

wd<-"C:\\Users\\hariKrishna.majety\\Documents\\Products\\Project 1"
setwd(wd)

text<-readLines("cleanedSituationGapQuestion.csv")
docs<-Corpus(VectorSource(text))
outputConn<-file("cleanedSitGapQues4.txt",open="w+")
#outputConn2<-file("cleanedSitGapQues5.txt",open="w+")

stopwords2 = c("ax","i","you","edu","s","t","m","subject","can","lines","re",
                   "what","there","all","we","one","the","a","an","of","or","in","for","by","on",
                   "but", "is", "in","a","not","with", "as", "was", "if","they", "are", "this", 
                   "and", "it", "have", "from", "at", "my","be","by","not", "that", "to","from",
                   "com","org","like","likes","so","team","client","want","wants","understand","able","will","different","need",
                    "based","better","understanding","need","across","right","left","appropriate","improved","using","reduce",
                    "increase","decrease","understands")

#docs[[1]][1]
docs<-tm_map(docs,tolower)
docs<-tm_map(docs,removeWords,stopwords2)
docs<-tm_map(docs,removeWords,stopwords("english"))
#docs[[1]][1]

i<-1
while(i<=length(docs))
{
  writeLines(docs[[i]][[1]],outputConn)
  #writeLines(docs2[[i]][[1]],outputConn2)
  i<-i+1
}
close(outputConn)
#close(outputConn2)
