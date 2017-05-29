library(dplyr)
library(lubridate)
library(stringr)
data<-"C:\\Users\\HariKrishna.Majety\\Documents\\TeamInd"
setwd(data)

#singleString <- readLines("gaps.csv")
i=0
inputConn<-file("situations.csv", open="r")
outputConn<-file("situations1.csv",open="a")
while(length(oneLine<-readLines(inputConn,n=1,warn = FALSE))>0)
{
  changedLine<-paste(gsub("<[^>]*>"," ",str_trim(oneLine)),"\n",sep="")
  writeLines(changedLine,outputConn)
  i=i+1
}
close(inputConn)
close(outputConn)
situation<-read.csv("situations1.csv")
View(situation)
