import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.feature.Word2Vec
import org.apache.spark.mllib.feature.Word2VecModel
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib
import org.apache.spark.h2o._
import org.apache.spark.examples.h2o._
import org.apache.spark.sql._

def sumArray (m: Array[Double], n: Array[Double]): Array[Double] = {
  for (i <- 0 until m.length) {m(i) = n(i)}
  return m
}

def divArray (m: Array[Double], divisor: Double) : Array[Double] = {
  for (i <- 0 until m.length) {m(i) /= divisor}
  return m
}

def wordToVector (w:String, m: Word2VecModel): Vector = {
  try {
    return m.transform(w)
  } catch {
    case e: Exception => return Vectors.zeros(100)
  }  
}

case class CRAIGSLIST(target: String, a: mllib.linalg.Vector)

//------------------------ Frame 1 -------------------------------------------------------

var rawJobTitles = sc.textFile("train1.csv")

var data = rawJobTitles.map(d => d.split(','))

var label = data.map(l => l(0))
var jobTitles = data.map(l => l(1))
var labelCounts = label.map(n => (n, 1)).reduceByKey(_+_).collect.mkString("\n")

var stopwords = Set("ax","i","you","edu","s","t","m","subject","can","lines","re","what"
    ,"there","all","we","one","the","a","an","of","or","in","for","by","on"
    ,"but", "is", "in","a","not","with", "as", "was", "if","they", "are", "this", "and", "it", "have"
    , "from", "at", "my","be","by","not", "that", "to","from","com","org","like","likes","so")

var nonWordSplit = jobTitles.flatMap(t => t.split("""\W""").map(_.toLowerCase))
var filterNumbers = nonWordSplit.filter(word => """[^0-9]*""".r.pattern.matcher(word).matches)
var wordCounts = filterNumbers.map(w => (w, 1)).reduceByKey(_+_)
var rareWords = wordCounts.filter{ case (k, v) => v < 2 }.map {case (k, v) => k }.collect.toSet

def token(line: String): Seq[String] = {
    line.split("""\W""") //get rid of nonWords such as puncutation as opposed to splitting by just " "
    .map(_.toLowerCase)
    .filter(word => """[^0-9]*""".r.pattern.matcher(word).matches) //remove mix of wordsnumbers
    .filterNot(word => stopwords.contains(word)) // remove stopwords defined above (you can add to this list if you want)
    .filter(word => word.size >= 2) // leave only words greater than 1 characters. This deletes A LOT of words but useful to reduce our feature-set
    .filterNot(word => rareWords.contains(word)) // remove rare occurences of words
}

var XXXwords = data.map(d => (d(0), token(d(1)).toSeq)).filter(s => s._2.length > 0)
var words = XXXwords.map(v => v._2)
var XXXlabels = XXXwords.map(v => v._1)

println(jobTitles.flatMap(lines => token(lines)).distinct.count) 


var word2vec = new Word2Vec()
var model = word2vec.fit(words)

var title_vectors = words.map(x => new DenseVector(divArray(x.map(m => wordToVector(m, model).toArray).reduceLeft(sumArray),x.length)).asInstanceOf[Vector])
var title_pairs = words.map(x => (x,new DenseVector(divArray(x.map(m => wordToVector(m, model).toArray).reduceLeft(sumArray),x.length)).asInstanceOf[Vector]))


var h2oContext = new H2OContext(sc).start()
import h2oContext._
implicit var sqlContext = new SQLContext(sc)
import sqlContext._
var resultRDD:SchemaRDD = XXXlabels.zip(title_vectors).map(v => CRAIGSLIST(v._1, v._2)).toDF

var train1:H2OFrame = h2oContext.asH2OFrame(resultRDD,"train1")
train1.replace(train1.find("target"),train1.vec("target").toCategoricalVec).remove()


//------------------------Frame 2-----------------------------------
rawJobTitles = sc.textFile("train2.csv")

data = rawJobTitles.map(d => d.split(','))

label = data.map(l => l(0))
jobTitles = data.map(l => l(1))
labelCounts = label.map(n => (n, 1)).reduceByKey(_+_).collect.mkString("\n")

stopwords = Set("ax","i","you","edu","s","t","m","subject","can","lines","re","what"
    ,"there","all","we","one","the","a","an","of","or","in","for","by","on"
    ,"but", "is", "in","a","not","with", "as", "was", "if","they", "are", "this", "and", "it", "have"
    , "from", "at", "my","be","by","not", "that", "to","from","com","org","like","likes","so")

nonWordSplit = jobTitles.flatMap(t => t.split("""\W""").map(_.toLowerCase))
filterNumbers = nonWordSplit.filter(word => """[^0-9]*""".r.pattern.matcher(word).matches)
wordCounts = filterNumbers.map(w => (w, 1)).reduceByKey(_+_)
rareWords = wordCounts.filter{ case (k, v) => v < 2 }.map {case (k, v) => k }.collect.toSet

def token(line: String): Seq[String] = {
    line.split("""\W""") //get rid of nonWords such as puncutation as opposed to splitting by just " "
    .map(_.toLowerCase)
    .filter(word => """[^0-9]*""".r.pattern.matcher(word).matches) //remove mix of wordsnumbers
    .filterNot(word => stopwords.contains(word)) // remove stopwords defined above (you can add to this list if you want)
    .filter(word => word.size >= 2) // leave only words greater than 1 characters. This deletes A LOT of words but useful to reduce our feature-set
    .filterNot(word => rareWords.contains(word)) // remove rare occurences of words
}

XXXwords = data.map(d => (d(0), token(d(1)).toSeq)).filter(s => s._2.length > 0)
words = XXXwords.map(v => v._2)
XXXlabels = XXXwords.map(v => v._1)

println(jobTitles.flatMap(lines => token(lines)).distinct.count) 


word2vec = new Word2Vec()
model = word2vec.fit(words)

title_vectors = words.map(x => new DenseVector(divArray(x.map(m => wordToVector(m, model).toArray).reduceLeft(sumArray),x.length)).asInstanceOf[Vector])
title_pairs = words.map(x => (x,new DenseVector(divArray(x.map(m => wordToVector(m, model).toArray).reduceLeft(sumArray),x.length)).asInstanceOf[Vector]))

resultRDD = XXXlabels.zip(title_vectors).map(v => CRAIGSLIST(v._1, v._2)).toDF

var train2:H2OFrame = h2oContext.asH2OFrame(resultRDD,"train2")
train2.replace(train2.find("target"),train2.vec("target").toCategoricalVec).remove()





//------------------------Frame 3-----------------------------------
rawJobTitles = sc.textFile("train3.csv")

data = rawJobTitles.map(d => d.split(','))

label = data.map(l => l(0))
jobTitles = data.map(l => l(1))
labelCounts = label.map(n => (n, 1)).reduceByKey(_+_).collect.mkString("\n")

stopwords = Set("ax","i","you","edu","s","t","m","subject","can","lines","re","what"
    ,"there","all","we","one","the","a","an","of","or","in","for","by","on"
    ,"but", "is", "in","a","not","with", "as", "was", "if","they", "are", "this", "and", "it", "have"
    , "from", "at", "my","be","by","not", "that", "to","from","com","org","like","likes","so")

nonWordSplit = jobTitles.flatMap(t => t.split("""\W""").map(_.toLowerCase))
filterNumbers = nonWordSplit.filter(word => """[^0-9]*""".r.pattern.matcher(word).matches)
wordCounts = filterNumbers.map(w => (w, 1)).reduceByKey(_+_)
rareWords = wordCounts.filter{ case (k, v) => v < 2 }.map {case (k, v) => k }.collect.toSet

def token(line: String): Seq[String] = {
    line.split("""\W""") //get rid of nonWords such as puncutation as opposed to splitting by just " "
    .map(_.toLowerCase)
    .filter(word => """[^0-9]*""".r.pattern.matcher(word).matches) //remove mix of wordsnumbers
    .filterNot(word => stopwords.contains(word)) // remove stopwords defined above (you can add to this list if you want)
    .filter(word => word.size >= 2) // leave only words greater than 1 characters. This deletes A LOT of words but useful to reduce our feature-set
    .filterNot(word => rareWords.contains(word)) // remove rare occurences of words
}

XXXwords = data.map(d => (d(0), token(d(1)).toSeq)).filter(s => s._2.length > 0)
words = XXXwords.map(v => v._2)
XXXlabels = XXXwords.map(v => v._1)

println(jobTitles.flatMap(lines => token(lines)).distinct.count) 


word2vec = new Word2Vec()
model = word2vec.fit(words)

title_vectors = words.map(x => new DenseVector(divArray(x.map(m => wordToVector(m, model).toArray).reduceLeft(sumArray),x.length)).asInstanceOf[Vector])
title_pairs = words.map(x => (x,new DenseVector(divArray(x.map(m => wordToVector(m, model).toArray).reduceLeft(sumArray),x.length)).asInstanceOf[Vector]))


resultRDD = XXXlabels.zip(title_vectors).map(v => CRAIGSLIST(v._1, v._2)).toDF

var train3:H2OFrame = h2oContext.asH2OFrame(resultRDD,"train3")
train3.replace(train3.find("target"),train3.vec("target").toCategoricalVec).remove()




//------------------------Frame 4-----------------------------------
rawJobTitles = sc.textFile("test1.csv")

data = rawJobTitles.map(d => d.split(','))

label = data.map(l => l(0))
jobTitles = data.map(l => l(1))
labelCounts = label.map(n => (n, 1)).reduceByKey(_+_).collect.mkString("\n")

stopwords = Set("ax","i","you","edu","s","t","m","subject","can","lines","re","what"
    ,"there","all","we","one","the","a","an","of","or","in","for","by","on"
    ,"but", "is", "in","a","not","with", "as", "was", "if","they", "are", "this", "and", "it", "have"
    , "from", "at", "my","be","by","not", "that", "to","from","com","org","like","likes","so")

nonWordSplit = jobTitles.flatMap(t => t.split("""\W""").map(_.toLowerCase))
filterNumbers = nonWordSplit.filter(word => """[^0-9]*""".r.pattern.matcher(word).matches)
wordCounts = filterNumbers.map(w => (w, 1)).reduceByKey(_+_)
rareWords = wordCounts.filter{ case (k, v) => v < 2 }.map {case (k, v) => k }.collect.toSet

def token(line: String): Seq[String] = {
    line.split("""\W""") //get rid of nonWords such as puncutation as opposed to splitting by just " "
    .map(_.toLowerCase)
    .filter(word => """[^0-9]*""".r.pattern.matcher(word).matches) //remove mix of wordsnumbers
    .filterNot(word => stopwords.contains(word)) // remove stopwords defined above (you can add to this list if you want)
    .filter(word => word.size >= 2) // leave only words greater than 1 characters. This deletes A LOT of words but useful to reduce our feature-set
    .filterNot(word => rareWords.contains(word)) // remove rare occurences of words
}

XXXwords = data.map(d => (d(0), token(d(1)).toSeq)).filter(s => s._2.length > 0)
words = XXXwords.map(v => v._2)
XXXlabels = XXXwords.map(v => v._1)

println(jobTitles.flatMap(lines => token(lines)).distinct.count) 


word2vec = new Word2Vec()
model = word2vec.fit(words)

title_vectors = words.map(x => new DenseVector(divArray(x.map(m => wordToVector(m, model).toArray).reduceLeft(sumArray),x.length)).asInstanceOf[Vector])
title_pairs = words.map(x => (x,new DenseVector(divArray(x.map(m => wordToVector(m, model).toArray).reduceLeft(sumArray),x.length)).asInstanceOf[Vector]))

resultRDD = XXXlabels.zip(title_vectors).map(v => CRAIGSLIST(v._1, v._2)).toDF

var test1:H2OFrame = h2oContext.asH2OFrame(resultRDD,"test1")
test1.replace(test1.find("target"),test1.vec("target").toCategoricalVec).remove()




//------------------------Frame 5-----------------------------------
rawJobTitles = sc.textFile("test2.csv")

data = rawJobTitles.map(d => d.split(','))

label = data.map(l => l(0))
jobTitles = data.map(l => l(1))
labelCounts = label.map(n => (n, 1)).reduceByKey(_+_).collect.mkString("\n")

stopwords = Set("ax","i","you","edu","s","t","m","subject","can","lines","re","what"
    ,"there","all","we","one","the","a","an","of","or","in","for","by","on"
    ,"but", "is", "in","a","not","with", "as", "was", "if","they", "are", "this", "and", "it", "have"
    , "from", "at", "my","be","by","not", "that", "to","from","com","org","like","likes","so")

nonWordSplit = jobTitles.flatMap(t => t.split("""\W""").map(_.toLowerCase))
filterNumbers = nonWordSplit.filter(word => """[^0-9]*""".r.pattern.matcher(word).matches)
wordCounts = filterNumbers.map(w => (w, 1)).reduceByKey(_+_)
rareWords = wordCounts.filter{ case (k, v) => v < 2 }.map {case (k, v) => k }.collect.toSet

def token(line: String): Seq[String] = {
    line.split("""\W""") //get rid of nonWords such as puncutation as opposed to splitting by just " "
    .map(_.toLowerCase)
    .filter(word => """[^0-9]*""".r.pattern.matcher(word).matches) //remove mix of wordsnumbers
    .filterNot(word => stopwords.contains(word)) // remove stopwords defined above (you can add to this list if you want)
    .filter(word => word.size >= 2) // leave only words greater than 1 characters. This deletes A LOT of words but useful to reduce our feature-set
    .filterNot(word => rareWords.contains(word)) // remove rare occurences of words
}

XXXwords = data.map(d => (d(0), token(d(1)).toSeq)).filter(s => s._2.length > 0)
words = XXXwords.map(v => v._2)
XXXlabels = XXXwords.map(v => v._1)

println(jobTitles.flatMap(lines => token(lines)).distinct.count) 


word2vec = new Word2Vec()
model = word2vec.fit(words)

title_vectors = words.map(x => new DenseVector(divArray(x.map(m => wordToVector(m, model).toArray).reduceLeft(sumArray),x.length)).asInstanceOf[Vector])
title_pairs = words.map(x => (x,new DenseVector(divArray(x.map(m => wordToVector(m, model).toArray).reduceLeft(sumArray),x.length)).asInstanceOf[Vector]))

resultRDD = XXXlabels.zip(title_vectors).map(v => CRAIGSLIST(v._1, v._2)).toDF

var test2:H2OFrame = h2oContext.asH2OFrame(resultRDD,"test2")
test2.replace(test2.find("target"),test2.vec("target").toCategoricalVec).remove()



//------------------------Frame 6-----------------------------------
rawJobTitles = sc.textFile("test3.csv")

data = rawJobTitles.map(d => d.split(','))

label = data.map(l => l(0))
jobTitles = data.map(l => l(1))
labelCounts = label.map(n => (n, 1)).reduceByKey(_+_).collect.mkString("\n")

stopwords = Set("ax","i","you","edu","s","t","m","subject","can","lines","re","what"
    ,"there","all","we","one","the","a","an","of","or","in","for","by","on"
    ,"but", "is", "in","a","not","with", "as", "was", "if","they", "are", "this", "and", "it", "have"
    , "from", "at", "my","be","by","not", "that", "to","from","com","org","like","likes","so")

nonWordSplit = jobTitles.flatMap(t => t.split("""\W""").map(_.toLowerCase))
filterNumbers = nonWordSplit.filter(word => """[^0-9]*""".r.pattern.matcher(word).matches)
wordCounts = filterNumbers.map(w => (w, 1)).reduceByKey(_+_)
rareWords = wordCounts.filter{ case (k, v) => v < 2 }.map {case (k, v) => k }.collect.toSet

def token(line: String): Seq[String] = {
    line.split("""\W""") //get rid of nonWords such as puncutation as opposed to splitting by just " "
    .map(_.toLowerCase)
    .filter(word => """[^0-9]*""".r.pattern.matcher(word).matches) //remove mix of wordsnumbers
    .filterNot(word => stopwords.contains(word)) // remove stopwords defined above (you can add to this list if you want)
    .filter(word => word.size >= 2) // leave only words greater than 1 characters. This deletes A LOT of words but useful to reduce our feature-set
    .filterNot(word => rareWords.contains(word)) // remove rare occurences of words
}

XXXwords = data.map(d => (d(0), token(d(1)).toSeq)).filter(s => s._2.length > 0)
words = XXXwords.map(v => v._2)
XXXlabels = XXXwords.map(v => v._1)

println(jobTitles.flatMap(lines => token(lines)).distinct.count) 


word2vec = new Word2Vec()
model = word2vec.fit(words)

title_vectors = words.map(x => new DenseVector(divArray(x.map(m => wordToVector(m, model).toArray).reduceLeft(sumArray),x.length)).asInstanceOf[Vector])
title_pairs = words.map(x => (x,new DenseVector(divArray(x.map(m => wordToVector(m, model).toArray).reduceLeft(sumArray),x.length)).asInstanceOf[Vector]))

resultRDD = XXXlabels.zip(title_vectors).map(v => CRAIGSLIST(v._1, v._2)).toDF

var test3:H2OFrame = h2oContext.asH2OFrame(resultRDD,"test3")
test3.replace(test3.find("target"),test3.vec("target").toCategoricalVec).remove()


