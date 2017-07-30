import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.{HashingTF, RegexTokenizer, StopWordsRemover, Tokenizer, Word2Vec}
import org.apache.spark.sql.SparkSession

/**
  * 通过word2vec算法寻找同义词
  */
object Word2VecScala {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
        conf.setAppName("Word2Vec")
        conf.setMaster("local[4]")
        val dataDir = "file:///E:/timliu/src/data/talk.politics.guns/*"
        val sess = SparkSession.builder().config(conf).getOrCreate()

        import sess.implicits._

        //加载文档，每个文档对应一个String
        //textDF = (xxx xxxlx xxxx )
        var textDF = sess.sparkContext
            .wholeTextFiles(dataDir)
            .map { case (file, text) => text }
            .map(Tuple1.apply) //  .map(Tuple1.apply(_))  .map((_))
            .toDF("sentence")

        //正则分词器
        val regexTokenizer = new RegexTokenizer()
            .setInputCol("sentence")
            .setOutputCol("words")
            .setPattern("\\w+").setGaps(false)

        //通过分词器进行变换,类似于split(" "),
        val tokenizedDF = regexTokenizer.transform(textDF)

        //创建停用词删除器,使用默认的停用词
        val remover = new StopWordsRemover().setInputCol("words").setOutputCol("filtered")

        //过滤停用词
        val filteredDF = remover.transform(tokenizedDF)

        //创建word2vec对象
        val word2Vec = new Word2Vec().setInputCol("filtered").setOutputCol("result").setVectorSize(3).setMinCount(0)

        //拟合filteredDF
        val model = word2Vec.fit(filteredDF)

        //寻找同义词
//        val synonyms1 = model.findSynonyms("1941", 10)
        val synonyms1 = model.findSynonyms("gun", 10)

        synonyms1.show(200)

    }
}
