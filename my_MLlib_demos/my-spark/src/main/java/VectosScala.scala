import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.mllib.feature.{Normalizer, StandardScaler}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.sql.SparkSession

/**
  * 操纵向量
  */
object VectosScala {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
        conf.setAppName("SpamFilter")
        conf.setMaster("local[4]")

        val sc = new SparkContext(conf) ;
        //密度向量
        val v1 = Vectors.dense(-2.0, 5.0, 1.0);
        val v2 = Vectors.dense(2.0, 0.0, 1.0);
        val vrdd = sc.makeRDD(Array(v1,v2)
        )
        //松散向量
        val v4 = Vectors.sparse(4,Array(0,2),Array(1.0,2.0));

        //标准向量缩放器
        val scaler = new StandardScaler(true,true)
        val model = scaler.fit(vrdd) ;
        val result = model.transform(vrdd) ;
        val arr = result.collect() ;
        println(arr)
        
        val n = new Normalizer()
        val rr = n.transform(vrdd).collect() ;
        println(rr)

        val stat = Statistics.colStats(sc.makeRDD(Array(Vectors.dense(0.0, 1.0, 2.0), Vectors.dense(2.0, 1.0, 0.0))));
        println(stat)


        val m = Statistics.corr(sc.makeRDD(Array(Vectors.dense(1.0, 1.0), Vectors.dense(2.0,2.0))),"pearson");
        println(m)
    }
}
