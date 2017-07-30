import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
// $example on$
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.mllib.util.MLUtils

/**
  * 决策树分类
  */
object DecisionTreeScala {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession
            .builder
            .appName("NaiveBayesExample")
            .master("local[4]")
            .getOrCreate()

        // 加载数据
        val data = MLUtils.loadLibSVMFile(spark.sparkContext, "file:///E:/timliu/src/data/sample_libsvm_data.txt")
        // Split the data into training and test sets (30% held out for testing)
        val splits = data.randomSplit(Array(0.7, 0.3))
        val (trainingData, testData) = (splits(0), splits(1))

        // Train a DecisionTree model.
        //  Empty categoricalFeaturesInfo indicates all features are continuous.
        val numClasses = 2
        val categoricalFeaturesInfo = Map[Int, Int]()
        val impurity = "gini"
        val maxDepth = 5
        val maxBins = 32

        //通过决策树训练分类器
        val model = DecisionTree.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
            impurity, maxDepth, maxBins)

        // Evaluate model on test instances and compute test error
        val labelAndPreds = testData.map { point =>
            val prediction = model.predict(point.features)
            (point.label, prediction)
        }
        val testErr = labelAndPreds.filter(r => r._1 != r._2).count().toDouble / testData.count()
        println("Test Error = " + testErr)
        println("Learned classification tree model:\n" + model.toDebugString)
    }
}
