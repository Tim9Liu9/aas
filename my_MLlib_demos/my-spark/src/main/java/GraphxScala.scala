import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark._
import org.apache.spark.graphx._
// To make some of the examples work we will also need RDD
import org.apache.spark.rdd.RDD

/**
  * Created by Administrator on 2017/7/18.
  */
object GraphxScala {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
        conf.setAppName("graphx")
        conf.setMaster("local[4]")
        val sc = new SparkContext(conf);

        //定点属性
        class VertexProperty()
        //用户属性
        case class UserProperty(val name: String) extends VertexProperty
        //商品属性
        case class ProductProperty(val name: String, val price: Double) extends VertexProperty

        //构造4个顶点
        val users: RDD[(VertexId, (String, String))] = sc.parallelize(Array(
            (3L, ("rxin", "student")),
            (7L, ("jgonzal", "postdoc")),
            (5L, ("franklin", "prof")),
            (2L, ("istoica", "prof")),
            (1L, ("istoica", "prof")))
        )

        //创建边元素
        val relationships: RDD[Edge[String]] = sc.parallelize(Array(
//            Edge(3L, 7L, "collab"),
//            Edge(5L, 3L, "advisor"),
//            Edge(2L, 5L, "colleague"),
//            Edge(5L, 7L, "pi")))
            Edge(2L, 7L, "collab"),
            Edge(7L, 5L, "advisor"),
            Edge(5L, 3L, "colleague"),
            Edge(7L, 3L, "pi")))

        //定义默认用户
        val defaultUser = ("John Doe", "Missing")

        //定义图对象
        val graph = Graph(users, relationships, defaultUser)

        //访问图的顶点集合，过滤出postdoc集合
        var r = graph.vertices.filter { case (id, (name, pos)) => pos == "postdoc" }.count
        println(r)

        //找出起点小于终点边个数
        r = graph.edges.filter(t=>t.srcId < t.dstId).count()
        println(r)

        //提取图的三元素
        val facts: RDD[String] = graph.triplets.map(triplet =>triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1)
        facts.collect.foreach(println(_))

        //图常见操作
        println(graph.numEdges)         //边的数量
        println(graph.numVertices)      //定点数量
        System.out.println("===============");
        println(graph.inDegrees.count())        //定点数量,入度
        println(graph.outDegrees.count())       //定点数量,入度
        println(graph.degrees.count())       //定点数量,入度

        //产生的边的RDD
        val rr = graph.edges.filter(t=>t.attr.contains("supervisor"))

        //mapVertices对顶点的属性进行变换,图结构不变。
        val newGraph = graph.mapVertices((vid,t)=>("Mr " + t._1,t._2))
        //
        newGraph.vertices.foreach(t=>println(t._2._1))

        //反向图,将边的方向进行对调
        val reverseGraph = graph.reverse
        println(reverseGraph.inDegrees.count())


        //定义函数
        def f1(t:EdgeTriplet[(String,String), String]) = t.attr.contains("advisor")
        def f2(a:VertexId,b:(String,String)) = a.toLong == 7L || a.toLong == 5L
        //子图,
        val subgraph = graph.subgraph(f1,f2)

        //通过过滤生成子图

        println(subgraph.numVertices)
        println(subgraph.numEdges)

        //生成子图，只有advisor的边和advisor涉及的点。
        val vids = graph.triplets.filter(t=>t.attr.contains("advisor")).flatMap(t=>Array(t.srcId,t.dstId)).distinct()
        val vidss = vids.collect()
        def ff(a:VertexId, b:(String,String)) = {
            vidss.contains(a.toLong)
        }
        //
        val sg1 = graph.subgraph(vpred= ff)
        println(sg1.numVertices)
        println(sg1.numEdges)


        //图2顶点
        val u2: RDD[(VertexId, Int)] = sc.parallelize(Array(
            (3L,1),
            (5L,2),
            (7L,3),
            (8L,4)
        ))
        val r2: RDD[Edge[Int]] = sc.parallelize(Array(
            Edge(7L, 3L, 1),
            Edge(7L, 5L, 2),
            Edge(5L, 3L, 3),
            Edge(5L, 8L, 3)))

        val defaultUser2 = 0
        val g2 = Graph(u2, r2, defaultUser2)

        System.out.println("===============");
        val g3 = graph.mask(g2)
        //g3.vertices.foreach(t=>println(t._1.toLong))
        g3.edges.foreach(println)
    }
}
