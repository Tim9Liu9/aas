import com.huaban.analysis.jieba.JiebaSegmenter
import com.huaban.analysis.jieba.JiebaSegmenter.SegMode
import org.apdplat.word.WordSegmenter

/**
  * Created by Administrator on 2017/7/17.
  */
object WordTokenizer {
    def main(args: Array[String]): Unit = {

        val str = "hello world"
        print(str.hashCode)
        var list = WordSegmenter.seg("南京市长江大桥")
        var list1 = WordSegmenter.seg("南京市的市长是江大桥同志")
        var list2 = WordSegmenter.seg("迅雷不急掩耳盗铃之势")
        println(list)
        println(list1)
        println(list2)

        // jieba 结巴分词：
        val segmenter = new JiebaSegmenter()

        var list_jieba1 = segmenter.process("南京市长江大桥", SegMode.INDEX)// SegMode.SEARCH
        var list_jieba1_k = segmenter.sentenceProcess("南京市长江大桥") // segmenter.process("南京市长江大桥", SegMode.SEARCH)

        var list_jieba2 = segmenter.process("南京市的市长是江大桥同志", SegMode.INDEX)
        var list_jieba2_k = segmenter.sentenceProcess("南京市的市长是江大桥同志")

        var list_jieba3 = segmenter.process("迅雷不急掩耳盗铃之势", SegMode.INDEX)
        var list_jieba3_k = segmenter.sentenceProcess("迅雷不急掩耳盗铃之势")

        println(list_jieba1)
        println(list_jieba1_k)

        println(list_jieba2)
        println(list_jieba2_k)

        println(list_jieba3)
        println(list_jieba3_k)




    }
}
