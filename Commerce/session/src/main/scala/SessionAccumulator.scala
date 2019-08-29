import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

class SessionAccumulator extends AccumulatorV2[String, mutable.HashMap[String, Int]] {

  // 如果acc.add（“a”）存在hashmap，则加一，不在就创建
  val countMap = new mutable.HashMap[String, Int]()

  // 判断累加器是否为空
  override def isZero: Boolean = {
    countMap.isEmpty
  }

  override def copy(): AccumulatorV2[String, mutable.HashMap[String, Int]] = {
    // copy操纵
    val acc = new SessionAccumulator
    // ++= 拼接，将两个map拼接到一起
    acc.countMap ++= this.countMap
    acc
  }

  override def reset(): Unit = {
    // 清理操作
    countMap.clear()
  }

  override def add(v: String): Unit = {
    // 更新操作，先判断传进来在不在countMap,不在就创建
    if (!this.countMap.contains(v))
      this.countMap += (v -> 0) // 从0开始
    this.countMap.update(v, countMap(v) + 1)
  }

  override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Int]]): Unit = {
    // merge将两个map进行合并，两个countMap相加
    other match {
        // 先确定传进来的other是自定义累加器的other类型  foldLeft() 折叠
        // （0 /: (1 to 3))(_+_) 0+(1+2+3)
        //  (0 /: (1 to 100)){case (int1,int2) => int1 +int2}
        //  (1 /: 3).foldLeft(0)
      // (this.countMap) /: acc.countMap
      case acc:SessionAccumulator => acc.countMap.foldLeft(this.countMap){
        // += 如果这个k对应的v有，覆盖，没有k对应的v追加
        // 将第一个map去除k，与取出来的新的k相加
        case (map,(k,v)) => map+=(k -> (map.getOrElse(k,v) +v))
      }
    }
  }

  override def value: mutable.HashMap[String, Int] = {
    this.countMap
  }
}
