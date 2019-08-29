
import java.util.UUID

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.model.UserVisitAction
import commons.utils.{DateUtils, ParamUtils}
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable

object PageConvertStat {
  def main(args: Array[String]): Unit = {
    // 获取任务限制条件
    val jsonStr = ConfigurationManager.config.getString(Constants.TASK_PARAMS)
    val taskParam = JSONObject.fromObject(jsonStr)

    // 获取唯一任务ID
    val taskUUID = UUID.randomUUID().toString

    // 创建SparkConf
    val sparkConf = new SparkConf().setAppName("pageConver").setMaster("local[*]")

    // 创建sparkSession
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    // 获取用户行为数据
    val sessionId2ActionRDD = getUserVisitAction(sparkSession,taskParam)
   // sessionIdActionRDD.foreach(println(_))

    //  pageFlowStr: "1,2,3,4,5,6,7"
    val pageFlowStr = ParamUtils.getParam(taskParam,Constants.PARAM_TARGET_PAGE_FLOW)

    // pageFlowArray: Array[Long] [1,2,3,4,5,6,7]
    val pageFlowArray = pageFlowStr.split(",")

    // pageFlowArray.slice(0,pageFlowArray.length -1): [1,2,3,4,5,6]
    // (pageFlowArray.tail: [2,3,4,5,6]
    // pageFlowArray.slice(0,pageFlowArray.length -1).zip(pageFlowArray.tail): [(1,2),(2,3)...]
    // targetPageSplit: [1_2,2_3,3_4...]
    val targetPageSplit = pageFlowArray.slice(0,pageFlowArray.length -1).zip(pageFlowArray.tail).map{
      case (page1,page2) => page1 + "_" + page2
    }
    // targetPageSplit.foreach(println(_))
    // sessionId2ActionRDD[(sessionId,action)]
    val sessionid2gROUPrdd = sessionId2ActionRDD.groupByKey()

    // pageSplitNumRDD:RDD[(String,1L)]
   val pageSplitNumRDD =  sessionid2gROUPrdd.flatMap{
      case(sessionId,iterableAction) =>
        // item1:action
        // item2:action
        val sortList = iterableAction.toList.sortWith((item1,item2) =>{
          // sortWith为true，取item1在前，时间早的放钱
          DateUtils.parseTime(item1.action_time).getTime <
            DateUtils.parseTime(item2.action_time).getTime
        })

        // pageList: List[long] [1,2,3,4...]
        val pageList = sortList.map{
          case action => action.page_id
        }

        val pageSplit = pageList.slice(0,pageList.length - 1).zip(pageList.tail).map{
          case (page1,page2) => page1 + "_" + page2
        }

        // 过滤,只保留targetPageSplit中有的切片
        val pageSplitFilter = pageSplit.filter{
          case pageSplit => targetPageSplit.contains(pageSplit)
        }

        pageSplitFilter.map{
          case pageSplit => (pageSplit,1L)
        }
    }
    // pageSplitCountMap: Map[(pageSplit,count)] page1_2,count1
    val pageSplitCountMap = pageSplitNumRDD.countByKey()

    // 开始的页面
    val startPage = pageFlowArray(0).toLong

    // 访问过page1页面的个数
    val startPageCount = sessionId2ActionRDD.filter{
      case(sessionId,action) => action.page_id ==startPage
    }.count()

    getPageConvert(sparkSession,taskUUID,targetPageSplit,startPageCount,pageSplitCountMap)

  }

  def getUserVisitAction(sparkSession: SparkSession, taskUUID: JSONObject) = {

    val startDate = ParamUtils.getParam(taskUUID,Constants.PARAM_START_DATE)
    val endDate = ParamUtils.getParam(taskUUID,Constants.PARAM_END_DATE)

    val sql = "select * from user_visit_action where date >='" + startDate + "'and date <='" +
    endDate + "'"

    import sparkSession.implicits._
    // as[],必须与表中的字段名一致
    sparkSession.sql(sql).as[UserVisitAction].rdd.map(item =>(item.session_id,item))

  }

  //
  def getPageConvert(sparkSession: SparkSession,
                     taskUUID: String,
                     targetPageSplit: Array[String],
                     startPageCount: Long,
                     pageSplitCountMap:collection.Map[String,Long]): Unit = {

    val pageSplitRatio = new mutable.HashMap[String,Double]()

    // lastPageCount为startPageCount的个数
    var lastPageCount = startPageCount.toDouble

    // 1,2,3,4,5,6,7
    // 1_2,2_3...
    for (pageSplit <- targetPageSplit){
      // 第一次循环: lastPageCount:page1   currentPageSplitCount:page1_page2 结果：page1_page2
      val currentPageSplitCount = pageSplitCountMap.get(pageSplit).get.toDouble
      val ratio = currentPageSplitCount / lastPageCount
      pageSplitRatio.put(pageSplit,ratio)
      lastPageCount = currentPageSplitCount
    }

    val convertStr = pageSplitRatio.map{
      case(pageSplit,ratio) => pageSplit + "=" +ratio
    }.mkString("|")

    val pageSplit = PageSplitConvertRate(taskUUID, convertStr)

    val pageSplitRatioRDD = sparkSession.sparkContext.makeRDD(Array(pageSplit))


    import sparkSession.implicits._
    pageSplitRatioRDD.toDF().write
      .format("jdbc")
      .option("url",ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("user",ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password",ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .option("dbtable","page_split_convert_rate")
      .mode(SaveMode.Append)
      .save

  }

}
