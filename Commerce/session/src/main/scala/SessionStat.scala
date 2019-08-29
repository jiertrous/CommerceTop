import java.util.{Date, Random, UUID}

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.model.{UserInfo, UserVisitAction}
import commons.utils._
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

object SessionStat {


  def main(args: Array[String]): Unit = {

    // 获取筛选条件,获取json串
    val jsonStr = ConfigurationManager.config.getString(Constants.TASK_PARAMS)
    // 获取筛选条件对应的JsonObject
    val taskParam = JSONObject.fromObject(jsonStr)

    // 创建全局唯一的主键
    val taskUUID = UUID.randomUUID().toString

    // 创建sparkConf
    val sparkConf = new SparkConf().setAppName("session").setMaster("local[*]")

    // 创建sparkSession（包含sparkContext）
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    // 获取原始的动作表数据
    // actionRDD: RDD[UserVisitAction]
    val actionRDD = getOriActionRDD(sparkSession, taskParam)

    // sessionId2ActionRDD: RDD[(sessionId, UserVisitAction)]
    val sessionId2ActionRDD = actionRDD.map(item => (item.session_id, item))

    // session2GroupActionRDD: RDD[(sessionId, iterable_UserVisitAction)]
    val session2GroupActionRDD = sessionId2ActionRDD.groupByKey()

    session2GroupActionRDD.cache()
    // sessionId2FullInfoRDD:RDD[(sessionId,fullInfo)]
    val sessionId2FullInfoRDD = getSessionFullInfo(sparkSession, session2GroupActionRDD)

    val sessionAccumulator = new SessionAccumulator
    // 注册信息
    sparkSession.sparkContext.register(sessionAccumulator)
    // 测试打印信息
    //  userId2AggrInfoRDD.foreach(println(_))
    // sessionId2FullInfoRDD.foreach(println(_))
    // sessionId2FilterRDD：RDD[(sessionId,fullInfo)]  所有符合过滤条件的RDD
    // sessionId2FilterRDD实:现先根绝限制条件session数据进行过滤，并完成来假期的更新
    val sessionId2FilterRDD = getSessionFilteredRDD(taskParam, sessionId2FullInfoRDD, sessionAccumulator)
    // 导入sql之前必须要执行一次action算子
    //sessionId2FilterRDD.foreach(println(_))
    sessionId2FilterRDD.count()

    // 获取最终统计结果
    getSessionRatio(sparkSession, taskUUID, sessionAccumulator.value)
    // sessionId2FullInfoRDD.foreach(println(_))

    // 需求二：session的随机抽取
    // sessionId2FilterRDD: RDD[(sid,fullInfo)]  一个session对应一条数据，也就是一个fullInfo
    sessionRandomExtract(sparkSession, taskUUID, sessionId2FilterRDD)

    // 需求三:Top10热门商品设计
    //  getSessionFilteredRDD:RDD[(sessionId,action)]
    //  getSessionFilteredRDD:RDD[(sessionId,action)]   // 符合条件的
    //  sessionId2FilterActionRDD:Join
    //  获取所有符合过滤条件的action数据
    val sessionId2FilterActionRDD = sessionId2ActionRDD.join(sessionId2FilterRDD).map {
      case (sessionId, (action, fullInfo)) =>
        (sessionId, action)
    }
    //  top10CategoryArray:Array[(sortKey,countInfo)]
    val top10CategoryArray = top10PopularCategories(sparkSession, taskUUID, sessionId2FilterActionRDD)

    // 需求四：Top10热门商品的Top10活跃session统计
    // sessionId2FilterActionRDD：RDD[(sessionId,action)]
    //  top10CategoryArray:Array[(sortKey,countInfo)]

    top10ActiveSession(sparkSession, taskUUID, sessionId2FilterActionRDD, top10CategoryArray)


  }

  def calculateVisitLength(visitLength: Long, sessionStatisticAccumulator: SessionAccumulator) = {
    // 范围内对应的累加器+1
    if (visitLength >= 1 && visitLength <= 3) {
      sessionStatisticAccumulator.add(Constants.TIME_PERIOD_1s_3s)
    } else if (visitLength >= 4 && visitLength <= 6) {
      sessionStatisticAccumulator.add(Constants.TIME_PERIOD_4s_6s)
    } else if (visitLength >= 7 && visitLength <= 9) {
      sessionStatisticAccumulator.add(Constants.TIME_PERIOD_7s_9s)
    } else if (visitLength >= 10 && visitLength <= 30) {
      sessionStatisticAccumulator.add(Constants.TIME_PERIOD_10s_30s)
    } else if (visitLength > 30 && visitLength <= 60) {
      sessionStatisticAccumulator.add(Constants.TIME_PERIOD_30s_60s)
    } else if (visitLength > 60 && visitLength <= 180) {
      sessionStatisticAccumulator.add(Constants.TIME_PERIOD_1m_3m)
    } else if (visitLength > 180 && visitLength <= 600) {
      sessionStatisticAccumulator.add(Constants.TIME_PERIOD_3m_10m)
    } else if (visitLength > 600 && visitLength <= 1800) {
      sessionStatisticAccumulator.add(Constants.TIME_PERIOD_10m_30m)
    } else if (visitLength > 1800) {
      sessionStatisticAccumulator.add(Constants.TIME_PERIOD_30m)
    }

  }

  def calculateStepLength(stepLength: Long, sessionStatisticAccumulator: SessionAccumulator) = {
    if (stepLength >= 1 && stepLength <= 3) {
      sessionStatisticAccumulator.add(Constants.STEP_PERIOD_1_3)
    } else if (stepLength >= 4 && stepLength <= 6) {
      sessionStatisticAccumulator.add(Constants.STEP_PERIOD_4_6)
    } else if (stepLength >= 7 && stepLength <= 9) {
      sessionStatisticAccumulator.add(Constants.STEP_PERIOD_7_9)
    } else if (stepLength >= 10 && stepLength <= 30) {
      sessionStatisticAccumulator.add(Constants.STEP_PERIOD_10_30)
    } else if (stepLength > 30 && stepLength <= 60) {
      sessionStatisticAccumulator.add(Constants.STEP_PERIOD_30_60)
    } else if (stepLength > 60) {
      sessionStatisticAccumulator.add(Constants.STEP_PERIOD_60)
    }
  }

  def getSessionFilteredRDD(taskParam: JSONObject,
                            sessionId2FullInfoRDD: RDD[(String, String)],
                            sessionAccumulator: SessionAccumulator) = {
    // 过滤条件
    val startAge = ParamUtils.getParam(taskParam, Constants.PARAM_START_AGE)
    val endAge = ParamUtils.getParam(taskParam, Constants.PARAM_END_AGE)
    val professionals = ParamUtils.getParam(taskParam, Constants.PARAM_PROFESSIONALS)
    val cities = ParamUtils.getParam(taskParam, Constants.PARAM_CITIES)
    val sex = ParamUtils.getParam(taskParam, Constants.PARAM_SEX)
    val keywords = ParamUtils.getParam(taskParam, Constants.PARAM_KEYWORDS)
    val categoryIds = ParamUtils.getParam(taskParam, Constants.PARAM_CATEGORY_IDS)

    //拼接人字符串，如果为空，就省略
    var filterInfo = (if (startAge != null) Constants.PARAM_START_AGE + "=" + startAge + "|" else "") +
      (if (endAge != null) Constants.PARAM_END_AGE + "=" + endAge + "|" else "") +
      (if (professionals != null) Constants.PARAM_PROFESSIONALS + "=" + professionals + "|" else "") +
      (if (cities != null) Constants.PARAM_CITIES + "=" + cities + "|" else "") +
      (if (sex != null) Constants.PARAM_SEX + "=" + sex + "|" else "") +
      (if (keywords != null) Constants.PARAM_KEYWORDS + "=" + keywords + "|" else "") +
      (if (categoryIds != null) Constants.PARAM_CATEGORY_IDS + "=" + categoryIds else "")
    if (filterInfo.endsWith("\\|")) {
      filterInfo = filterInfo.substring(0, filterInfo.length - 1)
    }

    // 根据filterInfo过滤sessionId2FullInfoRDD
    //filter方法，最好必须返回一个boolean变量
    sessionId2FullInfoRDD.filter {
      case (sessionId, fullInfo) =>
        var success = true
        // between指定，fullInfo中哪个字段与哪个帅选条件进行比较，刷选条件的左边与右边条件
        // age再start与end之内，不成立，就过滤该数据
        if (!ValidUtils.between(fullInfo, Constants.FIELD_AGE, filterInfo, Constants.PARAM_START_AGE, Constants.PARAM_END_AGE)) {
          success = false
        } else if (!ValidUtils.in(fullInfo, Constants.FIELD_PROFESSIONAL, filterInfo, Constants.PARAM_PROFESSIONALS)) {
          success = false
        } else if (!ValidUtils.in(fullInfo, Constants.FIELD_CITY, filterInfo, Constants.PARAM_CITIES)) {
          success = false
        } else if (!ValidUtils.equal(fullInfo, Constants.FIELD_SEX, filterInfo, Constants.PARAM_SEX)) {
          success = false
        } else if (!ValidUtils.in(fullInfo, Constants.FIELD_SEARCH_KEYWORDS, filterInfo, Constants.PARAM_KEYWORDS)) {
          success = false
        } else if (!ValidUtils.in(fullInfo, Constants.FIELD_CLICK_CATEGORY_IDS, filterInfo, Constants.PARAM_CATEGORY_IDS)) {
          success = false
        }
        if (success) {
          // 符合筛选条件，总数+1
          // 自定义累加器add方法
          // success为true，当前的条件的符合过滤条件，对session总数进行累加
          sessionAccumulator.add(Constants.SESSION_COUNT)
          // 符合时长的+1
          val visitLength = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_VISIT_LENGTH).toLong
          // 步长范围+1
          val stepLength = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_STEP_LENGTH).toLong
          // 范围内对应的累加器+1
          calculateVisitLength(visitLength, sessionAccumulator)
          calculateStepLength(stepLength, sessionAccumulator)
        }
        success
    }
  }

  def getSessionFullInfo(sparkSession: SparkSession,
                         session2GroupActionRDD: RDD[(String, Iterable[UserVisitAction])]) = {
    // userId2AggrInfoRDD: RDD[(userId, aggrInfo)]
    val userId2AggrInfoRDD = session2GroupActionRDD.map {
      // items._1 = ...,    case相当于模式匹配，case指定第一个ssessionid
      case (sessionId, iterableAction) =>
        var userId = -1L

        var startTime: Date = null
        var endTime: Date = null

        var stepLength = 0

        val searchKeywords = new StringBuffer("")
        val clickCategories = new StringBuffer("")

        for (action <- iterableAction) {
          if (userId == -1L) {
            userId = action.user_id
          }

          val actionTime = DateUtils.parseTime(action.action_time)
          // 如果startTime比actioTime晚，就刷新starttime
          if (startTime == null || startTime.after(actionTime)) {
            startTime = actionTime
          }
          if (endTime == null || endTime.before(actionTime)) {
            endTime = actionTime
          }

          val searchKeyword = action.search_keyword
          if (StringUtils.isNotEmpty(searchKeyword) && !searchKeywords.toString.contains(searchKeyword)) {
            searchKeywords.append(searchKeyword + ",")
          }

          val clickCategoryId = action.click_category_id
          if (clickCategoryId != -1 && !clickCategories.toString.contains(clickCategoryId)) {
            clickCategories.append(clickCategoryId + ",")
          }

          stepLength += 1
        }

        // searchKeywords.toString.substring(0, searchKeywords.toString.length)
        val searchKw = StringUtils.trimComma(searchKeywords.toString)
        val clickCg = StringUtils.trimComma(clickCategories.toString)

        val visitLength = (endTime.getTime - startTime.getTime) / 1000

        // 汇总，包含一个action的聚合信息
        val aggrInfo = Constants.FIELD_SESSION_ID + "=" + sessionId + "|" +
          Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKw + "|" +
          Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCg + "|" +
          Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|" +
          Constants.FIELD_STEP_LENGTH + "=" + stepLength + "|" +
          Constants.FIELD_START_TIME + "=" + DateUtils.formatTime(startTime)
        // 返回聚合信息，需要和公共对象连接
        (userId, aggrInfo)
    }

    // 返回结果打印   userId2AggrInfoRDD
    val sql = "select * from user_info"

    import sparkSession.implicits._
    // userId2InfoRDD:RDD[(userId, UserInfo)]
    val userId2InfoRDD = sparkSession.sql(sql).as[UserInfo].rdd.map(item => (item.user_id, item))
    //join操作,将两个表连接起来
    val sessionId2FullInfoRDD = userId2AggrInfoRDD.join(userId2InfoRDD).map {
      case (userId, (aggrInfo, userInfo)) =>
        val age = userInfo.age
        val professional = userInfo.professional
        val sex = userInfo.sex
        val city = userInfo.city

        val fullInfo = aggrInfo + "|" +
          Constants.FIELD_AGE + "=" + age + "|" +
          Constants.FIELD_PROFESSIONAL + "=" + professional + "|" +
          Constants.FIELD_SEX + "=" + sex + "|" +
          Constants.FIELD_CITY + "=" + city

        val sessionId = StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_SESSION_ID)

        (sessionId, fullInfo)
    }
    // 返回
    sessionId2FullInfoRDD
  }

  def getOriActionRDD(sparkSession: SparkSession, taskParam: JSONObject) = {
    val startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE)
    val endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE)

    val sql = "select * from user_visit_action where date>='" + startDate + "' and date<='" + endDate + "'"

    import sparkSession.implicits._
    sparkSession.sql(sql).as[UserVisitAction].rdd
  }

  def getSessionRatio(sparkSession: SparkSession, taskUUID: String, value: mutable.HashMap[String, Int]): Unit = {
    // getOrElse为分母，最低为1，避免/0
    val session_count = value.getOrElse(Constants.SESSION_COUNT, 1).toDouble

    val visit_length_1s_3s = value.getOrElse(Constants.TIME_PERIOD_1s_3s, 0)
    val visit_length_4s_6s = value.getOrElse(Constants.TIME_PERIOD_4s_6s, 0)
    val visit_length_7s_9s = value.getOrElse(Constants.TIME_PERIOD_7s_9s, 0)
    val visit_length_10s_30s = value.getOrElse(Constants.TIME_PERIOD_10s_30s, 0)
    val visit_length_30s_60s = value.getOrElse(Constants.TIME_PERIOD_30s_60s, 0)
    val visit_length_1m_3m = value.getOrElse(Constants.TIME_PERIOD_1m_3m, 0)
    val visit_length_3m_10m = value.getOrElse(Constants.TIME_PERIOD_3m_10m, 0)
    val visit_length_10m_30m = value.getOrElse(Constants.TIME_PERIOD_10m_30m, 0)
    val visit_length_30m = value.getOrElse(Constants.TIME_PERIOD_30m, 0)

    val step_length_1_3 = value.getOrElse(Constants.STEP_PERIOD_1_3, 0)
    val step_length_4_6 = value.getOrElse(Constants.STEP_PERIOD_4_6, 0)
    val step_length_7_9 = value.getOrElse(Constants.STEP_PERIOD_7_9, 0)
    val step_length_10_30 = value.getOrElse(Constants.STEP_PERIOD_10_30, 0)
    val step_length_30_60 = value.getOrElse(Constants.STEP_PERIOD_30_60, 0)
    val step_length_60 = value.getOrElse(Constants.STEP_PERIOD_60, 0)

    // 保留有效位数
    val visit_length_1s_3s_ratio = NumberUtils.formatDouble(visit_length_1s_3s / session_count, 2)
    val visit_length_4s_6s_ratio = NumberUtils.formatDouble(visit_length_4s_6s / session_count, 2)
    val visit_length_7s_9s_ratio = NumberUtils.formatDouble(visit_length_7s_9s / session_count, 2)
    val visit_length_10s_30s_ratio = NumberUtils.formatDouble(visit_length_10s_30s / session_count, 2)
    val visit_length_30s_60s_ratio = NumberUtils.formatDouble(visit_length_30s_60s / session_count, 2)
    val visit_length_1m_3m_ratio = NumberUtils.formatDouble(visit_length_1m_3m / session_count, 2)
    val visit_length_3m_10m_ratio = NumberUtils.formatDouble(visit_length_3m_10m / session_count, 2)
    val visit_length_10m_30m_ratio = NumberUtils.formatDouble(visit_length_10m_30m / session_count, 2)
    val visit_length_30m_ratio = NumberUtils.formatDouble(visit_length_30m / session_count, 2)

    val step_length_1_3_ratio = NumberUtils.formatDouble(step_length_1_3 / session_count, 2)
    val step_length_4_6_ratio = NumberUtils.formatDouble(step_length_4_6 / session_count, 2)
    val step_length_7_9_ratio = NumberUtils.formatDouble(step_length_7_9 / session_count, 2)
    val step_length_10_30_ratio = NumberUtils.formatDouble(step_length_10_30 / session_count, 2)
    val step_length_30_60_ratio = NumberUtils.formatDouble(step_length_30_60 / session_count, 2)
    val step_length_60_ratio = NumberUtils.formatDouble(step_length_60 / session_count, 2)

    val stat = SessionAggrStat(taskUUID, session_count.toInt, visit_length_1s_3s_ratio, visit_length_4s_6s_ratio, visit_length_7s_9s_ratio,
      visit_length_10s_30s_ratio, visit_length_30s_60s_ratio, visit_length_1m_3m_ratio,
      visit_length_3m_10m_ratio, visit_length_10m_30m_ratio, visit_length_30m_ratio,
      step_length_1_3_ratio, step_length_4_6_ratio, step_length_7_9_ratio,
      step_length_10_30_ratio, step_length_30_60_ratio, step_length_60_ratio)
    // 将stat封装为RDD
    val sessionRatioRDD = sparkSession.sparkContext.makeRDD(Array(stat))

    import sparkSession.implicits._
    sessionRatioRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .option("dbtable", "session_stat_ratio")
      .mode(SaveMode.Append)
      .save()
  }


  //``````````````````````````````````````需求二````````````````````````````````````````````


  def generateRandomIndexList(extractPerDay: Long, // 一天一共获取多少个
                              daySessionCount: Long, // 一天一共有多少个
                              hourCountMap: mutable.HashMap[String, Long], // 每个小时有多少个
                              hourListMap: mutable.HashMap[String, ListBuffer[Int]]): Unit = {

    for ((hour, count) <- hourCountMap) {
      // 获取一个小时要货=抽取多少条数据
      var hourExrCount = ((count / daySessionCount.toDouble) * extractPerDay).toInt
      // 避免一个小时要抽取的数量超过这个小时的总数
      if (hourExrCount > count) {
        hourExrCount = count.toInt
      }
      val random = new Random()
      hourListMap.get(hour) match {
        case None => hourListMap(hour) = new ListBuffer[Int]
          // 根据ListBuffer生成随机出
          for (i <- 0 until hourExrCount) {
            // 索引不可以超过总的个数
            var index = random.nextInt(count.toInt)
            while (hourListMap(hour).contains(index)) {
              // 校验去重，最新的index是否包含在hourListMap，在的话重新生成
              index = random.nextInt(count.toInt)
            }
            hourListMap(hour).append(index)
          }
        case Some(list) =>
          for (i <- 0 until hourExrCount) {
            var index = random.nextInt(count.toInt)
            while (hourListMap(hour).contains(index)) {
              index = random.nextInt(count.toInt)
            }
            hourListMap(hour).append(index)
          }
      }
    }
  }

  def sessionRandomExtract(sparkSession: SparkSession,
                           taskUUID: String,
                           sessionId2FilterRDD: RDD[(String, String)]) = {
    // dateHour2FullInfoRDD:RDD[dateHour,fullInfo]  一个session
    // 将session转化为时间的格式
    val dateHour2FullInfoRDD = sessionId2FilterRDD.map {
      case (sid, fullInfo) =>
        val startTime = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_START_TIME)
        //转化时间格式 dayHour = yyyy-MM-dd_hh
        val dateHour = DateUtils.getDateHour(startTime)
        (dateHour, fullInfo)
    }
    // chourCountMap:Map[(dateHour,count)]  每个小时的session数量
    val hourCountMao = dateHour2FullInfoRDD.countByKey()

    //  dateHourCountMap:Map[(Date,count)]
    val dateHourCountMap = new mutable.HashMap[String, mutable.HashMap[String, Long]]()

    for ((datehour, count) <- hourCountMao) {
      val date = datehour.split("_")(0)
      // 1小时
      val hour = datehour.split("_")(1)

      dateHourCountMap.get(date) match {
        // 双层map的底层map，ppt63
        case None => dateHourCountMap(date) = new mutable.HashMap[String, Long]()
          dateHourCountMap(date) += (hour -> count)
        // 进入该分支，追加到第二层map
        case Some(map) => dateHourCountMap(date) += (hour -> count)
      }
    }
    // 解决问题一: 一共多少天:  dateHourCountMap.size
    //              一天抽取多少条 100 / dateHourCountMap.size
    val extractPerDay = 100 / dateHourCountMap.size

    // 解决问题二: 一天有多少session: dateHourCountMap(date).values.sum
    // 解决问题三: 一个小时有多少session: dateHourCountMap(date)(hour)

    // 天  小时  ppt60
    val dateHourExtractIndexListMap = new mutable.HashMap[
      String, mutable.HashMap[String, ListBuffer[Int]]]()

    //  dateHourCountMap:Map[(Date,count)]
    for ((date, hourCountMap) <- dateHourCountMap) {
      val dateSessionCount = hourCountMap.values.sum

      dateHourExtractIndexListMap.get(date) match {
        // 当前listMap没有date对应的数据，创建date hour null
        case None => dateHourExtractIndexListMap(date) = new mutable.HashMap[String,
          ListBuffer[Int]]()

          generateRandomIndexList(extractPerDay, dateSessionCount, hourCountMap,
            dateHourExtractIndexListMap(date))
        // 获取每个小时抽取的session的index
        case Some(map) =>
          generateRandomIndexList(extractPerDay, dateSessionCount, hourCountMap,
            dateHourExtractIndexListMap(date))
      }
      // 防止数据量过大，需要广播大变量，提升任务性能
      val dateHourExtractIndexListMapBd = sparkSession.sparkContext.broadcast(dateHourExtractIndexListMap)

      //  dateHour2GroupRDD:RDD[(dateHour,iterableFullInfo)]
      val dateHour2GroupRDD = dateHour2FullInfoRDD.groupByKey()

      //  extractSessionRDD:SessionRandomExtract
      val extractSessionRDD = dateHour2GroupRDD.flatMap {
        case (dateHour, iterableFullInfo) =>
          val date = dateHour.split("_")(0)
          val hour = dateHour.split("_")(1)

          val extractList = dateHourExtractIndexListMapBd.value.get(date).get(hour)
          // 创建容器
          val extractSessionArrayBuffer = new ArrayBuffer[SessionRandomExtract]()

          var index = 0
          for (fullInfo <- iterableFullInfo) {
            if (extractList.contains(index)) {
              val sessionId = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_SESSION_ID)
              val startTime = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_START_TIME)
              val searchKeywords = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_SEARCH_KEYWORDS)
              val clickCateGories = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.PARAM_CATEGORY_IDS)

              val extractSession = SessionRandomExtract(taskUUID, sessionId, startTime, searchKeywords, clickCateGories)
              // SessionRandomExtract写入到extractSessionArrayBuffer
              extractSessionArrayBuffer += extractSession

            }
            index += 1
          }
          extractSessionArrayBuffer
      }
      import sparkSession.implicits._
      extractSessionRDD.toDF().write
        .format("jdbc")
        .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
        .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
        .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
        .option("dbtable", "session_extract")
        .mode(SaveMode.Append)
        .save()
    }
  }


  /*```````````````````````````````````````需求三`````````````````````````````````````````````````````````````*/


  def top10PopularCategories(sparkSession: SparkSession,
                             taskUUID: String,
                             sessionId2FilterActionRDD: RDD[(String, UserVisitAction)]) = {
    //  1.获取所有发生过点击，下单，付款的种类
    var cid2CidRDD = sessionId2FilterActionRDD.flatMap {
      case (sid, action) =>
        val categoryBuffer = new ArrayBuffer[(Long, Long)]()

        // 点击行为
        if (action.click_category_id != -1) {
          // 符合条件，放进categoryBuffer容器，这里只关注k，v可以是任意
          categoryBuffer += ((action.click_category_id, action.click_category_id))
        } else if (action.order_category_ids != null) { //  下单
          for (orderCid <- action.order_category_ids.split(","))
            categoryBuffer += ((orderCid.toLong, orderCid.toLong))
        } else if (action.pay_category_ids != null) { //  付款
          for (payCid <- action.pay_category_ids.split(","))
            categoryBuffer += ((payCid.toLong, payCid.toLong))
        }
        categoryBuffer
    }
    // 一个商品可以被点击，下单，购买，去重操作
    cid2CidRDD = cid2CidRDD.distinct()

    // 2.统计品类被点击次数
    val cid2ClickCountRDD = getClickCount(sessionId2FilterActionRDD)
    // cid2ClickCountRDD.foreach(println(_))

    // 订单次数
    val cid2OrderCountRDD = getOrderCount(sessionId2FilterActionRDD)
    // cid2OrderCountRDD.foreach(println(_))

    // 支付次数
    val cid2PayCountRDD = getPayCount(sessionId2FilterActionRDD)
    // cid2PayCountRDD.foreach(println(_))

    // 对四个RDD进行联合操作
    // (51,categoryid=51|clickCount=55|payCount=61) RDD[(cid,countInfo)]
    val cid2FullCountRDD = getFullCount(cid2CidRDD, cid2ClickCountRDD, cid2OrderCountRDD, cid2PayCountRDD)
    // cid2FullCountRDD.foreach(println(_))

    // 实现二次排序
    // 实现自定义二次排序key
    val sortKey2FullCountRDD = cid2FullCountRDD.map {
      case (cid, countInfo) =>
        val clickCount = StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_CLICK_COUNT).toLong
        val orderCount = StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_ORDER_COUNT).toLong
        val payCount = StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_PAY_COUNT).toLong

        val sortKey = SortKey(clickCount, orderCount, payCount)

        (sortKey, countInfo)
    }
    val top10CategoryArray = sortKey2FullCountRDD.sortByKey(false).take(10)

    // 先转化为RDD
    val top10CategoryRDD = sparkSession.sparkContext.makeRDD(top10CategoryArray).map {
      case (sortKey, countInfo) =>
        val cid = StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_CATEGORY_ID).toLong
        val clickCount = sortKey.clickCount
        val orderCount = sortKey.orderCount
        val payCount = sortKey.payCount

        Top10Category(taskUUID, cid, clickCount, orderCount, payCount)
    }
    import sparkSession.implicits._
    top10CategoryRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .option("dbtable", "top10_category")
      .mode(SaveMode.Append)
      .save

    top10CategoryArray
  }

  def getClickCount(sessionId2FilterActionRDD: RDD[(String, UserVisitAction)]) = {

    // 1.先过滤点击的数据
    //    val clickFilterRDD = sessionId2FilterActionRDD.filter{
    //      case (sessionId,action ) => action.click_category_id != -1L
    //    }

    // 先进行过滤，把点击行为对应的action保留下来
    val clickFilterRDD = sessionId2FilterActionRDD.filter(item => item._2.click_category_id != -1L)
    // 进行格式转换，为reduceByKey做准备
    val clickNumRDD = clickFilterRDD.map {
      case (sessionId, action) => (action.click_category_id, 1L)
    }
    // 点击的所有品牌
    clickNumRDD.reduceByKey(_ + _)
  }

  def getOrderCount(sessionId2FilterActionRDD: RDD[(String, UserVisitAction)]) = {
    val orderFilterRDD = sessionId2FilterActionRDD.filter(item => item._2.order_category_ids != null)

    val orderNumRDD = orderFilterRDD.flatMap {
      // action.order_category_ids.split(","):Array[String]
      // action.order_category_ids.split(",").map(item => (item.toLong,1L))
      // 先将字符串拆分成字符串数组，书用map转换数组中的每个元素，原来数组的每个元素都是一个String
      // 现在是(Long,1L)
      case (sessionId, action) => action.order_category_ids.split(",")
        .map(item => (item.toLong, 1L))
    }
    orderNumRDD.reduceByKey(_ + _)
  }

  def getPayCount(sessionId2FilterActionRDD: RDD[(String, UserVisitAction)]) = {

    val payFilterRDD = sessionId2FilterActionRDD.filter(item => item._2.pay_category_ids != null)
    //过滤
    val payNumRDD = payFilterRDD.flatMap {
      case (sessionId, action) => action.pay_category_ids.split(",")
        .map(item => (item.toLong, 1L))
    }
    payNumRDD.reduceByKey(_ + _)
  }

  // 每个品类被点击，下单，支付多少次
  def getFullCount(cid2CidRDD: RDD[(Long, Long)],
                   cid2ClickCountRDD: RDD[(Long, Long)],
                   cid2OrderCountRDD: RDD[(Long, Long)],
                   cid2PayCountRDD: RDD[(Long, Long)]) = {
    val cid2ClickInfoRDD = cid2CidRDD.leftOuterJoin(cid2ClickCountRDD).map {
      case (cid, (categoryId, option)) =>
        val clickCount = if (option.isDefined) option.get else 0
        val aggrCount = Constants.FIELD_CATEGORY_ID + "=" + cid + "|" +
          Constants.FIELD_CLICK_COUNT + "=" + clickCount

        (cid, aggrCount)
    }
    val cid2OrderInfoRDD = cid2ClickInfoRDD.leftOuterJoin(cid2OrderCountRDD).map {
      case (cid, (clickInfo, option)) =>
        val orderCount = if (option.isDefined) option.get else 0
        val aggrInfo = clickInfo + "|" +
          Constants.FIELD_ORDER_COUNT + "=" + orderCount

        (cid, aggrInfo)
    }

    val cid2PayInfoRDD = cid2OrderInfoRDD.leftOuterJoin(cid2PayCountRDD).map {
      case (cid, (orderInfo, option)) =>
        val payCount = if (option.isDefined) option.get else 0
        val aggrInfo = orderInfo + "|" +
          Constants.FIELD_PAY_COUNT + "=" + payCount

        (cid, aggrInfo)
    }
    cid2PayInfoRDD
  }


  /*`````````````````````````````````需求四`````````````````````````````*/

  def top10ActiveSession(sparkSession: SparkSession,
                         taskUUID: String,
                         sessionId2FilterActionRDD: RDD[(String, UserVisitAction)],
                         top10CategoryArray: Array[(SortKey, String)]): Unit = {
    //  第一步：过滤出所有点击过的Top10品类的action
    //  1.j过滤之后 cid2CountInfoRDD join sessionId2FilterActionRDD
    //  方案一：join保留两边都有的数据
    /*    val cid2CountInfoRDD = sparkSession.sparkContext.makeRDD(top10CategoryArray).map{
          case(sortKey,countInfo) =>
            val cid = StringUtils.getFieldFromConcatString(countInfo,"\\|",
              Constants.FIELD_CATEGORY_ID).toLong
            (cid,countInfo)
        }
          val cid2ActionRDD = sessionId2FilterActionRDD.map{
            case (sessionId,action) =>
              val cid = action.click_category_id
              (cid,action)
        }
        // join
        cid2CountInfoRDD.join(cid2ActionRDD).map{
          case (cid,(countInfo,action)) =>
            val sid = action.session_id
            (sid,action)
        }
    */
    //  方案二：filter
    //  cidArray:Array[Long],包含了热门品类信息
    val cidArray = top10CategoryArray.map {
      case (sprtKey, countInfo) =>
        val cid = StringUtils.getFieldFromConcatString(countInfo, "\\|",
          Constants.FIELD_CATEGORY_ID).toLong
        cid
    }
    //  所有符合过滤条件的，斌且点击过热门品类的热门数据
    val sessionId2ActionRDD = sessionId2FilterActionRDD.filter {
      case (sessionId, action) =>
        cidArray.contains(action.click_category_id)
    }
    // 按照sessionId进行聚合操作
    val sessionId2GroypRDD = sessionId2ActionRDD.groupByKey()

    //  cid2SessionCountRDD；RDD[(cid,sessionCount)]
    val cid2SessionCountRDD = sessionId2GroypRDD.flatMap {
      case (sessionId, iterableAction) =>
        val categoryCountMap = new mutable.HashMap[Long, Long]()
        for (action <- iterableAction) {
          val cid = action.click_category_id
          if (!categoryCountMap.contains(cid))
            categoryCountMap += (cid -> 0)
          categoryCountMap.update(cid,categoryCountMap(cid) +1)
        }
        //  记录了一个session对于它所有点击过的品类的点击次数
        for ((cid,count) <- categoryCountMap)
          yield (cid,sessionId + "=" + count)
    }

    //  cid2GroupRDD: RDD[(cid,iter ableSessionCount)]
    //  cidGroupRDD每一条数据都是一个category和它对应的所有点击过它的session对它的点击次数
    val cid2GroupRDD = cid2SessionCountRDD.groupByKey()

    val top10SessionRDD = cid2GroupRDD.flatMap{
      case (cid,iterableSessionCount) =>
      //  item:sessionCount String "sessionId=count"
        // GroupBy 之后对iterableSessionCount进行排序
      val sortList = iterableSessionCount.toList.sortWith((item1,item2) =>{
        // true :item1放在前面
        //  flase :item2放在前面
        // 取第一个值，long类型比较
        item1.split("=")(1).toLong > item2.split("=")(1).toLong
      }).take(10)
        // session封装
       val top10Session = sortList.map{
          //  item : sessionCount String "sessionId=count"
//          case class Top10Session(taskid:String,
//                                  categoryid:Long,
//                                  sessionid:String,
//                                  clickCount:Long)
          case item =>
            val sessionId = item.split("=")(0)
            val count = item.split("=")(1).toLong
            Top10Session(taskUUID,cid,sessionId,count)
        }
        top10Session
    }
    import sparkSession.implicits._
    top10SessionRDD.toDF().write
      .format("jdbc")
      .option("url",ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("user",ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password",ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .option("dbtable","top10_session")
      .mode(SaveMode.Append)
        .save

  }
}
