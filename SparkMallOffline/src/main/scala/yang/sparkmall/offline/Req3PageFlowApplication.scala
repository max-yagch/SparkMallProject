package yang.sparkmall.offline

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import yang.sparkmall.common.model.UserVisitAction
import yang.sparkmall.common.util.{ConfigUtil, StringUtil}

object Req3PageFlowApplication {
	def main(args: Array[String]): Unit = {
		// TODO: 1、创建sparkSql的环境对象
		val sparkConf: SparkConf = new SparkConf().setAppName("Category_top10").setMaster("local[*]")
		val spark: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
		//设置检查点 都是sparkcontext 设置检查点
		spark.sparkContext.setCheckpointDir("cp")
		import spark.implicits._

		// TODO: 2、从 hive中获取满足条件的对象
		spark.sql("use " + ConfigUtil.getValueByKey("hive.database"))


		// TODO: 2.1、获取条件
		var sql = "select * from user_visit_action where 1=1"
		//val jsonString: String = ConfigUtil.getValueByKey("condition.params.json")

		val startDate: String = ConfigUtil.getValueByJsonKey("startDate")
		val endDate: String = ConfigUtil.getValueByJsonKey("endDate")

		if (StringUtil.isNotEmpty(startDate)) {
			sql = sql + " and date >= '" + startDate + "'"
		}
		if (StringUtil.isNotEmpty(endDate)) {
			sql = sql + " and date <= '" + endDate + "'"
		}
		val actionDF: DataFrame = spark.sql(sql)
		val actionDS: Dataset[UserVisitAction] = actionDF.as[UserVisitAction]
		//数据类型为对象

		/*
		  date
		  user_id
		  session_id
		  page_id
		  action_time
		  search_keyword
		  click_category_id
		  click_product_id
		  order_category_ids
		  order_product_ids
		  pay_category_ids
		  pay_product_ids
		  city_id
		  */
		val actionRDD: RDD[UserVisitAction] = actionDS.rdd

		// TODO: 2.2 使用检查点缓存数据
		actionRDD.checkpoint()




		//=============================需求三代码==========================
		// TODO: 3 计算分母


		//同于后面过滤分子使用
		val pageids: Array[String] = ConfigUtil.getValueByJsonKey("targetPageFlow").split(",")

		// (1-2), (2-3)
		val pageflowIds: Array[String] = pageids.zip(pageids.tail).map {
			case (pageid1, pageid2) => {
				pageid1 + "-" + pageid2
			}
		}


		//过滤分母中不需要的数据
		val filterRDD: RDD[UserVisitAction] = actionRDD.filter(action => {
			pageids.contains(action.page_id.toString)
		})
		//转换结构并进行聚合
		val pageIdToOne: RDD[(Long, Long)] = filterRDD.map(t => {
			(t.page_id, 1L)
		})
		val pageIdToSum: RDD[(Long, Long)] = pageIdToOne.reduceByKey(_ + _)
		val pageIdSingleMap: Map[Long, Long] = pageIdToSum.collect().toMap


		// TODO:  4.1 计算分子

		//把筛选下来的原始数据进行分组
		val groupRDD: RDD[(String, Iterable[UserVisitAction])] = actionRDD.groupBy(_.session_id)


		//排序
		val sessionToPageflowRDD: RDD[(String, List[(String, Long)])] = groupRDD.mapValues { datas => {
			val sortActions: List[UserVisitAction] = datas.toList.sortWith {
				(Left, Right) => {
					Left.action_time < Right.action_time
				}
			}
			//转换结构
			val sessionPageids: List[Long] = sortActions.map(_.page_id)
			//形成拉拉链
			val pageid1Topageid2s: List[(Long, Long)] = sessionPageids.zip(sessionPageids.tail)
			pageid1Topageid2s.map {
				case (pageid1, pageid2) => {
					(pageid1 + "-" + pageid2, 1L)
				}
			}


		}
		}

		//不要session
		val mapRDD: RDD[List[(String, Long)]] = sessionToPageflowRDD.map(_._2)

		val flatmapRDD: RDD[(String, Long)] = mapRDD.flatMap(List => List)

		val finalRDD: RDD[(String, Long)] = flatmapRDD.filter {
			case (pageid, one) => {
				pageflowIds.contains(pageid)
			}
		}

		val resultRDD: RDD[(String, Long)] = finalRDD.reduceByKey(_ + _)

		resultRDD.foreach {
			case (pageflow, sum) => {

				val ids: Array[String] = pageflow.split("-")
				val pageid = ids(0)
				val sum1 = pageIdSingleMap.getOrElse(pageid.toLong, 1L)

				// 1-2
				println(pageflow + " = " + (sum.toDouble / sum1))
			}
		}


	}

}
