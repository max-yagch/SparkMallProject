package yang.sparkmall.offline


import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.UUID

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.util.AccumulatorV2
import yang.sparkmall.common.model.UserVisitAction
import yang.sparkmall.common.util.{ConfigUtil, StringUtil}

import scala.collection.{immutable, mutable}

object Category_top10 {
	def main(args: Array[String]): Unit = {
		// TODO: 1、创建sparkSql的环境对象
		val sparkConf: SparkConf = new SparkConf().setAppName("Category_top10").setMaster("local[*]")
		val spark: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
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
		//每一条都是完整的数据
		val actionRDD: RDD[UserVisitAction] = actionDS.rdd



		// TODO: 3、使用累加器

		//创建累加器对象
		val accumulator = new CategoryAccumulator
		//注册累加器
		spark.sparkContext.register(accumulator, "Category")
		//遍历基础数据的RDD 转换数据结构 添加-click -order -pay ，数据都添加进累加器中，累加器中的数据类型是map
		//只添加add进去的数据，自动进行次数的累计和统计
		actionRDD.foreach {
			actionData => {
				if (actionData.click_category_id != -1) {
					accumulator.add(actionData.click_category_id + "-click")
				} else if (StringUtil.isNotEmpty(actionData.order_category_ids)) {
					//123
					val strings: Array[String] = actionData.order_category_ids.split(",")
					for (id <- strings) {
						accumulator.add(id + "-order")
					}
				} else if (StringUtil.isNotEmpty(actionData.pay_category_ids)) {
					val strings: Array[String] = actionData.pay_category_ids.split(",")
					for (id <- strings) {
						accumulator.add(id + "-pay")
					}
				}

			}
		}
		//获取累加器中的数据 map集合
		val accuData: mutable.HashMap[String, Long] = accumulator.value


		// TODO:  4、将累加器的结果通过品类id进行分组
		//集合的操作
		val accuDataToCategory: Map[String, mutable.HashMap[String, Long]] = accuData.groupBy {
			case (key, suncount) => {
				val keys: Array[String] = key.split("-")
				keys(0)
			}
		}

		// TODO:5、将分组结果装换为对象

		val taskId: String = UUID.randomUUID().toString
		//使用样例类，只把有用的数据分装进成对象
		val categorytop10Datas: immutable.Iterable[Category_top10] = accuDataToCategory.map {
			case (categoryId, map) => {
				Category_top10(
					taskId,
					categoryId,
					map.getOrElse(categoryId + "-click", 0L),
					map.getOrElse(categoryId + "-order", 0L),
					map.getOrElse(categoryId + "-pay", 0L)
				)
			}
		}
		// TODO: 6、排序
		val sort: List[Category_top10] = categorytop10Datas.toList.sortWith {
			(Left, Right) => {

				if (Left.clickCount > Right.clickCount) {
					true
				} else if (Left.clickCount == Right.clickCount) {
					if (Left.orderCount > Right.orderCount) {
						true

					} else if (Left.orderCount > Right.orderCount) {
						if (Left.payCount > Right.payCount) {
							true
						} else {
							false
						}
					}
					else {
						false
					}
				} else {
					false
				}

			}
		}


		// TODO: 7、取前十 存进mysql

		val top10: List[Category_top10] = sort.take(10)
		//println(top10)
		val drvierClass = ConfigUtil.getValueByKey("jdbc.driver.class")
		val url = ConfigUtil.getValueByKey("jdbc.url")
		val user
		= ConfigUtil.getValueByKey("jdbc.user")
		val password = ConfigUtil.getValueByKey("jdbc.password")

		Class.forName(drvierClass)
		val connection: Connection = DriverManager.getConnection(url, user, password)

		val insertSQL = "insert into category_top10 ( taskId, category_id, click_count, order_count, pay_count ) values ( ?, ?, ?, ?, ? )"

		val pstat: PreparedStatement = connection.prepareStatement(insertSQL)

		top10.foreach {
			data => {
				pstat.setString(1, data.taskId)
				pstat.setString(2, data.categoryId)
				pstat.setLong(3, data.clickCount)
				pstat.setLong(4, data.orderCount)
				pstat.setLong(5, data.payCount)
				pstat.executeUpdate()
			}
		}

		pstat.close()
		connection.close()


		spark.stop()
	}

}

// TODO: 样例类
case class Category_top10(taskId: String, categoryId: String, clickCount: Long, orderCount: Long, payCount: Long)


// TODO: 声明累加器

class CategoryAccumulator extends AccumulatorV2[String, mutable.HashMap[String, Long]] {

	var map = new mutable.HashMap[String, Long]()

	override def isZero: Boolean = map.isEmpty

	override def copy(): AccumulatorV2[String, mutable.HashMap[String, Long]] = {
		new CategoryAccumulator
	}

	override def reset(): Unit = {
		map.clear()
	}

	override def add(v: String): Unit = {
		map(v) = map.getOrElse(v, 0L) + 1
	}

	override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Long]]): Unit = {
		var map1 = map
		var map2 = other.value

		map = map1.foldLeft(map2) {
			(innermap, t) => {
				innermap(t._1) = innermap.getOrElse(t._1, 0L) + t._2
				innermap
			}
		}

	}

	override def value: mutable.HashMap[String, Long] = map
}