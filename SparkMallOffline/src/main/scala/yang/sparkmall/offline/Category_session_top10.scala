package yang.sparkmall.offline

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.UUID

import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import yang.sparkmall.common.model.UserVisitAction
import yang.sparkmall.common.util.{ConfigUtil, StringUtil}

import scala.collection.{immutable, mutable}

object Category_session_top10 {
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

		//"========================================================需求二代码========================"

		// TODO 4.1 将数据进行过滤筛选，留下满足条件数据（点击数据，品类前10）
		//获取最受欢迎的前十的品类的ID，并声明为广播变量
		val ids: List[String] = top10.map(_.categoryId)
		val idsBroadcast: Broadcast[List[String]] = spark.sparkContext.broadcast(ids)

		//println(actionRDD.count())
		//对2019-07月份的数据进行过滤，没有点击的和品类不在前十里面的数据删除掉
		val filterRDD: RDD[UserVisitAction] = actionRDD.filter(action => {
			if (action.click_category_id != -1) {
				idsBroadcast.value.contains(action.click_category_id.toString)
			} else {
				false
			}

		})
		//println(filterRDD.count())
		// TODO 4.2 将过滤后的数据进行结构的转换（ categoryId+sessionId, 1 ）丢掉无用的数据
		val categoryIdAndSessionToOneRDD: RDD[(String, Int)] = filterRDD.map(action => {
			(action.click_category_id + "_" + action.session_id, 1)
		})


		// TODO: 4.3 将结构转换后的数据进行聚合统计（category+session，sum）
		val categoryIdAndSessionToSumRDD: RDD[(String, Int)] = categoryIdAndSessionToOneRDD.reduceByKey(_ + _)


		// TODO: 4.4 将聚合后的数据转换结构
		//尽量使用模式匹配
		val categoryToSessionAndSumRDD: RDD[(String, (String, Int))] = categoryIdAndSessionToSumRDD.map {
			case (key, sum) => {
				val keys: Array[String] = key.split("_")
				(keys(0), (keys(1), sum))
			}
		}


		/*val categoryToSessionAndSumRDD: RDD[(String, (String, Int))] = categoryIdAndSessionToSumRDD.map(t => {
			val strings: Array[String] = t._1.split("_")
			(strings(0), (strings(1), t._2))
		})*/

		// TODO: 4.5 把数据按照品类进行分组
		val groupRDD: RDD[(String, Iterable[(String, Int)])] = categoryToSessionAndSumRDD.groupByKey()

		// TODO: 4.6 将分组后的数据进行排序取前十
		val resultRDD: RDD[(String, List[(String, Int)])] = groupRDD.mapValues(datas => {
			datas.toList.sortWith((Left, Right) => {
				Left._2 > Right._2
			})
		}.take(10))
		//把每条数据转换成类
		val mapRDD: RDD[List[CategorySessionTop10]] = resultRDD.map {
			case (categoryId, list) => {
				list.map {
					case (sessionId, sum) => {
						CategorySessionTop10(taskId, categoryId, sessionId, sum)
					}
				}
			}
		}
		//最终的RDD
		val dataRDD: RDD[CategorySessionTop10] = mapRDD.flatMap(List => List)

		// TODO: 将结果保存进mysql中
		dataRDD.foreachPartition(datas => {
			val dirverClass = ConfigUtil.getValueByKey("jdbc.driver.class")
			val url = ConfigUtil.getValueByKey("jdbc.url")
			val user = ConfigUtil.getValueByKey("jdbc.user")
			val password = ConfigUtil.getValueByKey("jdbc.password")

			Class.forName(dirverClass)

			val connection: Connection = DriverManager.getConnection(url, user, password)

			val insertSql = "insert into category_top10_session_count( taskId, categoryId, sessionId, clickCount ) values ( ?, ?, ?, ? ) "

			val stat: PreparedStatement = connection.prepareStatement(insertSql)

			datas.foreach(data => {
				stat.setString(1, data.taskId)
				stat.setString(2, data.categoryId)
				stat.setString(3, data.sessionId)
				stat.setLong(4, data.clickCount)
				stat.executeUpdate()
			})

			stat.close()
			connection.close()
		})


		spark.stop()
	}

}

// TODO: 样例类
case class CategorySessionTop10(taskId: String, categoryId: String, sessionId: String, clickCount: Long)