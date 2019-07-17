
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.{SparkConf, rdd}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis
import util.{KafkaUtil, redisUtil}
import java.util

import yang.sparkmall.common.util.Datautil

object Seq4blockNameList {
	def main(args: Array[String]): Unit = {


		// 需求四：广告黑名单实时统计
		// TODO: 准备SparkStreaming上下文环境对象
		val conf: SparkConf = new SparkConf().setAppName("Seq4blockNameList").setMaster("local[*]")
		//streamingcontext 创建 需要传入sparkcontext 和 一个时间间隔
		val streamingcontext: StreamingContext = new StreamingContext(conf, Seconds(5))

		val topic =  "ads_log"
		//设置检查点
		streamingcontext.sparkContext.setCheckpointDir("cp2")

		// TODO: 从kafka中获取数据
		val kafkaStreamingDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtil.getKafkaStream(topic, streamingcontext)

		val dataDStream: DStream[String] = kafkaStreamingDStream.map(rdd => rdd.value())

		val ObjDStream: DStream[kafkaObj] = dataDStream.map(t => {
			val strings: Array[String] = t.split(" ")
			kafkaObj(strings(0), strings(2), strings(2), strings(3), strings(4))
		})


		// TODO: 0.对数据进行筛选过滤，是黑名单里的数据就不要了


		// TODO: 会发生空指针异常，是因为序列化规则

		// TODO: 黑名单的数据无法更新，应该周期性的获得最新黑名单中的数据
		//dirver中的代码只能执行一次，像这样周期性的操作要用到transform 里面的代码会自定义执行次数
		//



		val filterDStream: DStream[kafkaObj] = ObjDStream.transform(rdd => {
			// Drvier(N)
			val jedisClient: Jedis = redisUtil.getJedisClient
			val userids: util.Set[String] = jedisClient.smembers("blacklist")


			jedisClient.close()
			// 使用广播变量
			val useridsBroadcast: Broadcast[util.Set[String]] = streamingcontext.sparkContext.broadcast(userids)
			rdd.filter(message => {
				// Executor(M)
				!useridsBroadcast.value.contains(message.userid)
			})
		})








		// TODO 1. 将数据转换结构 （date-ads-user, 1）
		val dateAdsUserToOneDStream: DStream[(String, Long)] = filterDStream.map {
			obj => {
				val date: String = Datautil.formatStringByTimestamp("yyyy-MM-dd", obj.timestamp.toLong)

				(date + "_" + obj.adsid + "_" + obj.userid, 1L)
			}
		}

		// TODO: 有状态聚合
		val dateAdsUserToSumDStream: DStream[(String, Long)] = dateAdsUserToOneDStream.updateStateByKey[Long] {
			(seq: Seq[Long], buffer: Option[Long]) => {
				val sum = buffer.getOrElse(0L) + seq.size
				Option(sum)
			}
		}


		dateAdsUserToSumDStream.foreachRDD{
			rdd=>{
				rdd.foreach{
					case (key,sum)=>{
						if (sum >=100){
							// TODO: 超过阀值，将用户加入黑名单
							val strings: Array[String] = key.split("_")
							val userid: String = strings(2)

							val client: Jedis = redisUtil.getJedisClient
							client.sadd("blacklist",userid)
							client.close()
						}
					}
				}
			}
		}


		streamingcontext.start()

		streamingcontext.awaitTermination()
	}

}

case class kafkaObj(timestamp: String, area: String, city: String, userid: String, adsid: String)