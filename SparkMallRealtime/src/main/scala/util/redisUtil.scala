package util

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}
import yang.sparkmall.common.util.ConfigUtil

object redisUtil {
	//jedis连接池
	var jedisPool:JedisPool=null

	def getJedisClient: Jedis = {
		//如果连接池是空创建连接池
		if(jedisPool==null){

			val host = ConfigUtil.getValueByKey("redis.host")
			val port = ConfigUtil.getValueByKey("redis.port")

			val jedisPoolConfig = new JedisPoolConfig()

			jedisPoolConfig.setMaxTotal(100)  //最大连接数
			jedisPoolConfig.setMaxIdle(20)   //最大空闲
			jedisPoolConfig.setMinIdle(20)     //最小空闲
			jedisPoolConfig.setBlockWhenExhausted(true)  //忙碌时是否等待
			jedisPoolConfig.setMaxWaitMillis(500)//忙碌时等待时长 毫秒
			jedisPoolConfig.setTestOnBorrow(true) //每次获得连接的进行测试

			jedisPool=new JedisPool(jedisPoolConfig,host,port.toInt)
		}
		println(s"jedisPool.getNumActive = ${jedisPool.getNumActive}")
		jedisPool.getResource
	}
}
