package yang.sparkmall.common.util

import java.io.InputStream
import java.util.{Properties, ResourceBundle}

import com.alibaba.fastjson.{JSON, JSONObject}

object ConfigUtil {

	private val bundle: ResourceBundle = ResourceBundle.getBundle("config")
	private val condBundle: ResourceBundle = ResourceBundle.getBundle("condition")

	def main(args: Array[String]): Unit = {
		//println(getValueByKey("hive.database"))
		println(getValueByJsonKey("startDate"))
	}

	/**
	  * 从条件中获取数据
	  *
	  * @param jsonkey
	  * @return
	  */
	def getValueByJsonKey(jsonkey: String): String = {
		val jsonString: String = condBundle.getString("condition.params.json")
		val jsonObj: JSONObject = JSON.parseObject(jsonString)
		jsonObj.getString(jsonkey)

	}

	/**
	  *
	  * @param key
	  */
	def getValueByKey(key: String) = {
		/* val stream: InputStream = Thread.currentThread().getContextClassLoader.getResourceAsStream("config.properties")
		 val properties = new Properties()
		 properties.load(stream)
		 properties.get(key)*/
		bundle.getString(key)
	}

}
