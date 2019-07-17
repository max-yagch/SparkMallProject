package yang.sparkmall.common.util

import java.text.SimpleDateFormat
import java.util.Date

object Datautil {
	def formatStringByTimestamp(string: String = "yyyy-MM-dd HH-mm-ss",long: Long):String={
		formatStringByDate(new Date(long),string)

	}


	def formatStringByDate(d : Date, f:String = "yyyy-MM-dd HH:mm:ss"): String = {
		val format = new SimpleDateFormat(f)
		format.format(d)
	}
}
