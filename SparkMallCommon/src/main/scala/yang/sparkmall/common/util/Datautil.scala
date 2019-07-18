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

	def parseLongByString( s : String, f:String = "yyyy-MM-dd HH:mm:ss" ) : Long = {
		parseDateByString(s, f).getTime
	}

	def parseDateByString(s : String, f:String = "yyyy-MM-dd HH:mm:ss"): Date = {
		val sdf = new SimpleDateFormat(f)
		sdf.parse(s)
	}
}
