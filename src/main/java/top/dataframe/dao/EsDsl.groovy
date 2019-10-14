package top.dataframe.dao

import groovy.transform.CompileStatic
import top.dataframe.bean.DateRangeRes

@CompileStatic
class EsDsl {
//    static String getUnionSql(List<String> arr) {
//        arr.collect {
//            "($it)"
//        }.join(" union all ")
//    }

    DateRangeRes dateRange(String start, String end, String format, int defaultDays, String key) {
        if (!(end?.trim())) {
            end = new Date().format(format) as int
        }
        if (!(start?.trim())) {
            start = (Date.parse(format, end as String) - defaultDays).format(format) as int
        }

        String dsl = """
            {
                "range": {
                    "$key": {
                        "gte": "$start",
                        "lte": "$end"
                    }
                }
            }
        """.toString().trim()

        new DateRangeRes(start, end, dsl)
    }

    String range(String start, String end, String key) {
        """
            {
                "range": {
                    "$key": {
                        "gte": "$start",
                        "lte": "$end"
                    }
                }
            }
        """.toString().trim()
    }

    String query(String way, String key, String value) {
        value = value?.trim()
        if (!value) {
            return ""
        }
        if ("wildcard" == way) {
            value += "*"
        }
        """
            {
              "$way": {
                "$key": "$value"
              }
            }
        """.toString().trim()
    }
}
