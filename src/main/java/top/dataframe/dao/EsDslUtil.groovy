package top.dataframe.dao

import groovy.transform.CompileStatic

@CompileStatic
class EsDslUtil {
    static String getUnionSql(List<String> arr) {
        arr.collect {
            "($it)"
        }.join(" union all ")
    }
}
