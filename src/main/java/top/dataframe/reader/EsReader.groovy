package top.dataframe.reader

import groovy.transform.CompileStatic

@CompileStatic
class EsReader implements DataFrameReader{
    String dsl
    Map<String, Object> connectInfo

    EsReader(Map<String, Object> connectInfo, String dsl){
        this.dsl = dsl
        this.connectInfo = connectInfo
    }

    @Override
    List<Map<String, Object>> read() {
        // TODO
//        Sql db
//        try {
//            db = Sql.newInstance(jdbcInfo)
//            List<GroovyRowResult> rows = db.rows(sql)
//            if (rows.size() > 0) {
//                return db.rows(sql) as List<Map<String, Object>>
//            }
//        } finally {
//            db?.close()
//        }

        null
    }
}
