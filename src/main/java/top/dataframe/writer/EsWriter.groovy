package top.dataframe.writer


import groovy.transform.CompileStatic
import top.dataframe.DataFrame

@CompileStatic
class EsWriter implements DataFrameWriter{
    private DataFrame df
    private Map<String, Object> jdbcInfo
    private String tb

    EsWriter(Map<String, Object> jdbcInfo, String tb) {
        this.jdbcInfo = jdbcInfo
        this.tb = tb
    }

    @Override
    int write(DataFrame df) {
        this.df = df
        // TODO
//    int es(Map<String, Object> esInfo, String[] ids) {
//        println df.rows.collect {Map<String,Object> it->
//            String id = ids.collect { id ->
//                it.get(id)
//            }.inject { a, b ->
//                (a as String) + (b as String)
//            }
//
//            """
//                {"update":{"_id":"$id"}}\n{"doc":${new JsonOutput().toJson(it)}, "doc_as_upsert" : true}\n
//            """.toString().trim()
//        }.inject {a,b->
//            a+"\n"+b
//        }
//
//        0
//    }

        0
    }
}
