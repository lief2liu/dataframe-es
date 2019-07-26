package top.dataframe.spark.conf

import groovy.transform.CompileStatic

@CompileStatic
class SparkEsExt implements SparkConfExt {
    private Map<String, String> esInfo

    SparkEsExt(Map<String, String> esInfo) {
        this.esInfo = esInfo
    }

    @Override
    SparkConfig enable(SparkConfig config) {
        config.set("es.write.operation", "upsert")
                .set("es.mapping.id", "_id")
                .set("es.mapping.exclude", "_id")
                .set("es.nodes.wan.only", "true")
                .set("es.nodes.data.only", "false")

        esInfo?.each {
            config.set(it.key, it.value)
        }

        config
    }
}
