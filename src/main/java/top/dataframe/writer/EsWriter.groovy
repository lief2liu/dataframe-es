package top.dataframe.writer

import groovy.json.JsonOutput
import groovy.transform.CompileStatic
import org.apache.http.HttpEntity
import org.apache.http.entity.ContentType
import org.apache.http.nio.entity.NStringEntity
import org.apache.http.util.EntityUtils
import org.elasticsearch.client.Response
import org.elasticsearch.client.RestClient
import top.dataframe.DataFrame
import top.dataframe.dao.EsConn

@CompileStatic
class EsWriter implements DataFrameWriter {
    private Map<String, Object> esInfo
    private String index
    private String type

    EsWriter(Map<String, Object> esInfo, String index, String type) {
        this.esInfo = esInfo
        this.index = index
        this.type = type
    }

    @Override
    int write(DataFrame df) {
        RestClient client

        try {
            client = new EsConn().get(esInfo)

            String dsl = df.rows.collect { Map<String, Object> it ->
                String id = it.get("_id")
                it.remove("_id")
                """
                    {"update":{"_id":"$id"}}\n{"doc":${new JsonOutput().toJson(it)}, "doc_as_upsert" : true}\n
                """.toString().trim()
            }.inject { a, b ->
                a + "\n" + b
            }

            bulkUpsert(client, dsl + "\n{}")
        } finally {
            client?.close()
        }

        df.size()
    }

    private String bulkUpsert(RestClient client, String dsl) {
        HttpEntity entity = new NStringEntity(dsl, ContentType.APPLICATION_JSON)
        Response response = client.performRequest("POST", "/$index/$type/_bulk",
                Collections.singletonMap("pretty", "true"), entity)
        return EntityUtils.toString(response.getEntity())
    }
}
// 假如要传多个id
//            String id = ids.collect { id ->
//                it.get(id)
//            }.inject { a, b ->
//                (a as String) + (b as String)
//            }