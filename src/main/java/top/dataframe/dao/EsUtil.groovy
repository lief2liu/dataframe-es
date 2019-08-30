package top.dataframe.dao

import groovy.transform.CompileStatic
import org.apache.http.HttpEntity
import org.apache.http.entity.ContentType
import org.apache.http.nio.entity.NStringEntity
import org.elasticsearch.client.RestClient

@CompileStatic
class EsUtil {
    static void createIndexWithMapping(Map<String, Object> esInfo, String index, String type, String mapping) {
        RestClient client = new EsConn().get(esInfo)
        try {
            HttpEntity entity = new NStringEntity("", ContentType.APPLICATION_JSON)
            client.performRequest("PUT", "/$index", Collections.singletonMap("pretty", "true"), entity)
            entity = new NStringEntity(mapping, ContentType.APPLICATION_JSON)
            client.performRequest("POST", "/$index/$type/_mapping", Collections.singletonMap("pretty", "true"), entity)
        } finally {
            client?.close()
        }
    }
}
