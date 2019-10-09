package top.dataframe.dao

import groovy.transform.CompileStatic
import org.apache.http.HttpEntity
import org.apache.http.entity.ContentType
import org.apache.http.nio.entity.NStringEntity
import org.apache.http.util.EntityUtils
import org.elasticsearch.client.Response
import org.elasticsearch.client.RestClient

@CompileStatic
class EsOp {
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

    static void dropIndex(Map<String, Object> esInfo, String index) {
        RestClient client = new EsConn().get(esInfo)
        try {
            HttpEntity entity = new NStringEntity("", ContentType.APPLICATION_JSON)
            client.performRequest("DELETE", "/$index", Collections.singletonMap("pretty", "true"), entity)
        } finally {
            client?.close()
        }
    }

    static String queryForString(RestClient client, String dsl, String index, String type) {
        try {
            HttpEntity entity = new NStringEntity(dsl, ContentType.APPLICATION_JSON)
            Response response = client.performRequest("POST", "/$index/$type/_search",
                    Collections.singletonMap("pretty", "true"), entity)
            String res = EntityUtils.toString(response.getEntity())
            return res
        } catch (IOException e) {
            e.printStackTrace()
            return null
        }
    }
}
