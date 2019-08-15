package top.dataframe.reader

import groovy.json.JsonSlurper
import groovy.transform.CompileStatic
import org.apache.http.HttpEntity
import org.apache.http.entity.ContentType
import org.apache.http.nio.entity.NStringEntity
import org.apache.http.util.EntityUtils
import org.elasticsearch.client.Response
import org.elasticsearch.client.RestClient
import top.dataframe.dao.EsConn

@CompileStatic
class EsReader implements DataFrameReader,Serializable {
    private Map<String, Object> esInfo
    private String index
    private String type
    String dsl

    EsReader(Map<String, Object> esInfo, String index, String type, String dsl) {
        this.esInfo = esInfo
        this.index = index
        this.type = type
        this.dsl = dsl
    }

    @Override
    List read() {
        RestClient client
        try {
            client = new EsConn().get(esInfo)
            return query(client, dsl, index, type)
        } finally {
            client?.close()
        }
    }

    private String queryForString(RestClient client, String dsl, String index, String type) {
        try {
            HttpEntity entity = new NStringEntity(dsl, ContentType.APPLICATION_JSON)
            Response response = client.performRequest("POST", "/$index/$type/_search",
                    Collections.singletonMap("pretty", "true"), entity)
            return EntityUtils.toString(response.getEntity())
        } catch (IOException e) {
            e.printStackTrace()
            return null
        }
    }

    private List query(RestClient client, String dsl, String index, String type) {
        String res = queryForString(client, dsl, index, type)
        Map<String, Object> resMap = new JsonSlurper().parseText(res) as Map
        List resList = resMap["hits"]["hits"] as List
        Map aggMap = ["total": resMap["hits"]["total"]]
        List<Map<String, Object>> rows
        Map aggs = resMap["aggregations"] as Map
        if (resList.size() == 0) {
            if (!aggs) {
                return null
            }
            try {
                rows = resMap["aggregations"]["group_count"]["buckets"] as List<Map<String, Object>>
            } catch (Exception e) {
                rows = []
            }
        } else {
            rows = resList.collect {
                Map cloMap = it["_source"] as Map
                cloMap.remove("score")
                [id: it["_id"]] + cloMap
            }
        }

        if (rows.size() == 0) {
            return null
        }

        if (aggs) {
            aggMap.putAll(aggs)
            aggMap.remove("group_count")
        }

        [rows, aggMap]
    }
}
