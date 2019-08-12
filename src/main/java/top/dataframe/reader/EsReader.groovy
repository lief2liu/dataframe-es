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
class EsReader implements DataFrameReader {
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
        if (resList.size() == 0) {
            if (!resMap["aggregations"]) {
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
            Map aggs = resMap["aggregations"] as Map
            if (aggs) {
                aggMap.putAll(aggs)
            }
        }

        if (rows.size() == 0) {
            return null
        }
        [rows, aggMap]
    }

    public static void main(String[] args) {
        String xx = """ 3827245
            3829571
            3082734
            3850239
            3871237
            3932440
            3833555
            4027493
            3884923
            3875995
            3820400
            3817150
            3834750
            3088234
            3088234
            3086943
            3830891
            3826977 """
        xx.eachLine { String it ->
            print "'${it.trim()}',"
        }
    }
}
