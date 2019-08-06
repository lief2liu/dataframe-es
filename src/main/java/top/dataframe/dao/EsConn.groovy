package top.dataframe.dao

import groovy.transform.CompileStatic
import org.apache.http.HttpHost
import org.apache.http.auth.AuthScope
import org.apache.http.auth.UsernamePasswordCredentials
import org.apache.http.client.CredentialsProvider
import org.apache.http.impl.client.BasicCredentialsProvider
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder
import org.elasticsearch.client.RestClient
import org.elasticsearch.client.RestClientBuilder
import top.dataframe.bean.DateRangeRes
import top.dataframe.val.EsInfoKey

@CompileStatic
class EsConn {
    RestClient get(String username, String password, String hostname, int port) {
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider()
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(username, password))
        RestClient
                .builder(new HttpHost(hostname, port))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                        return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider)
                    }
                }).build()
    }

    RestClient get(Map<String, Object> esInfo) {
        String username = esInfo.get(EsInfoKey.USER)
        String password = esInfo.get(EsInfoKey.PASSWORD)
        String hostname = esInfo.get(EsInfoKey.NODES)
        int port = esInfo.get(EsInfoKey.PORT) as int
        get(username, password, hostname, port)
    }
}
