package cn.rain.config;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * description:
 *
 * @author 任伟
 * @date 2018/5/29 9:56
 */
@SuppressWarnings("all")
@Configuration
public class MyESConfig {

    @Bean
    public TransportClient client() throws UnknownHostException {
        // 注意，这里配置的端口是es的TCP端口（默认是9300），不是http端口（9200）
        InetSocketTransportAddress master = new InetSocketTransportAddress(InetAddress.getByName("192.168.1.61"), 9300);
        InetSocketTransportAddress node1 = new InetSocketTransportAddress(InetAddress.getByName("192.168.1.62"), 9300);
        InetSocketTransportAddress node2 = new InetSocketTransportAddress(InetAddress.getByName("192.168.1.63"), 9300);

        Settings settings = Settings.builder()
                .put("cluster.name", "escluster-rain") // 设置集群的名称，必须设置，否则找不到
                .build();


        TransportClient client = new PreBuiltTransportClient(settings);
        client.addTransportAddress(master);
        client.addTransportAddress(node1);
        client.addTransportAddress(node2);

        return client;
    }
}