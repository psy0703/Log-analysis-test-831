package com.ng.offline;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * 离线数据处理系统中的Kafka高级消费者程序将消息从Kafka集群中消费出来，然后写入指定的HDFS文件中，
 * 随后，通过crontab周期性地将HDFS中存储的日志文件加载到Hive数据仓库中。
 */
public class Kafka2HDFS {
    /**
     * HDFS文件系统
     */
    private static FileSystem fs = null;
    /**
     * HDFS输出流
     */
    private static FSDataOutputStream outputStream = null;
    /**
     * HDFS用户名
     */
    private static String user = "psy831";
    /**
     * HDFS主机路径
     */
    private static String hdfsPath = "hdfs://psy831:9000";

    public static void main(String[] args) throws IOException{
        //创建配置信息
        Properties props = new Properties();
        //设置连接到的zookeeper
        props.put("zookeeper.connect", "psy831:2181");
        //设置消费者组
        props.put("group.id", "g1");
        //设置zookeeper的 session连接超时
        props.put("zookeeper.session.timeout.ms", "500");
        //设置zookeeper同步时间间隔
        props.put("zookeeper.sync.time.ms", "250");
        //设置自动提交时间间隔
        props.put("auto.commit.interval.ms", "1000");

        //创建kafka消费者连接器
        ConsumerConnector consumerConnector = Consumer.createJavaConsumerConnector(new ConsumerConfig(props));
        //创建一个topicCountMap（主题&&消费该主题的线程数）
        HashMap<String, Integer> topicCountMap = new HashMap<>();
        topicCountMap.put("log-analysis", 1);
        //获取消息流
        Map<String, List<KafkaStream<byte[], byte[]>>> messageStreams = consumerConnector.createMessageStreams(topicCountMap);

        //根据主题以及线程顺序获取数据流
        List<KafkaStream<byte[], byte[]>> streams = messageStreams.get("log-analysis");
        KafkaStream<byte[], byte[]> stream = streams.get(0);

        ConsumerIterator<byte[], byte[]> iterator = stream.iterator();

        //创建文件存储的路径
        //获取当前时间
        long ts = System.currentTimeMillis();
        //创建一个时间格式化对象
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMM-dd HH:mm");
        //获取数据存储的路径（yyyyMM/dd/HHmm）
        String path = getPath(ts,sdf);

        try {
            //获取HDFS的文件系统
            fs = FileSystem.get(new URI(hdfsPath), new Configuration(), user);
            //获取输出流
            if(!fs.exists(new Path(path))){
                //如果存储路径不存在，创建输出流
                fs.create(new Path(path));
            } else{
                //如果文件路径已存在，将数据追加到该文件
                fs.append(new Path(path));
            }

        } catch (IOException | InterruptedException | URISyntaxException e) {
            e.printStackTrace();
        }

        while (iterator.hasNext()){
            //获取一行数据
            MessageAndMetadata<byte[], byte[]> messageAndMetadata = iterator.next();
           String log = new String(messageAndMetadata.message());
            //获取当前时间
            long currentTs = System.currentTimeMillis();

            //测试：每满一分钟将数据刷入新的文件
            if (currentTs - ts > 60000){
                //重新获取文件路径
                String currentPath = getPath(currentTs, sdf);
                outputStream.close();//关流
                //新的文件
                fs.create(new Path(currentPath));
                //滚动时间戳
                ts = currentTs;
            }
            //将数据输出到控制台
            System.out.println(log);

            //写入HDFS（测试按分钟存一个文件)
            outputStream.write((log + "\r\n").getBytes());
            //将数据落盘
            outputStream.hsync();
        }
    }

    private static String getPath(long currentTimeMillis, SimpleDateFormat sdf) {
        //"yyyyMM-dd HH:mm"
        String formatTime = sdf.format(new Date(currentTimeMillis));

        //yyyyMM/dd/HHmm
        String[] split = formatTime.split(" ");
        String parentPath = split[0].replaceAll("-", "/");
        String fileName = split[1].replaceAll(":", "/");
        String path = hdfsPath + "/logData/" + parentPath + "/" +fileName;
        return path;
    }
}
