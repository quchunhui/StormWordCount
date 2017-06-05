import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.StringTokenizer;

public class WordCountTopology {
    public static class WordCountSpout extends BaseRichSpout {
        SpoutOutputCollector _collector;
        Random _rand;

        @Override
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            _collector = collector;
            _rand = new Random();
        }

        @Override
        public void nextTuple() {
            // 睡眠一段时间后再产生一个数据
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            // 句子数组
            String[] sentences = new String[] {
                "Hello LaoNiu Bye LaoNiu",
                "Hello Storm Word Count",
                "Bye Storm Hello Storm",
                "Hello LaoNiu Count"
            };

            // 随机选择一个句子
            String sentence = sentences[_rand.nextInt(sentences.length)];

            // 发射该句子给Bolt
            System.out.print("[--------------]Spout emit=" + sentence + "\n");
            _collector.emit(new Values(sentence));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            // 定义一个字段word
            declarer.declare(new Fields("word"));
        }
    }

    public static class WordSplitBolt extends BaseRichBolt {
        OutputCollector _collector;

        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            _collector = collector;
        }

        @Override
        public void execute(Tuple input) {
            // 接收到一个句子
            String sentence = input.getString(0);
            // 把句子切割为单词
            StringTokenizer iter = new StringTokenizer(sentence);
            // 发送每一个单词
            while(iter.hasMoreElements()){
                _collector.emit(new Values(iter.nextToken()));
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            // 定义一个字段
            declarer.declare(new Fields("word"));
        }
    }

    public static class WordCountBolt extends BaseRichBolt {
        Map<String, Integer> counts = new HashMap<>();
        OutputCollector _collector;

        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            _collector = collector;
        }

        @Override
        public void execute(Tuple input) {
            // 接收一个单词
            String word = input.getString(0);
            // 获取该单词对应的计数
            Integer count = counts.get(word);

            // 计数增加
            if(count == null) {
                count = 0;
            } else {
                count++;
            }

            // 将单词和对应的计数加入map中
            counts.put(word, count);
            System.out.println(word + ":" + count);

            // 发送单词和计数（分别对应字段word和count）
            _collector.emit(new Values(word, count));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            // 定义两个字段word和count
            declarer.declare(new Fields("word", "count"));
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.print("[--------------]Start topology\n");
        // 创建一个拓扑
        TopologyBuilder builder = new TopologyBuilder();
        // 设置Spout，这个Spout的名字叫做"Spout"，设置并行度为5
        builder.setSpout("spout", new WordCountSpout(), 1);
        // 设置分词Bolt，并行度为8，它的数据来源是spout的
        builder.setBolt("split", new WordSplitBolt(), 1).shuffleGrouping("spout");
        // 设置计数Bolt,你并行度为12，它的数据来源是split的word字段
        builder.setBolt("count", new WordCountBolt(), 1).fieldsGrouping("split", new Fields("word"));

        Config conf = new Config();
        conf.setDebug(false);

        if(args != null && args.length > 0){
            System.out.print("[--------------]StormSubmitter Start\n");
            conf.setNumWorkers(3);
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
            System.out.print("[--------------]StormSubmitter End\n");
        }else{
            System.out.print("[--------------]LocalCluster Start\n");
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("WordCount", conf, builder.createTopology());
            System.out.print("[--------------]LocalCluster End\n");
            Thread.sleep(60000);
            cluster.shutdown();
            System.out.print("[--------------]Cluster Shutdown\n");
        }
    }
}
