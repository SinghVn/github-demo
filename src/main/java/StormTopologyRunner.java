
import com.dea.stream.storm.FirstBolt;
import com.dea.stream.storm.FirstSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;


public class StormTopologyRunner {

    public static  void main(String args[]){

        TopologyBuilder builder= new TopologyBuilder();
        builder.setSpout("First-Spout",new FirstSpout());
        builder.setBolt("First-Bolt",new FirstBolt(),3).shuffleGrouping("First-Spout");

        StormTopology topology = builder.createTopology();
        Config conf= new Config();
        conf.setDebug(true);
        conf.put("fileToWrite","D:/big_data_2018/project_work/output.txt");
        LocalCluster localCluster= new LocalCluster();
        try{
            localCluster.submitTopology("First-Topology",conf,topology);
            Thread.sleep(10000);
        }catch (Exception e){
            e.getMessage();
        }finally {
         //   localCluster.shutdown();
        }

    }
}
