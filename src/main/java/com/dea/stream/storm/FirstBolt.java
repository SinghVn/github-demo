package com.dea.stream.storm;


import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.PrintWriter;
import java.util.Map;

public class FirstBolt extends BaseBasicBolt {

    private PrintWriter printWriter;
    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        String fileName=stormConf.get("fileToWrite").toString();
        try{
            printWriter=new PrintWriter(fileName,"UTF-8");
        }catch (Exception e){
            throw new RuntimeException("Error opening the file"+ e.getMessage());
        }
    }
    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        String symbol= tuple.getValue(0).toString();
        Double price=(Double) tuple.getValueByField("price");
        Double prevClose=(Double) tuple.getValueByField("prev_close");
        String timestamp=tuple.getValue(1).toString();
        boolean gain=true;
        if(price<=prevClose){
            gain=false;
        }

        basicOutputCollector.emit(new Values(symbol,timestamp,price,gain));
        printWriter.println(symbol+","+timestamp+","+price+","+gain);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("company","timestamp","price","gain"));
    }

    @Override
    public void cleanup(){
        printWriter.close();
    }
}
