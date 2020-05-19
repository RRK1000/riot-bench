package in.dream_lab.bm.stream_iot.storm.bolts.ETL.TAXI;

import in.dream_lab.bm.stream_iot.tasks.join.TableJoin;
import in.dream_lab.bm.stream_iot.tasks.AbstractTask;
import in.dream_lab.bm.stream_iot.tasks.filter.RangeFilterCheck;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JoinBolt extends BaseRichBolt {

	private Properties p;
	private int maxCountPossible;
	private HashMap<Long, String> itemIdMap;

	private ArrayList<String> schemaFieldOrderList;
	private String schemaFieldOrderFilePath;
	private String[] metaFields;

	public JoinBolt(Properties p_) {
		p = p_;
		maxCountPossible = Integer.parseInt(p_.getProperty("JOIN.MAX_COUNT_VALUE"));
		schemaFieldOrderFilePath = p_.getProperty("JOIN.SCHEMA_FILE_PATH");
		String metaField = p_.getProperty("JOIN.META_FIELD_SCHEMA");
		metaFields = metaField.split(",");

	}

	OutputCollector collector;
	private static Logger l;
	TableJoin joinTask;

	public static void initLogger(Logger l_) {
		l = l_;
	}

	@Override
	public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

		this.collector = outputCollector;
		initLogger(LoggerFactory.getLogger("APP"));
		joinTask = new TableJoin();
		schemaFieldOrderList = new ArrayList<String>();

		this.itemIdMap = new HashMap<Long, String>();
	}

	@Override
	public void execute(Tuple input) {
		System.out.println("Join Bolt!!!!");
        // String obsVal = (String) input.getValueByField("OBSVAL");

		// HashMap<String, String> map = new HashMap();
        // map.put(AbstractTask.DEFAULT_KEY, obsVal);
		// Float res = joinTask.doTask(map);
        // String updatedValue = (String) joinTask.getLastResult();
        // collector.emit(new Values(updatedValue,"1", "2"));
		// String fieldsList = input.getFields().toString();
		// String fields = fieldsList.substring(1, fieldsList.length() - 1);

		// schemaFieldOrderList = new ArrayList<String>();
		// for (String field : fields.split(", ")) {
		// 	System.out.println("Field: " + field);
		// 	schemaFieldOrderList.add(field);
		// }

		// String itemId = (String) input.getValueByField("ITEMID");
		// Long itemIdLong = Long.parseLong(itemId);
		// if (itemIdMap.containsKey(itemIdLong) == true) {
		// 	StringBuilder joinedValues = new StringBuilder();
		// 	for (String s : schemaFieldOrderList) {
		// 		joinedValues.append((String) input.getValueByField(s)).append(",");
		// 	}
		// 	String tmp = itemIdMap.get(itemIdLong).substring(2);
		// 	StringBuilder newValue = joinedValues.append(tmp);
		// 	joinedValues = newValue.deleteCharAt(joinedValues.length() - 1);
		// 	itemIdMap.remove(itemIdLong);
		// 	tmp = newValue.toString().substring(2);
		// 	collector.emit(new Values(itemId, tmp));
		// } else {
		// 	StringBuilder joinedValues = new StringBuilder();
		// 	for (String s : schemaFieldOrderList) {
		// 		joinedValues.append((String) input.getValueByField(s)).append(",");
		// 	}
		// 	itemIdMap.put(itemIdLong, joinedValues.toString());
		// 	joinedValues = joinedValues.deleteCharAt(joinedValues.length() - 1);
		// 	String tmp = joinedValues.toString().substring(2);
			collector.emit(new Values("itemId", "tmp", "tmp2"));

		// }
	}

	@Override
	public void cleanup() {
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("MSGID", "VALUE1", "VALUE2"));
	}
}
