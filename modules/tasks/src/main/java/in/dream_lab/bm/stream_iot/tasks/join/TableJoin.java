package in.dream_lab.bm.stream_iot.tasks.join;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

import org.slf4j.Logger;

import in.dream_lab.bm.stream_iot.tasks.AbstractTask;

/**
 * @author shilpa
 *
 */
public class TableJoin extends AbstractTask<String, String> {
	private static final Object SETUP_LOCK = new Object();
	private static boolean doneSetup = false;
	private HashMap<Long, String> itemIdMap;

	@Override
	public void setup(Logger l_, Properties p_) {
		super.setup(l_, p_);
		synchronized (SETUP_LOCK) {
			if (!doneSetup) {
				doneSetup = true;
			}
			itemIdMap = new HashMap<Long, String>();
		}

	}

	@Override
	protected Float doTaskLogic(Map<String, String> map) {
		// reading the meta and obsvalue from string
		String in = (String) map.get(AbstractTask.DEFAULT_KEY);
		List<String> list = new ArrayList<String>(Arrays.asList(in.split(",")));
		String itemId = list.get(0);
		System.out.println("Table Join itemId: " + itemId);

		String tmp;

		Long itemIdLong = Long.parseLong(itemId);
		if (itemIdMap.containsKey(itemIdLong) == true) {
			StringBuilder joinedValues = new StringBuilder();
			for (String s : list) {
				joinedValues.append(s + ',');
			}
			tmp = itemIdMap.get(itemIdLong).substring(2);
			StringBuilder newValue = joinedValues.append(tmp);
			joinedValues = newValue.deleteCharAt(joinedValues.length() - 1);
			itemIdMap.remove(itemIdLong);
			tmp = newValue.toString().substring(2);
			setLastResult(tmp);
		} else {
			StringBuilder joinedValues = new StringBuilder();
			for (String s : list) {
				joinedValues.append(s + ',');
			}
			itemIdMap.put(itemIdLong, joinedValues.toString());
			joinedValues = joinedValues.deleteCharAt(joinedValues.length() - 1);
			tmp = joinedValues.toString().substring(2);
		}

		return 1.0f;
	}

}
