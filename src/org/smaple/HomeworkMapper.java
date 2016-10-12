/////////////////////////////////////////////////////////////////////
//  HomeworkMapper.java - Mapper Class                             //
//                                                                 //
//  Language:      Java, Eclipse                                   //
//  Platform:      Dell, Windows 8.1                               //
//  Application:   CSE-612 , Assignment2,Spring2015                //
//  Author:		   Ankur Pandey                                    //
//  Reference:     HW Template Help                                //
/////////////////////////////////////////////////////////////////////

package org.smaple;

//----< Import Files >------------------------------------------------
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;

//----< Mapper Class  >------------------------------------------------------
public class HomeworkMapper extends Mapper<Text, Text, Text, Text> {

	// --- Map for storing Original found keys and values
	public static Map<String, ArrayList<String>> originalMap = new HashMap<String, ArrayList<String>>();
	// --- Map for Storing modified value for example by removing "1,3" 1
	public static Map<String, ArrayList<String>> modifiedMap = new HashMap<String, ArrayList<String>>();

	// ---< Writing to Reducer >-----------------------------
	public static void writingToReducer(String modKey,
			ArrayList<String> modValue, Context context,
			Map.Entry<String, ArrayList<String>> mapItr) throws IOException,
			InterruptedException {
		if (modifiedMap.containsKey(modKey)) {
			if (!(modifiedMap.get(modKey).contains(mapItr.getKey()))) {
				modifiedMap.get(modKey).add(mapItr.getKey());
				// -- Passing to reducer
				context.write(new Text(modKey), new Text(mapItr.getKey()));
			}
		} else {
			modValue.add(mapItr.getKey());
			modifiedMap.put(modKey, modValue);
			// -- Passing to reducer
			context.write(new Text(modKey), new Text(mapItr.getKey()));
		}
	}

	// ----< Map function overriding >---------------------------
	@Override
	protected void map(Text key, Text value, Context context)
			throws IOException, InterruptedException {
		String Key_ = key.toString();
		String Value_ = value.toString();

		// -- Check for self loop means key and value same
		if (Key_ != Value_) {
			// --- Calling Key and value insertion in original map
			keyAndValueInsertionInoriginalMap(Key_, Value_);
			// --- processing Map
			processingOriginalMap(context);
		}
	}

	// ----< Key and value insertion in original map >-----------------------
	public static void keyAndValueInsertionInoriginalMap(String Key,
			String Value) {
		if (!(originalMap.containsKey(Key))) {
			// ---- Adding to OriginalMap as key does not exists
			ArrayList<String> newValue = new ArrayList<String>();
			newValue.add(Value);
			originalMap.put(Key, newValue);
		} else {
			// ---- Check if that key does not have value
			if (!(originalMap.get(Key).contains(Value))) {
				if (!(originalMap.containsKey(Value))) {
					originalMap.get(Key).add(Value);
				} else {
					// ---- Check for location
					int location_ = 0;
					for (Map.Entry<String, ArrayList<String>> pair : originalMap
							.entrySet()) {
						// -- loop break as proper location for insertion found
						if (!(originalMap.get(Key).contains(pair.getKey())))
							break;
						location_++;
					}
					// --- inserting value
					originalMap.get(Key).add(location_, Value);
				}
			}
		}
	}

	// ---< processing OriginalMap >-----------------------------
	public static void processingOriginalMap(Context context)
			throws IOException, InterruptedException {
		for (Map.Entry<String, ArrayList<String>> mapItr : originalMap
				.entrySet()) {
			// --- Checking if size is greater than 0
			if (mapItr.getValue().size() > 0) {
				for (int i = 0; i < mapItr.getValue().size(); i++) {
					if (originalMap.containsKey(mapItr.getValue().get(i))
							&& originalMap.get(mapItr.getValue().get(i)).size() > 0) {
						for (int j = 0; j < originalMap.get(
								mapItr.getValue().get(i)).size(); j++) {
							// list for store newValue
							ArrayList<String> modValue = new ArrayList<String>();
							// modifiedKey
							String modKey = originalMap.get(
									mapItr.getValue().get(i)).get(j)
									+ "," + mapItr.getValue().get(i);
							// --- Calling Writing to reducer
							writingToReducer(modKey, modValue, context, mapItr);
						}
					}
				}
			}
		}
	}
}
