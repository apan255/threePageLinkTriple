

package org.smaple;

//----< Import Files >------------------------------------------------
import java.io.IOException;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;

//----< Reducer Class  >------------------------------------------
public class HomeworkReducer extends Reducer<Text, Text, Text, Text> {
	// ----< Processing Values that has been received from mapper >---------
	public static String processingValues(Text key, Iterable<Text> values) {
		String modifiedValue = "";
		for (Text text_ : values) {
			if ((text_.toString().contains(","))) {
				return null;
			} else {
				if (modifiedValue.length() == 0) {
					if (!(key.toString().contains(text_.toString())))
						modifiedValue = text_.toString();
				} else {
					if (!(key.toString().contains(text_.toString())))
						modifiedValue = modifiedValue + "," + text_.toString();
				}
			}
		}
		return modifiedValue;
	}

	// ----< Over Riding Reduce Function >-----------------------------------
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		if (key.toString().contains(",")) {
			String value = processingValues(key, values);
			if (value != null) {
				if (value.length() > 0) {
					// --- Writing to output File given by command
					context.write(key, new Text(value));
				}
			} else {
				return;
			}
		}
	}
}
