import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

	public class MaxCancellationReducer  extends Reducer<Text, Text, Text, Text> {
	 
	public void reduce(Text carrier, Iterable<Text> orgDest, Context context)
	      throws IOException, InterruptedException{
		  
		  Map<Text,Integer> orgDestCancellations = new HashMap<Text,Integer>();
		  String maxorgDestCancellations = null;
		  Text keyWithMaxValue = null;
	
		 // Adding the entries to the orgDestCancellations map with the origin,destination as key and number of cancellations as value
		  while(orgDest.iterator().hasNext()) {
			Text  originDestinations = orgDest.iterator().next(); 
			Text orgDestMapKey = new Text(originDestinations.toString()); 
			  if(!orgDestCancellations.isEmpty() && orgDestCancellations.containsKey(originDestinations))
			  {
				  int count=(Integer)orgDestCancellations.get(originDestinations);
				  orgDestCancellations.put(orgDestMapKey,count+1);
			  }
			  else
			  {
				  orgDestCancellations.put(orgDestMapKey, 1);
			  }
		  }
		  //Getting the maximum value from the orgDestCancellations map
		  	int maxValueInTheMap=(Collections.max(orgDestCancellations.values()));
		  //Getting the key with maximum value from the orgDestCancellations map
		  for(Entry<Text, Integer> entry:orgDestCancellations.entrySet()) {
			  if((entry.getValue()) == maxValueInTheMap)
			  {
				 keyWithMaxValue = entry.getKey();  
				 maxorgDestCancellations = keyWithMaxValue +"		" + maxValueInTheMap;
				 break;
			  }
		  }
		  context.write(carrier,new Text(maxorgDestCancellations));  
	  }
	}



