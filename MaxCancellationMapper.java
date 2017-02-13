

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

	public class MaxCancellationMapper extends
	    Mapper<LongWritable, Text, Text, Text> {
	  public void map(LongWritable key, Text value, Context context)
	      throws IOException, InterruptedException {
	 try{
		String line=value.toString();
		//Excluding the header line form csv file
		if(!line.startsWith("Year"))
		{
		
	    String[] completeFlightInfo=line.split(","); 
	   
		String carrier=completeFlightInfo[8];

		int isCancelled=Integer.parseInt(completeFlightInfo[21]);
		//Checking and getting the values of a cancelled flight
			if(isCancelled==1)
			{
				String origin=completeFlightInfo[16];
				String destination=completeFlightInfo[17];
				String orgDest=origin+","+destination;
				context.write(new Text(carrier), new Text(orgDest));
			}
		}
	 }
	   catch(NumberFormatException ne){
		   System.out.println("Exception in MaxCancellationMapper"+ne);
	   }
		
	}
	
}
	   
	   
	   
	
	
	
	   
	     
	  
	  


