import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class Assignment18_2 {
public static class BulkLoadMap extends Mapper<LongWritable,Text,ImmutableBytesWritable,Put>
{
	
	
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, ImmutableBytesWritable, Put>.Context context)
					throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		//super.map(key, value, context)
		
		String   line = value.toString();
		String[] parts=line.split(",");
		
		String rowKey = "details";
		ImmutableBytesWritable HKey = new ImmutableBytesWritable(Bytes.toBytes(rowKey));
		
		Put HPut = new Put(Bytes.toBytes(rowKey));
		HPut.add(Bytes.toBytes(rowKey), Bytes.toBytes("id"), Bytes.toBytes(parts[0]));
		HPut.add(Bytes.toBytes(rowKey), Bytes.toBytes("name"), Bytes.toBytes(parts[2]));
		HPut.add(Bytes.toBytes(rowKey), Bytes.toBytes("location"), Bytes.toBytes(parts[2]));
		HPut.add(Bytes.toBytes(rowKey), Bytes.toBytes("age"), Bytes.toBytes(parts[3]));
		context.write(HKey,HPut);
		
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		Configuration conf = HBaseConfiguration.create();
		JobConf jobconf = new JobConf(conf);
		HBaseAdmin admin = new HBaseAdmin(conf);
		String inputPath = args[0];
				
		HTableDescriptor tableDescriptor = new
			      HTableDescriptor(TableName.valueOf("customer")); 	
		
		// Adding column families to table descriptor
	      tableDescriptor.addFamily(new HColumnDescriptor("details"));
	    
	   // Execute the table through admin
	      if(admin.tableExists("customer"))
	      {
	    	  admin.disableTable("customer");
	    	  System.out.println(" Existing table dropped and recreated ");
	    	  admin.deleteTable("customer");
	      }	      
	      admin.createTable(tableDescriptor);
	      System.out.println("New Table created ");
	      admin.close();
	      
	      
	      HTable hTable = new HTable(conf,"customer");   
	      //conf.set("hbase.mapred.outputtable", args[2]);
	      Job job = new Job(conf,"HBase_Bulk_loader");  
	      job.setMapOutputKeyClass(ImmutableBytesWritable.class);
	      job.setMapOutputValueClass(Put.class);
	      job.setSpeculativeExecution(false);
	      job.setReduceSpeculativeExecution(false);
	      job.setInputFormatClass(TextInputFormat.class);
	      job.setOutputFormatClass(HFileOutputFormat.class);
	      job.setJarByClass(Assignment18_2.class);
	      job.setMapperClass(Assignment18_2.BulkLoadMap.class);
	      FileInputFormat.addInputPaths(jobconf,inputPath);
	      TextOutputFormat.setOutputPath(jobconf,new Path(args[1]));
	      HFileOutputFormat.configureIncrementalLoad(job,hTable);
	      System.exit(job.waitForCompletion(true) ? 0 : 1);
	      //hTable.close();
	      
	      
	      
	      
	   }
	      
	      
	      

	}

}
