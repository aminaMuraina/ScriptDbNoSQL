//import java.io.IOException;
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.hbase.HBaseConfiguration;
//import org.apache.hadoop.hbase.HTableDescriptor;
//import org.apache.hadoop.hbase.TableName;
//public class ApacheHBase {
//
//	public static void launch(String databaseName) {
//
//		Configuration conf = HBaseConfiguration.create();
//		conf.addResource(new Path("/usr/local/Cellar/hbase/1.1.5_1/libexec/conf/hbase-site.xml"));
//		//System.setProperty("hadoop.home.dir", "/usr/local/Cellar/hadoop/2.7.2");
//
//
//		HBaseAdmin admin=null;
//		try {
//			admin = new HBaseAdmin(conf);
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		
//		HTableDescriptor table = new HTableDescriptor(TableName.valueOf("TablenameAmina"));
//		try {
//			admin.createTable(table);
//		} catch (IOException e) {
//			// TODO Auto-generated catch block			
//		}
//
//		try {
//			admin.close();
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//		}
//
////		//creating column family descriptor
////		HColumnDescriptor family = new HColumnDescriptor(toBytes("column family"));
////
////		//adding coloumn family to HTable
////		table.addFamily(family);
//	}
//	
////	public static boolean createTable(Admin admin, HTableDescriptor table, byte[][] splits)
////			throws IOException {
////			  try {
////			    admin.createTable( table, splits );
////			    return true;
////			  } catch (TableExistsException e) {
////			    logger.info("table " + table.getNameAsString() + " already exists");
////			    // the table already exists...
////			    return false;
////			  }
////			}
//}