
public class StartUp {

	public static void main(String[] args) {

		final String databaseName = "testDBNoSQL";

		// MongoDB.launch(databaseName);
		// MongoDB.launchInsert(databaseName);
		// MongoDB.QueryTimeLettura(databaseName);
		// System.out.println("FINE - MongoDB ");

		ApacheCassandra.launch(databaseName);
		// ApacheCassandra.launchInsert(databaseName);
		 //ApacheCassandra.QueryTimeLettura(databaseName);

		 System.out.println("FINE - ApacheCassandra ");

		// MySQL.launch(databaseName);
		//MySQL.launchInsert(databaseName);
		// MySQL.QueryTimeLettura(databaseName);

		//System.out.println("FINE - MYsql ");
	}
}
