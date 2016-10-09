import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

public class MySQL {

	static SimpleDateFormat dateformat = new SimpleDateFormat("yyyy-MM-dd");

	private static Connection connect = null;
	private static Statement statement = null;
	private static ResultSet resultSet = null;
	static Random rand = new Random();

	static final String tableNameImmobili = "immobili";
	static final String tableNameClienti = "clienti";
	static final String tableNameProposteDiVendita = "proposteDiAcquisto";

	static final String DB_URL = "jdbc:mysql://localhost:3306/";

	public static void launch(String databaseName) {
		// Connessione al server di MySQL
		serverConnection();
		// Result set get the result of the SQL query

		// Delete database/schema
		long startTime = System.nanoTime();
		dropDatabase(databaseName);
		long endTime = System.nanoTime();
		System.out.println("Cancellazione database/schema vuoto: " + ((endTime - startTime) / 1000000 / 1000) % 60
				+ " secondi, " + (endTime - startTime) / 1000000 + " millisecondi");

		// Create Database/schema
		startTime = System.nanoTime();
		createDatabase(databaseName);
		endTime = System.nanoTime();
		System.out.println("Creazione database/schema vuoto: " + ((endTime - startTime) / 1000000 / 1000) % 60
				+ " secondi, " + (endTime - startTime) / 1000000 + " millisecondi");

		// Create Tables
		startTime = System.nanoTime();
		createTableImmobili(databaseName);
		createTableClienti(databaseName);
		createTableProposteDiVendita(databaseName);
		endTime = System.nanoTime();
		System.out.println(
				"Creazione tabelle (immobili, clienti e proposte): " + ((endTime - startTime) / 1000000 / 1000) % 60
						+ " secondi, " + (endTime - startTime) / 1000000 + " millisecondi");

		// insert data into tables
		startTime = System.nanoTime();
		insertIntoTableImmobili(databaseName, 100000);
		insertIntoClienti(databaseName, 100000);
		insertIntoProposteDiVendita(databaseName, 100000);
		endTime = System.nanoTime();
		System.out.println("Inserimento di 100.000 righe per ogni tabella (immobili, clienti e proproste): "
				+ ((endTime - startTime) / 1000000 / 1000) % 60 + " secondi, " + (endTime - startTime) / 1000000
				+ " millisecondi");

		// Read all row in table
		startTime = System.nanoTime();
		readData(tableNameImmobili, databaseName);
		endTime = System.nanoTime();
		System.out
				.println("Lettura delle righe nella tabella immobili: " + ((endTime - startTime) / 1000000 / 1000) % 60
						+ " secondi, " + (endTime - startTime) / 1000000 + " millisecondi");

		// Update row in table
		startTime = System.nanoTime();
		updateARowInClienti(databaseName);
		endTime = System.nanoTime();
		System.out.println(
				"Aggiornamento di una riga nella tabella clienti: " + ((endTime - startTime) / 1000000 / 1000) % 60
						+ " secondi, " + (endTime - startTime) / 1000000 + " millisecondi");

		// Delete row in table
		startTime = System.nanoTime();
		deleteARowInClienti(databaseName);
		endTime = System.nanoTime();
		System.out
				.println("Rimozione di una riga nella tabella clienti: " + ((endTime - startTime) / 1000000 / 1000) % 60
						+ " secondi, " + (endTime - startTime) / 1000000 + " millisecondi");

		// Delete tables
		startTime = System.nanoTime();
		deleteTable(databaseName, tableNameImmobili);
		deleteTable(databaseName, tableNameClienti);
		deleteTable(databaseName, tableNameProposteDiVendita);
		endTime = System.nanoTime();
		System.out.println(
				"Cancellazione delle tabelle (immobili e clienti):" + ((endTime - startTime) / 1000000 / 1000) % 60
						+ " secondi, " + (endTime - startTime) / 1000000 + " millisecondi");

		// delete database/Schema
		// startTime = System.nanoTime();
		// dropDatabse(databaseName);
		// endTime = System.nanoTime();
		// System.out.println("Cancellazione keyspace con 200000 righe: " +
		// ((endTime - startTime) / 1000000 / 1000) % 60
		// + " secondi, " + (endTime - startTime) / 1000000 + " millisecondi");

		// Close all connection
		closeConnection();

	}

	public static void launchInsert(String databaseName) {
		// Connessione al server di MySQL
		serverConnection();

		// Delete database/schema
		dropDatabase(databaseName);

		// Create Database/schema
		createDatabase(databaseName);

		// Create Tables
		createTableImmobili(databaseName);
		createTableClienti(databaseName);
		createTableProposteDiVendita(databaseName);

		// insert data into tables
		int max = 0;
		for (int i = 0; max < 21; i = i + 5000) {
			max = max + 1;

			insertIntoProposteDiVendita(databaseName, i);
			insertIntoClienti(databaseName, i);
			insertIntoTableImmobili(databaseName, i);
			try {
				long startTime = System.nanoTime();
				statement.executeUpdate("use testDBNoSQL;");
				statement.executeQuery("select count(*) as numproposta,im.Indirizzo,cl.Cognome "
						+ "from proposteDiAcquisto as pa, Immobili as im ,Clienti as cl "
						+ "where  pa.id_immobile = im.id_Immobile " + "and pa.codiceFiscale = cl.CodiceFiscale "
						+ "GROUP BY im.indirizzo , cl.cognome;");
				long endTime = System.nanoTime();
				System.out.println(
						"query con  " + i + " righe in ogni tabella : " + ((endTime - startTime) / 1000000 / 1000) % 60
								+ " secondi, " + (endTime - startTime) / 1000000 + " millisecondi");

				readData(tableNameClienti, databaseName);

			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		closeConnection();
	}

	private static void deleteTable(String databaseName, String tableName) {
		try {
			statement.executeUpdate("Use " + databaseName);
			statement.executeUpdate("DROP TABLE " + tableName + ";");

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private static void deleteARowInClienti(String databaseName) {
		try {
			statement.executeUpdate("Use " + databaseName);
			statement.executeUpdate("DELETE FROM  " + tableNameClienti + " WHERE codiceFiscale='MRNCRAASwetavio1' ;");

		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	private static void updateARowInClienti(String databaseName) {
		try {
			statement.executeUpdate("Use " + databaseName);
			statement.executeUpdate(
					"UPDATE " + tableNameClienti + " SET nome= 'amina' WHERE codiceFiscale='MRNCRAASwetavio1' ;");

		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	private static void insertIntoProposteDiVendita(String databaseName, int numLen) {
		try {
			for (int i = 0; i < numLen; i++) {

				statement.executeUpdate("Use " + databaseName);

				long time = System.currentTimeMillis();
				java.sql.Date date = new java.sql.Date(time);

				statement.executeUpdate("INSERT INTO " + tableNameProposteDiVendita
						+ " ( data, prezzo, ggValidita, id_Immobile, codiceFiscale) " + "VALUES( '" + date + "', "
						+ 12.05 + "," + i + "," + i + ", " + i + "); ");
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	private static void insertIntoClienti(String databaseName, int numLen) {
		try {
			for (int i = 0; i < numLen; i++) {

				statement.executeUpdate("Use " + databaseName);

				long time = System.currentTimeMillis();
				java.sql.Date date = new java.sql.Date(time);

				statement.executeUpdate("INSERT INTO " + tableNameClienti + " ("
				// + "codiceFiscale,"
						+ " cognome, nome, datadinascita, indirizzoDiResidenza) " + "VALUES("
						// + " 'SKEISDNS" + new Date() +
						// rand.nextInt(200000000)+ "',"
						+ " 'Mario','Rossi','" + date + "', 'via pascoli 3'); ");
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	private static void insertIntoTableImmobili(String databaseName, int numLen) {
		try {
			for (int i = 0; i < numLen; i++) {

				statement.executeUpdate("Use " + databaseName);

				long time = System.currentTimeMillis();
				java.sql.Date date = new java.sql.Date(time);

				statement.executeUpdate(
						"INSERT INTO " + tableNameImmobili + " ( metratura, prezzo, dataDiVendita,indirizzo) "
								+ "VALUES(" + 200 + "," + 12.90 + ", '" + date + "', 'via pascoli 3'); ");
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private static void createTableProposteDiVendita(String databaseName) {
		try {
			statement.executeUpdate("Use " + databaseName);
			statement.executeUpdate("CREATE TABLE " + tableNameProposteDiVendita
					+ " ( data date, prezzo float, ggValidita integer, id_Immobile int references " + tableNameImmobili
					+ " (id_Immobile), codiceFiscale int references " + tableNameClienti + " (codiceFiscale));");

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private static void createTableClienti(String databaseName) {
		try {
			statement.executeUpdate("Use " + databaseName);
			statement.executeUpdate("CREATE TABLE " + tableNameClienti
			// + " ( codiceFiscale varchar(200) PRIMARY KEY, "
					+ " ( codiceFiscale int NOT NULL  AUTO_INCREMENT, "
					+ "nome varchar(255), cognome varchar(255), indirizzoDiResidenza varchar(255), dataDiNascita date, PRIMARY KEY(codiceFiscale));");

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private static void createTableImmobili(String databaseName) {
		try {

			statement.executeUpdate("Use " + databaseName);
			statement.executeUpdate("CREATE TABLE " + tableNameImmobili
					+ " ( id_immobile integer NOT NULL  AUTO_INCREMENT, indirizzo varchar(255),prezzo float, metratura int, dataDiVendita date, PRIMARY KEY(id_immobile));");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private static void createDatabase(String databaseName) {

		try {
			statement.executeUpdate("CREATE DATABASE IF NOT EXISTS " + databaseName);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private static void dropDatabase(String databaseName) {
		try {
			statement.executeUpdate("DROP DATABASE IF EXISTS " + databaseName);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private static void serverConnection() {
		try {
			// STEP 2: Register JDBC driver
			Class.forName("com.mysql.jdbc.Driver");

			// STEP 3: Open a connection
			System.out.println("Connecting to database...");
			connect = DriverManager.getConnection(DB_URL, "root", "scoprila");

			// STEP 4: Execute a query
			System.out.println("Creating database...");
			statement = connect.createStatement();
			// Statements allow to issue SQL queries to the database
		} catch (Exception e) {
			System.out.println("Error during server connectionnnn " + e);
		}
	}

	private static void readData(String tableName, String databaseName) {
		try {
			statement.executeUpdate("Use " + databaseName);
			resultSet = statement.executeQuery("Select * from " + tableName + ";");

			int r = 0;
			// ResultSet is initially before the first data set
			while (resultSet.next()) {
				r = r + 1;
			}
			System.out.println("--- FINEeeessss" + r);

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private static void closeConnection() {
		try {
			if (resultSet != null) {
				resultSet.close();
			}

			if (statement != null) {
				statement.close();
			}

			if (connect != null) {
				connect.close();
			}
		} catch (Exception e) {

		}
	}

}