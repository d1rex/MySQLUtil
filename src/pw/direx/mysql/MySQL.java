package pw.direx.mysql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import com.mysql.jdbc.Driver;
import com.mysql.jdbc.PreparedStatement;

/**
 * 
 * @author DirexArtworks
 *
 */
public class MySQL {
	
	/**
	 * Lokale Variablen, welche für den Verbindungsaufbau zur MySQL-Datenbank benötigt werden.
	 * Diese Variablen werden in den Konstruktoren definiert.
	 */
	private String hostAdress, username, password, database;
	private Integer port;
	private boolean autoReconnect;
	private Connection connection;
	/**
	 * Ende der Variablen
	 */
	
	/**
	 * Ein simpler Konstruktor, welcher als Port den MySQL-Standard übergibt und automatisch die Verbindung neu herstellen wird,
	 * wenn sie abbricht. Geeignet für Einsteiger.
	 * 
	 * @param hostAdress
	 * @param username
	 * @param password
	 * @param database
	 */
	public MySQL(String hostAdress, String username, String password, String database)
	{
		/**
		 * Verweis auf den komplexen Konstruktor, allerdings mit prädefinierten Werten.
		 */
		this(hostAdress, 3306, username, password, database, true);
	}
	
	/**
	 * Ein komplexerer Konstruktor, bei welchem man alle Variablen selbst definieren muss. Zwar für den Durchschnittsnutzer nicht wirklich
	 * schwerer zu verstehen, jedoch ist es sinnvoll 2 Konstruktoren anzulegen, weil der Code im Nachhinein aufgeräumter wirkt.
	 * Ebenso wird der sog. "driver" initialisiert, damit der DriverManager überhaupt weiß, was er machen soll.
	 * 
	 * @param hostAdress
	 * @param port
	 * @param username
	 * @param password
	 * @param database
	 * @param autoReconnect
	 */
	public MySQL(String hostAdress, Integer port, String username, String password, String database, boolean autoReconnect)
	{
		this.hostAdress = hostAdress;
		this.port = port;
		this.username = username;
		this.password = password;
		this.database = database;
		this.autoReconnect = autoReconnect;
		
		/**
		 * Den "driver" initialisieren
		 */
		try 
		{
			DriverManager.registerDriver(new Driver());
			Runtime.getRuntime().addShutdownHook(new Thread(this::disconnect));
		} 
		catch (Exception e)
		{
			/**
			 * Fehlerausgabe, wenn A) der "driver" nicht initialisiert werden konnte / B) die "shutdown hook" nicht registriert werden konnte und anschließend beenden.
			 */
			e.printStackTrace();
			System.exit(-1);
		}
	}
	
	/**
	 * Die Methode zum Herstellen der Verbindung. Sollte es einen Fehler geben, so wird dieser in der Konsole ausgegeben und das Programm
	 * schließt sich mit dem sog. "exit code" -1, was im Allgemeinen dafür steht, dass das Programm aufgrund eines Fehlers beendet wurde.
	 */
	public void connect()
	{
		try
		{
			/**
			 * Abfragen, ob die Verbindung nicht bereits hergestellt ist.
			 * Wenn die Verbindung nicht besteht -> verbinden und das Connection-Objekt in der Klasse erneuern.
			 */
			if(this.getConnection() == null)
			{
				/**
				 * Verbinden und erneuern.
				 */
				this.connection = DriverManager.getConnection("jdbc:mysql://" + this.hostAdress + ":" + this.port + "/" + this.database + "?autoReconnect=" + this.autoReconnect, this.username, this.password);
				System.out.println("[MySQL] Connection established.");
				return;
			}
			else
			{
				/**
				 * Wenn die Verbindung nun also besteht -> Verbindungsversuch durch ein sog. "return statement" abbrechen.
				 * Da der sog. "return type" der Methode "connect()" void ist, brauchen wir beim "return statement" kein Argument.
				 * Für ein Gegenbeispiel siehe "getConnection()"
				 */
				System.out.println("[MySQL] Already connected. Aborting...");
				return;
			}
		}
		catch (Exception e) 
		{
			/**
			 * Fehlerausgabe, wenn es einen Fehler gab und anschließend beenden.
			 */
			e.printStackTrace();
			System.exit(-1);
		}
	}
	
	/**
	 * Diese Methode wird dafür genutzt, die Verbindung ordentlich zu beenden.
	 * Sie wird im Konstruktor bereits als eine sog. "shutdown hook" registriert, das heißt, dass die Methode
	 * automatisch beim Schließen der Anwendung ausgeführt wird.
	 */
	private void disconnect()
	{
		try
		{
			/**
			 * Abfragen, ob jemals eine Verbindung hergestellt wurde.
			 */
			if(this.getConnection() != null)
			{
				/**
				 * Wenn dies so ist -> Abfragen, ob der MySQL-Server die Verbindung nicht bereits abgebrochen hat.
				 */
				if(this.getConnection().isClosed())
				{
					/**
					 * Verbindung besteht nicht mehr
					 */
					System.out.println("[MySQL] There is no matter of disconnecting as the connection is already closed.");
					return;
				}
				else
				{
					/**
					 * Verbindung besteht noch -> Verbindung beenden und die Definition des Connection-Objekts zurücksetzen
					 */
					System.out.println("[MySQL] Closing connection...");
					this.getConnection().close();
					this.connection = null;
					System.out.println("[MySQL] Connection closed.");
				}
			}
			else
			{
				/**
				 * Verbindung bestand nie
				 */
				System.out.println("[MySQL] There is no matter of disconnecting as no connection was ever established.");
				return;
			}
		}
		catch (Exception e) 
		{
			/**
			 * Fehlerausgabe, wenn es einen Fehler gab und anschließend beenden.
			 */
			e.printStackTrace();
			System.exit(-1);
		}
	}
	
	/**
	 * Diese Methode wird dafür genutzt, die Verbindung neu herzustellen. Ich habe keine Ahnung wann dies sinnvoll sein könnte, da man eigentlich
	 * recht kurz verbunden sein sollte (ca. 5-6h maximal), aber falls jemand plant eine serverähnliche Anwendung zu erstellen, die dann mehrere Tage verbunden
	 * sein könnte, halte ich es für sinnvoll, diese Methode einzubinden. 
	 */
	public void reconnect()
	{
		/**
		 * Einfach zuerst die Verbindung schließen. Da wir alle Fehlerquellen umgangen haben sollte dies reibungslos laufen
		 * Anschließend einfach erneut verbinden
		 */
		try 
		{
			this.disconnect();
			/**
			 * Kurze Verzögerung vor dem erneuten Verbindungsaufbau (1 Sekunde = 1000 Millisekunden)
			 */
			Thread.sleep(1000);
			this.connect();
		}
		catch (Exception e) 
		{
			/**
			 * Fehlerausgabe, wenn es komischerweise einen Fehler gab und anschließend beenden.
			 */
			e.printStackTrace();
			System.exit(-1);
		}
	}
	
	/**
	 * Diese Methode wird dafür genutzt, der Datenbank einen Befehl zu geben. Die sqlQuery entspricht hierbei dem Befehl für den Server.
	 * Anwendungsbeispiel: Erstellen einer neuen Tabelle
	 * @param sqlQuery
	 */
	public void executeUpdate(String sqlQuery)
	{
		try
		{
			/**
			 * Dem Server den Befehl mitteilen
			 */
			this.getConnection().prepareStatement(sqlQuery).executeUpdate();
		}
		catch (Exception e) 
		{
			/**
			 * Fehlerausgabe, wenn es einen Fehler gab, jedoch nicht beenden, da dies bei weitem kein kritischer Fehler ist.
			 */
			e.printStackTrace();
		}
	}
	
	/**
	 * Dies ist eine simple Methode für das erhalten eines sog. "result set", welches die für die Anfrage gefundenen Werte enthält. Sollte
	 * es keine auf die Anfrage passenden Werte geben, so wird ein leeres "result set" zurückgegeben. Der Nutzer dieser API (advanced programming
	 * interface = erweiterte Programmier-Schnittstelle) kann mit resultSet.next() abfragen, ob ein "result set" Werte enthält.
	 * @param sqlQuery
	 */
	public ResultSet executeQuery(String sqlQuery)
	{
		try
		{
			/**
			 * Dem Server den Befehl mitteilen, es wird automatisch auf die Antwort gewartet und sobald sie erhalten wurde wird das erhaltene
			 * "result set" zurückgegeben.
			 */
			return this.getConnection().prepareStatement(sqlQuery).executeQuery();
		}
		catch (Exception e) 
		{
			/**
			 * Fehlerausgabe, wenn es einen Fehler gab, jedoch nicht beenden, da dies bei weitem kein kritischer Fehler ist und abschließend
			 * nichts (null) zurückgeben, da wir nichts von der Datenbank erhalten haben.
			 */
			e.printStackTrace();
			return null;
		}
	}
	
	/**
	 * Dies ist die für jeden Anwender empfohlene Methode, da die Fehlerquote bezüglich dem Formulieren der sqlQuery deutlich gesenkt wird,
	 * da man einem PreparedStatement zunächst Platzhalter zu weisen kann, welche man anschließend ersetzt.
	 * 
	 * Beispiel:
	 * 
	 *		PreparedStatement preparedStatement = this.getConnection.prepareStatement("SELECT * FROM Mitarbeiter WHERE Vorname=?");
	 *		dann das ? durch "Tobias" ersetzen mit:
	 *		preparedStatement.setString(1, "Tobias")
	 *		und abschließend also:
	 *		executeQuery(preparedStatement);
	 *
	 * Gegenbeispiel:
	 * 
	 *		executeQuery("SELECT * FROM Mitarbeiter WHERE Name='Tobias'");
	 *
	 * Der Vorteil wird deutlicher, wenn man am Ende der Query mehrere Bedinungen übergibt, neben dem Vornamen also den Nachnamen.
	 *
	 * @param sqlQuery
	 */
	public ResultSet executeQuery(PreparedStatement preparedStatement)
	{
		try
		{
			/**
			 * Dem Server den Befehl mitteilen, es wird automatisch auf die Antwort gewartet und sobald sie erhalten wurde wird das erhaltene
			 * "result set" zurückgegeben.
			 */
			return preparedStatement.executeQuery();
		}
		catch (Exception e) 
		{
			/**
			 * Fehlerausgabe, wenn es einen Fehler gab, jedoch nicht beenden, da dies bei weitem kein kritischer Fehler ist und abschließend
			 * nichts (null) zurückgeben, da wir nichts von der Datenbank erhalten haben.
			 */
			e.printStackTrace();
			return null;
		}
	}
	
	/*
	 * Ab hier befinden sich nur noch sog. "getter", welche für den Codestyle relevant sind, um ein hässliches
	 * 
	 * this.connection
	 * 
	 * durch ein übersichtliches
	 * 
	 * this.getConnection()
	 * 
	 * ersetzen zu können.
	 */
	
	public Connection getConnection() 
	{
		/**
		 * Der "return type" der Methode ist Connection, also müssen wir dem "return statement" eine Instanz eines Connection-Objekts geben.
		 * In diesem Fall die Connection, welche oben in der Klasse definiert ist.
		 */
		return connection;
	}
	
	public String getUsername() 
	{
		return username;
	}
	
	public String getDatabase() 
	{
		return database;
	}
	
	public String getHostAdress()
	{
		return hostAdress;
	}
	
	public Integer getPort() 
	{
		return port;
	}
	
	public boolean isAutoReconnect()
	{
		return autoReconnect;
	}

}
