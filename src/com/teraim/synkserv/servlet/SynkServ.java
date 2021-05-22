package com.teraim.synkserv.servlet;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.logging.Logger;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.teraim.fieldapp.synchronization.EndOfStream;
import com.teraim.fieldapp.synchronization.SyncFailed;

/**
 * Servlet implementation class SynkServ
 */
@WebServlet(asyncSupported = true, urlPatterns = { "/SynkServ" })

public class SynkServ extends HttpServlet {
	@Override
	public void destroy() {
		//executor.shutdown();
		super.destroy();
	}

	//Configures max rows that will be returned. Should be the same number in the clients. 
	private static final int MAX_ROWS_TO_RETURN = 10;
	private static final String VER = "44";
	private static final long serialVersionUID = 1L;
	public static final Object TRANS_ACK = "TRANSACK";
	//private ExecutorService executor;

	/**
	 * @see HttpServlet#HttpServlet()
	 */
	public SynkServ() {
		super();

	}


	public void init(){

		System.out.println("in init...version "+VER);
		try {
			Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
		} catch (ClassNotFoundException e) {
			logger.severe("did not find driver");
			e.printStackTrace();
		}


	}



	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {

		response.setContentType("application/json");
		//response.setContentType("text/plain");
		//response.getWriter().append("Served at: ").append(request.getContextPath()).append("\n").append(request.getParameter("action"));
		String action = request.getParameter("action");


		//action denotes the query
		if (action!=null) {
			switch (action) {
			case "get_team_status":
				String result ="{}";
				String GROUP = request.getParameter("team");
				String USERUUID = request.getParameter("useruuid");
				String APP = request.getParameter("project");
				String FROM_THIS_TO_CURRENT = request.getParameter("timestamp");
				//System.out.println("Pars: timestamp "+FROM_THIS_TO_CURRENT+" project "+APP+" team "+GROUP);
				if (chk(GROUP)&chk(APP)) {
					//String last_check = chkIsTime(FROM_THIS_TO_CURRENT);
					if (FROM_THIS_TO_CURRENT!=null) {
						try { 
							connectDatabase();					
							if (con!=null) {

								Timestamp last_check = new Timestamp(Long.parseLong(FROM_THIS_TO_CURRENT));
								//			System.out.println("timestamp "+last_check+" project "+APP+" team "+GROUP);
								final Statement stmt = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);
								ResultSet res = stmt.executeQuery(
										"SELECT [USER],COUNT(*) as antal,MAX(TIMEOFINSERT) AS latest FROM " +
												" (SELECT [USER],USERUUID,TIMEOFINSERT, SYNCGROUP, APP FROM audit "+
												" WHERE SYNCGROUP='"+GROUP+"' AND APP = '"+APP+"' AND USERUUID <> '"+USERUUID+"' AND TIMEOFINSERT > '"+last_check+"' )"+
												" as d group by [USER]"
										);
								StringBuilder sb = new StringBuilder();
								sb.append("{");
								int i=0;
								//pack result into json.
								while (res.next()) {
									sb.append("\"USER").append(i++).append("\":");
									sb.append("[");
									sb.append("\"").append(res.getString("USER")).append("\"");
									sb.append(",");
									sb.append(res.getString("antal"));
									sb.append(",");
									sb.append("\"").append(res.getTimestamp("latest").getTime()).append("\"");
									sb.append("]");
									sb.append(",");
								}
								//remove last comma
								if (sb.length()>1)
									sb.deleteCharAt(sb.length()-1);
								sb.append("}");
								//			System.out.println("Result: "+sb);
								result = sb.toString();

							} else
								System.err.println("conn null");
						} catch(SQLException e) {
							e.printStackTrace();
							System.err.println("sql exception");
						} catch(NumberFormatException e) {
							e.printStackTrace();
							System.err.println("not a number: "+FROM_THIS_TO_CURRENT);
						}
					} else
						System.err.println("time par null");											
				} else
					System.err.println("Failed parameter chk");
				response.getWriter().println(result);
				break;
			default:
				;
			}
		}

	}


	private boolean chk(String s) {
		return s!=null && s.length()>0 && s.length()<100;
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	private Connection con = null;


	final static Logger logger = Logger.getLogger("logger");

	private void connectDatabase() {
		con=null;

		try {
			///Class.forName("org.postgresql.Driver");
			Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
			//String dbName = System.getProperty("RDS_DB_NAME");
			//String userName = System.getProperty("RDS_USERNAME");
			//String password = System.getProperty("RDS_PASSWORD");
			String userName = "kalle";
			String password = "AbraKadabra!1";
			//Hardcode DB name for now to avoid having to replace beanstalk env. dns.
			String hostname = "ebdb.cljwr0n66av2.eu-west-1.rds.amazonaws.com"; //System.getProperty("RDS_HOSTNAME");
			//String goofer = System.getProperty("Goofer");
			String port = "1433";
			//String jdbcUrl ="jdbc:sqlserver://ultdbv2-5.slu.se;instanceName=ultinst5;databaseName=Rlo_prod;user=fieldappklient;password=z5_A*9Iu2";
			String jdbcUrl = "jdbc:sqlserver://" + hostname + ":" + port + ";databaseName=Rlo_prod;user=" + userName + ";password=" + password;
			//				logger.severe("connection to database with "+jdbcUrl);
			con = DriverManager.getConnection(jdbcUrl);
			//				logger.warning("Remote connection successful.");
			//System.out.println("username password hostname port goofer: "+userName+" "+password+" "+hostname+" "+port+" "+goofer);
			//System.out.println("Connection is null? "+con==null);
		}
		catch (ClassNotFoundException e) { logger.warning(e.toString());}
		catch (SQLException e) { logger.warning(e.toString());}
		//} else
		//	logger.severe("RDS HOSTNAME WAS NULL");

		if (con==null)
			logger.severe("Failed to get connection to database");

	}


	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {

		
		Timestamp 			currTime 		= new java.sql.Timestamp(System.currentTimeMillis());
		ObjectOutputStream 	out=null;
		ObjectInputStream  	in=null;
		String errLog=null, errDetails = null;

		try {

				connectDatabase();
				
				if (con==null) {
					writeLog("no connection to database");
					return;
				}
				in 	= new ObjectInputStream(request.getInputStream());
				final String GROUP = (String)in.readObject();
				final String USER = (String)in.readObject();		
				final String USERUUID = (String)in.readObject();
				final String APP = (String)in.readObject();	
				errDetails = "team: "+GROUP+" user: "+USER+" UUID: "+USERUUID+" APP: "+APP;
				
				//insert
				
				Object DATA = in.readObject();
				if (!(DATA instanceof EndOfStream))
					errLog = insertData(DATA,
							GROUP,
							APP,
							USER,
							USERUUID, 
							currTime);
							
				//transmit
				
				//Fetch start timestamp.
				Long   startTime = (Long)in.readObject();
				out	= new ObjectOutputStream(response.getOutputStream());
				errLog = sendData(startTime,
							MAX_ROWS_TO_RETURN, 
							out, 
							GROUP, 
							APP, 
							USER, 
							USERUUID, 
							currTime);
				

		} catch (SQLException e) {
			errLog = "SQL EXCEPTION";		
			e.printStackTrace();
		} catch (IOException e) {
			errLog = "IO EXCEPTION in write";
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			errLog = "ClassNotFound Exception in ReadObject";
			e.printStackTrace();
		} finally {
			try {
				if (in!=null)
					in.close();
				if (out !=null)
					out.close();
				if (con != null)
					con.close();
			} catch (Exception e) {
				writeLog(e.getMessage());
			}
		} 
		
			
		if (errLog != null) {
			if (errDetails != null)
				writeLog(errDetails);
			writeLog(errLog);
			out.writeObject(new SyncFailed(errLog));
		}
		
	}


	private String insertData(Object DATA, String GROUP, String APP, String USER, String USERUUID, Timestamp currTime) throws SQLException {

		PreparedStatement statement = con.prepareStatement("INSERT INTO audit (SYNCGROUP, [USER], USERUUID, APP, TIMEOFINSERT, SYNCOBJECTS) VALUES (?,?,?,?,?,?)");
		statement.setString(1, GROUP);
		statement.setString(2, USER);
		statement.setString(3, USERUUID);
		statement.setString(4, APP);
		statement.setTimestamp(5, currTime);
		byte[] bytes = objToByte(DATA);
		if (bytes==null) {
			return "Couldn't create byte array of object";
		}
		Blob blob = new javax.sql.rowset.serial.SerialBlob(bytes);
		statement.setBlob(6, blob);
		statement.execute();
		return null;
	}



	//send data requested
	//return error message if any
	private String sendData(long startTime,  int rowsToReturn, ObjectOutputStream out, String GROUP, String APP, String USER, String USERUUID, Timestamp currTime) throws SQLException, IOException {

		String errLog = null;
		//check if any objects are available for this client.
		//Number of rows with max 1000 sync objects that will be returned.  
		//Statement stmt = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,MAX_ROWS_TO_RETURN);
		Statement stmt = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);
		//ResultSet res = stmt.executeQuery("select TIMEOFINSERT, SYNCOBJECTS FROM audit WHERE"
		//			+" SYNCGROUP = '"+GROUP
		Timestamp timeForLastEntryRead = new Timestamp(startTime);
		ResultSet res = stmt.executeQuery("select TOP "+rowsToReturn
				+" TIMEOFINSERT, SYNCOBJECTS FROM audit WHERE"
				+" SYNCGROUP = '"+GROUP
				+"' AND APP = '"+APP
				+"' AND USERUUID <> '"+USERUUID
				+"' AND TIMEOFINSERT > '"+timeForLastEntryRead+"'"
				);
		//							+"' LIMIT "+MAX_ROWS_TO_RETURN);
		//Send size back to caller as ACK.
		res.last();
		int totalRows = res.getRow();
		res.beforeFirst();
		
		out.writeObject(totalRows+"");
		Blob blob;
		Timestamp time=null,maxTime= null;
		byte[] bytes;

		while (res.next()) {

			time = res.getTimestamp(1);

			blob = res.getBlob(2);
			//System.out.println("EXTRACTING "+i +"with timestamp "+time);
			bytes = blob.getBytes(1,(int)blob.length());
			if (bytes!=null) {
				//Write BLOB!!
				out.writeObject(bytes);

				if (maxTime == null || time.after(maxTime)) {
					//System.out.println("updated maxTime to "+time);
					maxTime = time;
				}
			} else {
				errLog = "Could not turn blob into byte array. User: "+USER;
			}
		}
		//End with new TIME_FOR_LAST_ENTRY_THIS_USER_HAS_CHECKED
		//If no max, use current time.
		if (maxTime == null)
			maxTime = currTime;
		out.writeObject(Long.valueOf(maxTime.getTime()));
		//System.out.println("Flushing!!!");
		out.flush();
		//update call entry.
		PreparedStatement statement = con.prepareStatement("INSERT INTO callentries (SYNCGROUP, CALLER, USERUUID, APP, TIMEOFCALL) VALUES (?,?,?,?,?)");
		statement.setString(1,GROUP);
		statement.setString(2,USER);
		statement.setString(3,USERUUID);					
		statement.setString(4,APP);
		statement.setTimestamp(5, currTime);
		statement.execute();
		//We are done. selebrate with end of stream object.		
		return errLog;
		
	}


	private void printHeadersNames(HttpServletRequest request) {
		System.out.println("Headers:");
		Enumeration<String> names = request.getHeaderNames();
		while (names.hasMoreElements()) {
			System.out.println(names.nextElement());
		}
	}

	private byte[] objToByte(Object object) { 
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		ObjectOutput out = null;
		byte[] bytes;
		try {
			out = new ObjectOutputStream(bos);   
			out.writeObject(object);
			bytes = bos.toByteArray();

		} catch (IOException e) {
			e.printStackTrace();
			return null;
		} finally {
			try {
				if (out != null) {
					out.close();
				}
			} catch (IOException ex) {
				// ignore close exception
			}
			try {
				bos.close();
			} catch (IOException ex) {
				// ignore close exception
			}
		}
		return bytes;
	}


	private void writeLog(String logEntry) {
		writeLog(logEntry,"","");
	}
	private void writeLog(String logEntry, String user, String uuid) {
		if(con != null && logEntry != null) {
			try {
				PreparedStatement statement = con.prepareStatement("INSERT INTO log (TIME, ENTRY, [USER], USERUUID) VALUES (GETUTCDATE(),?,?,?)");
				statement.setString(1, logEntry);
				statement.setString(2, user);
				statement.setString(3, uuid);
				statement.executeUpdate();
			} catch (SQLException ex) { ex.printStackTrace();}
		}
		System.err.println(logEntry);
	}


}
