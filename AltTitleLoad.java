import java.util.*;
import java.sql.*;
import java.io.*;

/**
* This class adds data from a ~ delimited text file to the film_alternate_title
* table in the movies database
*
* @author Shane Dettmer
* @version 1.1
* @since 2022-09-05
*/
public class AltTitleLoad{

   private Connection connection;


//altTitle TABLE VARIABLES
   private int oldMovieID = 0;
   private int newMovieID = 0;
   private String altTitle = "";
   private static final String altTitleNotes = "";
   private int rank;

   private int previousID = 0;
   private int altTitleCount = 0;

   
   private int altTitleErrors = 0;
   private int altTitleRowCount = 0;

   private GregorianCalendar currentDate;

   private PrintWriter out;

   private BufferedReader altTitleIn;

   private File altTitleFile;

   private String altTitleRecord = "";

   private String altTitleFileName = "";


   private String date = "";

   private String schema = "movies";

   private String directory = 
      "C:\\Users\\Shane Dettmer\\OneDrive\\Documents\\Movies\\Backups\\";


   /**
   * Accepts the input date of the text file
   * from the command line.  It will establish the MySQL connection,
   * create the needed files and initiate the loading process
   *
   * @param inputDate This is the date of the text file entered on command line
   */
   public AltTitleLoad( String inputDate )
   {

      try
      {
           Class.forName("com.mysql.cj.jdbc.Driver");
      }
      catch ( ClassNotFoundException e )
      {
         System.out.println( "Error registering driver" );
         e.printStackTrace();
         System.exit( 1 );
      }

      try {
            String connectionURL = "jdbc:mysql://localhost:3306/movies";
            connection = DriverManager.getConnection(connectionURL, "default", "test1234");

            connection.setAutoCommit( true );
      }

      catch ( SQLException sqle ) 
      {
         System.out.println( "SQL Error occurred" );
         sqle.printStackTrace();
      }

      catch ( Exception e ) {
         System.out.println( "ERROR occurred" );
         e.printStackTrace();
      }


      //=================================
      //CREATE THE OUTPUT ERROR FILE
      //=================================
      try {

         out = new PrintWriter(
               new BufferedWriter(
               new FileWriter("film_altTitle_language Load Errors.txt")));

      }
      catch( IOException ioe ) {
         
         System.out.println( "**************************************" );
         System.out.println( "AN I/O EXCEPTION HAS OCCURRED " );
         System.out.println( "film_alternate_title Load Errors.txt" );
         System.out.println( "**************************************" );
         System.exit( 1 );
      }


      date = inputDate;

      try {

         altTitleFileName = directory + "ALTERNATE_TITLE " + date + ".txt";
         altTitleFile = new File( altTitleFileName );
         if( altTitleFile.exists() ) {
            altTitleIn = new BufferedReader(
                            new FileReader( altTitleFile ) );
            altTitleRecord = altTitleIn.readLine();
         }

      }
      catch( IOException ioe ) {
  
         System.out.println( "**************************************" );
         System.out.println( "AN I/O EXCEPTION HAS OCCURRED " );
         System.out.println( altTitleFileName );
         System.out.println( "**************************************" );
         System.exit( 1 );
      } 
      catch( NullPointerException npe ) {
         System.out.println( "**************************************" );
         System.out.println( "Null File: " );
         System.out.println( altTitleFileName );
         System.out.println( "**************************************" );
         System.exit( 1 );
      }

      altTitleLoad();


      exitProgram();

   }

   /**
   * This method loops through the text file, extracting the data
   * data from each record and then INSERTs a new record into the 
   * table.
   * 
   */
   public void altTitleLoad()
   {

      IDTranslate idtrans = new IDTranslate();

      String tokens[];


      while ( altTitleRecord != null ) {
         tokens = altTitleRecord.split( "~" );

         if ( tokens.length == 2 ) {
            altTitleCount++;
            oldMovieID = Integer.parseInt( tokens[ 0 ] );
            newMovieID = idtrans.getNewMovieID( connection, oldMovieID );
//            System.out.println("Old ID = " + oldMovieID + " New ID: " + newMovieID);
 

            altTitle = editForApostrophe( tokens[ 1 ] );

            if(newMovieID == previousID) {
               rank++;
            } else {
               rank = 1;
               previousID = newMovieID;
            }


            String query = "INSERT INTO " + schema + ".film_alternate_title VALUES(" +
                           newMovieID + ", " +
                           rank + ",'" +
                           altTitle + "', '" +
                           altTitleNotes + "')";

            if(newMovieID == -1) {
               altTitleErrors++;
            } else {
               insertTable( query );
               altTitleRowCount++;
            }
         }
         else {
            out.println( "=====================================================" );
            out.println( "ARRAY NOT CORRECT LENGTH FOR altTitle: " );
            out.println( altTitleRecord );
            out.println( "=====================================================" );
            altTitleErrors++;
         }

         
         readaltTitleRecord();

      }
      System.out.println( "film_alternate_title COMPLETE: " + altTitleRowCount + " ROWS" );
    
         
   }


   /**
   * Reads next record from input file
   *
   */
   private void readaltTitleRecord() {
   

      try {
         altTitleRecord = altTitleIn.readLine();
      }
      catch( IOException ioe ) {
         System.out.println( "*************************************************" );
         System.out.println( "INPUT/OUTPUT ERROR HAS OCCURRED" );
         System.out.println( "THE INPUT FILE IS " + altTitleFileName );
         ioe.printStackTrace();
         System.out.println( "*************************************************" );
         System.exit( 1 );
      }
   }

   /**
   * Inserts a record into the database table
   *
   *@param query SQL statement that will be executed
   */
   public void insertTable( String query )
   {
      
      try {
         Statement statement = connection.createStatement();
         int result = statement.executeUpdate( query );

//         connection.commit();
         statement.close();
      }
        
      catch ( SQLException sqle ) {
         sqle.printStackTrace();

         out.println( "   " );
         out.println( "=================================================================" );
         String output = "";
         output += "DB2 Encountered an Error during Table Update:\n" +
                   "SQL STATEMENT WHEN ERROR OCCURED:\n" +
                   query + "\n" +
                   "Error Code: " + sqle.getErrorCode() +
                   "\nSQLState: " + sqle.getSQLState() +
                   "\nMessage: " +  sqle.getMessage();
         out.println( output );
         out.println( "=================================================================" );
         System.out.println( output );

      }


   }

   /**
   * Searches for an apostrophe in a given String and then
   * adds a new apostrophe next to it to prevent SQL errors
   * 
   * @param name the String to be edited
   * @return String Returms the edited version of the input String
   */
   public static String editForApostrophe( String name )
   {

      String editedInput = "";


      for ( int i = 0; i < name.length(); i++ )
      {
         if ( name.charAt( i ) != '\'' )
            editedInput += name.charAt( i );
         else  {
            editedInput += name.charAt( i );
            editedInput += '\'';
         }
      }

      return editedInput;
   }

   /**
   * Writes control totals to the output file and
   * closes all resources and exits the program
   *
   */
   public void exitProgram()
   {



       out.println( "MOVIE LIST TABLE RELOAD TOTALS: " );
       out.println( "-------------------------- " );
       out.println( "                           " );
       out.println( "TOTAL INPUT ROWS:                    " + altTitleCount );
       out.println( "TOTAL film_altTitle_language ROWS:           " + altTitleRowCount );
       out.println( "film_altTitle_language ROWS IN ERROR:        " + altTitleErrors );


       out.close();

      try {
         connection.close();
      } catch(SQLException e) {
         System.out.println( "Error occurred when closing SQL connection:" );
         System.out.println( "Error Code " + e.getErrorCode() );
         System.out.println( "SQL State: " + e.getSQLState() );
         e.printStackTrace();
      }

       System.exit( 0 );

   }
   

   /**
   * Creates an instance of the class and runs the application
   * One argument is necessary on the command line
   *
   * @param args The date portion of the file name in yyyy-mm-dd format
   */
   public static void main( String args[] )
   {
      String date = args[ 0 ];
      new AltTitleLoad( date );
   }

  

}