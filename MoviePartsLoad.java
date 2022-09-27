import java.util.*;
import java.sql.*;
import java.io.*;

/**
* This class adds data from a ~ delimited text file to the 
* film_parts table in the movies database
*
* @author Shane Dettmer
* @version 1.1
* @since 2022-08-18
*/
public class MoviePartsLoad{

   private Connection connection;


//filmParts TABLE VARIABLES
   private int oldMovieID = 0;
   private int newMovieID = 0;
   private int rank = 0;
   private String filmPartsName = "";
   private String filmPartsTitle = "";
   private int runtime = 0;

   private int previousID = 0;
   private int filmPartsRecordCount = 0;

   
   private int filmPartsErrors = 0;
   private int filmPartsRowCount = 0;

   private GregorianCalendar currentDate;

   private PrintWriter out;

   private BufferedReader filmPartsIn;

   private File filmPartsFile;

   private String filmPartsRecord = "";

   private String filmPartsFileName = "";


   private String date = "";

   private String schema = "movies";

   private String directory = 
      "C:\\Users\\Shane Dettmer\\OneDrive\\Documents\\Movies\\Backups\\";


   /**
   * Accepts the input date of the text file
   * from the command line.  It will establish the MySQL connection
   * create the needed files and initiate the loading process
   *
   * @param inputDate This is the date of the text file entered on command line
   */
   public MoviePartsLoad( String inputDate )
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
               new FileWriter("film_parts Load Errors.txt")));

      }
      catch( IOException ioe ) {
         
         System.out.println( "**************************************" );
         System.out.println( "AN I/O EXCEPTION HAS OCCURRED " );
         System.out.println( "Film Parts Load Errors.txt" );
         System.out.println( "**************************************" );
         System.exit( 1 );
      }


      date = inputDate;

      try {

         filmPartsFileName = directory + "MOVIE_PARTS " + date + ".txt";
         filmPartsFile = new File( filmPartsFileName );
         if( filmPartsFile.exists() ) {
            filmPartsIn = new BufferedReader(
                            new FileReader( filmPartsFile ) );
            filmPartsRecord = filmPartsIn.readLine();
         }

      }
      catch( IOException ioe ) {
  
         System.out.println( "**************************************" );
         System.out.println( "AN I/O EXCEPTION HAS OCCURRED " );
         System.out.println( filmPartsFileName );
         System.out.println( "**************************************" );
         System.exit( 1 );
      } 
      catch( NullPointerException npe ) {
         System.out.println( "**************************************" );
         System.out.println( "Null File: " );
         System.out.println( filmPartsFileName );
         System.out.println( "**************************************" );
         System.exit( 1 );
      }

      System.out.println("File Name: " + filmPartsFileName);
      filmPartsLoad();


      exitProgram();

   }

   /**
   * This method loops through the text file, extracting the 
   * data from each record and then INSERTs a new record into the 
   * table.
   * 
   */
   public void filmPartsLoad()
   {

      IDTranslate idtrans = new IDTranslate();

      String tokens[];


      while ( filmPartsRecord != null ) {
         tokens = filmPartsRecord.split( "~" );

         if ( tokens.length == 5 ) {
            filmPartsRecordCount++;
            oldMovieID = Integer.parseInt( tokens[ 0 ] );
            rank = Integer.parseInt( tokens[ 1 ] );
            newMovieID = idtrans.getNewMovieID( connection, oldMovieID );
//            System.out.println("Old ID = " + oldMovieID + " New ID: " + newMovieID);
 

            filmPartsName = editForApostrophe( tokens[ 2 ] );
            filmPartsTitle = editForApostrophe( tokens[ 3 ] );

            runtime = Integer.parseInt( tokens[ 4 ] );

            if(newMovieID == previousID) {
               rank++;
            } else {
               rank = 1;
               previousID = newMovieID;
            }



            String query = "INSERT INTO " + schema + ".film_parts VALUES(" +
                           newMovieID + ", " +
                           rank + ", '" +
                           filmPartsName + "', '" +
                           filmPartsTitle + "', " +
                           runtime + ")";

            if(newMovieID == -1) {
               filmPartsErrors++;
            } else {
               insertTable( query );

            }
         }
         else {
            out.println( "=====================================================" );
            out.println( "ARRAY NOT CORRECT LENGTH FOR filmParts: " );
            out.println( filmPartsRecord );
            out.println( "=====================================================" );
            filmPartsErrors++;
         }

         
         readFilmPartsRecord();

      }
      System.out.println( "film_parts COMPLETE: " + filmPartsRowCount + " ROWS" );
    
         
   }


   /**
   * Reads next record from input file
   *
   */
   private void readFilmPartsRecord() {
   

      try {
         filmPartsRecord = filmPartsIn.readLine();
      }
      catch( IOException ioe ) {
         System.out.println( "*************************************************" );
         System.out.println( "INPUT/OUTPUT ERROR HAS OCCURRED" );
         System.out.println( "THE INPUT FILE IS " + filmPartsFileName );
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
         filmPartsErrors++;
         return;

      }
        filmPartsRowCount++;


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
       out.println( "TOTAL INPUT ROWS:                    " + filmPartsRecordCount );
       out.println( "TOTAL film_parts ROWS:               " + filmPartsRowCount );
       out.println( "film_parts ROWS IN ERROR:            " + filmPartsErrors );


       out.close();

      try {
         connection.close();
      } catch(SQLException e) {
         System.out.println( "Error occurred when closing SQL connection:" );
         System.out.println( "Error Code " + e.getErrorCode() );
         System.out.println( "SQL State: " + e.getSQLState() );
         e.printStackTrace();
         System.exit(-1);
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
      new MoviePartsLoad( date );
   }

  

}