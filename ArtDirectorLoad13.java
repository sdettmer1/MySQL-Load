import java.util.*;
import java.sql.*;
import java.io.*;

/**
* This class adds data from a ~ delimited text file to the film_person
* table (type = 13) in the movies database
*
* @author Shane Dettmer
* @version 1.1
* @since 2022-09-11
*/
public class ArtDirectorLoad13 {

   private Connection connection;


//MUSICAL_DIRECTOR TABLE VARIABLES
   private int oldMovieID = 0;
   private int newMovieID = 0;
   private int rank = 0;
   private String artDirectorName = "";
   private String artDirectorUncredited;
   private String artDirectorNotes = "";
   private static final int PERSON_TYPE = 13;

   private int previousID = 0;
   private int artDirectorRecordCount = 0;

   
   private int artDirectorErrors = 0;
   private int artDirectorRowCount = 0;

   private GregorianCalendar currentDate;

   private PrintWriter out;
   private PrintWriter idOut;

   private BufferedReader artDirectorIn;

   private File artDirectorFile;

   private String artDirectorRecord = "";

   private String artDirectorFileName = "";


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
   public ArtDirectorLoad13( String inputDate )
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
               new FileWriter("ART_DIRECTOR Load Errors.txt")));

      }
      catch( IOException ioe ) {
         
         System.out.println( "**************************************" );
         System.out.println( "AN I/O EXCEPTION HAS OCCURRED " );
         System.out.println( "ART_DIRECTOR Load Errors.txt" );
         System.out.println( "**************************************" );
         System.exit( 1 );
      }


      date = inputDate;

      try {

         artDirectorFileName = directory + "ART_DIRECTOR " + date + ".txt";
         artDirectorFile = new File( artDirectorFileName );
         if( artDirectorFile.exists() ) {
            artDirectorIn = new BufferedReader(
                            new FileReader( artDirectorFile ) );
            artDirectorRecord = artDirectorIn.readLine();
         }

      }
      catch( IOException ioe ) {
  
         System.out.println( "**************************************" );
         System.out.println( "AN I/O EXCEPTION HAS OCCURRED " );
         System.out.println( artDirectorFileName );
         System.out.println( "**************************************" );
         System.exit( 1 );
      } 
      catch( NullPointerException npe ) {
         System.out.println( "**************************************" );
         System.out.println( "Null File: " );
         System.out.println( artDirectorFileName );
         System.out.println( "**************************************" );
         System.exit( 1 );
      }

      artDirectorLoad();


      exitProgram();

   }

   /**
   * This method loops through the text file, extracting the data
   * from each record and then INSERTs a new record into the 
   * table.
   * 
   */
   public void artDirectorLoad()
   {

      IDTranslate idtrans = new IDTranslate();

      String tokens[];


      while ( artDirectorRecord != null ) {
         tokens = artDirectorRecord.split( "~" );

         if ( tokens.length == 4 ) {
            artDirectorRecordCount++;
            oldMovieID = Integer.parseInt( tokens[ 0 ] );
            newMovieID = idtrans.getNewMovieID( connection, oldMovieID );
 //           System.out.println("Old ID = " + oldMovieID + " New ID: " + newMovieID);

            artDirectorName = editForApostrophe( tokens[ 1 ] );
            artDirectorUncredited = ( tokens[ 2 ] );
            artDirectorNotes = editForApostrophe( tokens[ 3 ] );

            if(newMovieID == previousID) {
               rank++;
            } else {
               rank = 1;
               previousID = newMovieID;
            }

            String query = "INSERT INTO " + schema + ".film_person VALUES(" +
                           newMovieID + ", " +
                           PERSON_TYPE + ", " +
                           rank + ", '" +
                           artDirectorName + "', '" +
                           artDirectorNotes + "', '" +
                           artDirectorUncredited + "', " +
                           "null)";

            if(newMovieID == -1) {
               artDirectorErrors++;
            } else {
               insertTable( query );
               artDirectorRowCount++;
            }
         }
         else {
            out.println( "=====================================================" );
            out.println( "ARRAY NOT CORRECT LENGTH FOR ART_DIRECTOR: " );
            out.println( artDirectorRecord );
            out.println( "=====================================================" );
            artDirectorErrors++;
         }

         
         readartDirectorRecord();

      }
      System.out.println( "ART_DIRECTOR COMPLETE: " + artDirectorRowCount + " ROWS" );
    
         
   }


   /**
   * Reads next record from input file
   *
   */
   private void readartDirectorRecord() {
   

      try {
         artDirectorRecord = artDirectorIn.readLine();
      }
      catch( IOException ioe ) {
         System.out.println( "*************************************************" );
         System.out.println( "INPUT/OUTPUT ERROR HAS OCCURRED" );
         System.out.println( "THE INPUT FILE IS " + artDirectorFileName );
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
       out.println( "TOTAL INPUT ROWS:               " + artDirectorRecordCount );
       out.println( "TOTAL ART_DIRECTOR ROWS:       " + artDirectorRowCount );
       out.println( "ART_DIRECTOR ROWS IN ERROR:    " + artDirectorErrors );


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
      new ArtDirectorLoad13( date );
   }

  

}