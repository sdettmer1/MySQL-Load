import java.util.*;
import java.sql.*;
import java.io.*;

/**
* This class adds data from a ~ delimited text file to the awards_film
* table in the movies database
*
* @author Shane Dettmer
* @version 1.1
* @since 2022-08-26
*/
public class AwardsFilmLoad{

   private Connection connection;


//awardsFilm TABLE VARIABLES
   private int oldMovieID = 0;
   private int newMovieID = 0;
   private int ceremonyID = 0;
   private int awardID = 0;
   private int rank = 0;
   private String nomResult = "";
   private String filmName = "";
   private String awardsNotes = "";

   private int previousID = 0;
   private int awardsFilmRecordCount = 0;

   
   private int awardsFilmErrors = 0;
   private int awardsFilmRowCount = 0;

   private GregorianCalendar currentDate;

   private PrintWriter out;

   private BufferedReader awardsFilmIn;

   private File awardsFilmFile;

   private String awardsFilmRecord = "";

   private String awardsFilmFileName = "";


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
   public AwardsFilmLoad( String inputDate )
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
               new FileWriter("awards_film Load Errors.txt")));

      }
      catch( IOException ioe ) {
         
         System.out.println( "**************************************" );
         System.out.println( "AN I/O EXCEPTION HAS OCCURRED " );
         System.out.println( "Awards Film Load Errors.txt" );
         System.out.println( "**************************************" );
         System.exit( 1 );
      }


      date = inputDate;

      try {

         awardsFilmFileName = directory + "AWARDS_FILM " + date + ".txt";
         awardsFilmFile = new File( awardsFilmFileName );
         if( awardsFilmFile.exists() ) {
            awardsFilmIn = new BufferedReader(
                            new FileReader( awardsFilmFile ) );
            awardsFilmRecord = awardsFilmIn.readLine();
         }

      }
      catch( IOException ioe ) {
  
         System.out.println( "**************************************" );
         System.out.println( "AN I/O EXCEPTION HAS OCCURRED " );
         System.out.println( awardsFilmFileName );
         System.out.println( "**************************************" );
         System.exit( 1 );
      } 
      catch( NullPointerException npe ) {
         System.out.println( "**************************************" );
         System.out.println( "Null File: " );
         System.out.println( awardsFilmFileName );
         System.out.println( "**************************************" );
         System.exit( 1 );
      }

      System.out.println("File Name: " + awardsFilmFileName);
      awardsFilmLoad();


      exitProgram();

   }

   /**
   * This method loops through the text file, extracting the data
   * from each record and then INSERTs a new record into the 
   * table.
   * 
   */
   public void awardsFilmLoad()
   {

      IDTranslate idtrans = new IDTranslate();

      String tokens[];


      while ( awardsFilmRecord != null ) {
         tokens = awardsFilmRecord.split( "~" );

         if ( tokens.length == 7 ) {
            awardsFilmRecordCount++;
            ceremonyID = Integer.parseInt( tokens [ 0 ] );
            awardID = Integer.parseInt( tokens[ 1 ] );
            rank = Integer.parseInt( tokens[ 2 ] );
            nomResult = editForApostrophe( tokens[ 3 ] );
            filmName = editForApostrophe( tokens[ 4 ] );
            oldMovieID = Integer.parseInt( tokens[ 5 ] );  
            awardsNotes = editForApostrophe( tokens[ 6 ] );     
            newMovieID = idtrans.getNewMovieID( connection, oldMovieID );
            
//            System.out.println("Old ID = " + oldMovieID + " New ID: " + newMovieID);



            String query = "INSERT INTO " + schema + ".awards_film VALUES(" +
                           ceremonyID + ", " +
                           awardID + ", " +
                           rank + ", '" +
                           nomResult + "', '" +
                           filmName + "', " +
                           newMovieID + ", '" +
                           awardsNotes + "')";

            if(newMovieID == -1 && oldMovieID != -1) {
               out.println("Error: " + oldMovieID  + "  " + filmName);
               awardsFilmErrors++;
            } else {
               insertTable( query );

            }
         }
         else {
            out.println( "=====================================================" );
            out.println( "ARRAY NOT CORRECT LENGTH FOR AWARDS_FILM: " );
            out.println( awardsFilmRecord );
            out.println( "=====================================================" );
            awardsFilmErrors++;
         }

         
         readAwardsFilmRecord();

      }
      System.out.println( "awards_film COMPLETE: " + awardsFilmRowCount + " ROWS" );
    
         
   }


   /**
   * Reads next record from input file
   *
   */
   private void readAwardsFilmRecord() {
   

      try {
         awardsFilmRecord = awardsFilmIn.readLine();
      }
      catch( IOException ioe ) {
         System.out.println( "*************************************************" );
         System.out.println( "INPUT/OUTPUT ERROR HAS OCCURRED" );
         System.out.println( "THE INPUT FILE IS " + awardsFilmFileName );
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
         awardsFilmErrors++;
         return;

      }
        awardsFilmRowCount++;


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
       out.println( "TOTAL INPUT ROWS:                    " + awardsFilmRecordCount );
       out.println( "TOTAL awards_film ROWS:              " + awardsFilmRowCount );
       out.println( "awards_film ROWS IN ERROR:           " + awardsFilmErrors );


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
      new AwardsFilmLoad( date );
   }

  

}