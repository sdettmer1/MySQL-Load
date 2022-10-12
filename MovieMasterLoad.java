import java.util.*;
import java.sql.*;
import java.io.*;

/**
* This class adds data from a ~ delimited text file to the 
* film_master table in the movies database
*
* @author Shane Dettmer
* @version 1.1
* @since 2022-08-25
*/
public class MovieMasterLoad{

   private Connection connection;


//MOVIEMASTER TABLE VARIABLES
   private int movieID = 0;
   private int oldMovieID = 0;
   private String title = "";
   private String originalTitle = "";
   private int releaseYear = 0;
   private int runtime = 0;
   private String colorInd = "";
   private int monthRecorded = 0;
   private int yearRecorded = 0;
   private int monthLastViewed = 0;
   private int yearLastViewed = 0;
   private String formatInd = "";
   private String mediaType = "";
   private String aspectRatio = "";
   private String hdmiInd = "";
   private String subtitleInd = "N";
   private String silentInd = "N";
   private String colorProcess = "";
   private String cinemaProcess = "";
   private String releaseDate = "";
   private String imagePath = "";
   private String lastUpdate = "";
   
   private int movieMasterErrors = 0;
   private int movieMasterRowCount = 0;




   private GregorianCalendar currentDate;

   private PrintWriter out;
   private PrintWriter idOut;

   private BufferedReader movieMasterIn;

   private File movieMasterFile;

   private File IDFile;

   private String movieMasterRecord = "";

   private String movieMasterFileName = "";


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
   public MovieMasterLoad( String inputDate )
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
               new FileWriter("Movie Master Load Errors.txt")));

      }
      catch( IOException ioe ) {
         
         System.out.println( "**************************************" );
         System.out.println( "AN I/O EXCEPTION HAS OCCURRED " );
         System.out.println( "Movie Master Load Errors.txt" );
         System.out.println( "**************************************" );
         System.exit( 1 );
      }

      try {
         idOut = new PrintWriter(
                    new BufferedWriter(
                    new FileWriter("Movie Master ID File.csv")));
      }
      catch( IOException ioe2 ) {

         System.out.println( "**************************************" );
         System.out.println( "AN I/O EXCEPTION HAS OCCURRED " );
         System.out.println( "Movie Master ID File.txt" );
         System.out.println( "**************************************" );
         System.exit( 1 );
      }

      date = inputDate;

      try {

         movieMasterFileName = directory + "MOVIEMASTER " + date + ".txt";
         movieMasterFile = new File( movieMasterFileName );
         if( movieMasterFile.exists() ) {
            movieMasterIn = new BufferedReader(
                            new FileReader( movieMasterFile ) );
            movieMasterRecord = movieMasterIn.readLine();
         }

      }
      catch( IOException ioe ) {
  
         System.out.println( "**************************************" );
         System.out.println( "AN I/O EXCEPTION HAS OCCURRED " );
         System.out.println( movieMasterFileName );
         System.out.println( "**************************************" );
         System.exit( 1 );
      } 
      catch( NullPointerException npe ) {
         System.out.println( "**************************************" );
         System.out.println( "Null File: " );
         System.out.println( movieMasterFileName );
         System.out.println( "**************************************" );
         System.exit( 1 );
      }

      movieMasterReload();


      exitProgram();

   }

   /**
   * This method loops through the text file, extracting the 
   * data from each record and then INSERTs a new record into the 
   * table.
   * 
   */
   public void movieMasterReload()
   {

      String tokens[];


      while ( movieMasterRecord != null ) {
         tokens = movieMasterRecord.split( "~" );

         if ( tokens.length == 17 ) {
            movieID++;
            oldMovieID = Integer.parseInt( tokens[ 0 ] );
            originalTitle = tokens[ 1 ].trim();
            title = editForApostrophe( tokens [ 1 ] );
            title = editForApostrophe( originalTitle );
            title = title.trim();
            releaseYear = Integer.parseInt( tokens[ 2 ] );
            runtime = Integer.parseInt( tokens[ 3 ] );
            colorInd = tokens[ 4 ].trim();
            if ( colorInd.equals ("--") ) {
               colorInd = "";
            }
            monthRecorded = Integer.parseInt( tokens[ 5 ] );
            yearRecorded  = Integer.parseInt( tokens[ 6 ] );
            monthLastViewed = Integer.parseInt( tokens[ 7 ] );
            yearLastViewed  = Integer.parseInt( tokens[ 8 ] );
            formatInd = tokens[ 9 ].trim();
            mediaType = tokens[ 10 ].trim();
            if ( mediaType.equals( "--" ) ) {
               mediaType = "";
            }

            aspectRatio = editForApostrophe( tokens[ 11 ] );
            aspectRatio = aspectRatio.trim();
            if ( aspectRatio.equals( "--" ) ) {
               aspectRatio = "";
            }

            hdmiInd = tokens[ 12 ];
            
            colorProcess = editForApostrophe( tokens[ 13 ] ).trim();
            cinemaProcess = editForApostrophe( tokens[ 14 ]).trim();
            releaseDate = editForApostrophe( tokens[ 15 ] );
            releaseDate = releaseDate.trim();
            if ( releaseDate.equals( "--" ) ) {
               releaseDate = "";
            }
            lastUpdate = tokens[ 16 ].trim();

            String query = "INSERT INTO " + schema + ".film_master VALUES (" +
                           movieID + ", '" +
                           title + "', " +
                           releaseYear + ", " +
                           runtime + ", " +
                           monthRecorded + ", " +
                           yearRecorded + ", " +
                           monthLastViewed + ", " +
                           yearLastViewed + ", '" +
                           mediaType + "', '" +
                           aspectRatio + "', '" +
                           colorInd + "', '" +
                           formatInd + "', '" + 
                           hdmiInd + "', '" +
                           subtitleInd + "', '" +
                           silentInd + "', '" +
                           colorProcess + "', '" +
                           cinemaProcess + "', '" +
                           releaseDate + "', '" +
                           imagePath + "', '" +
                           lastUpdate + "')";
            insertTable( query );
            movieMasterRowCount++;
         }
         else {
            out.println( "=====================================================" );
            out.println( "ARRAY NOT CORRECT LENGTH FOR MOVIEMASTER: " );
            out.println( movieMasterRecord );
            out.println( "=====================================================" );
            movieMasterErrors++;
         }

         idOut.println( oldMovieID + "," + movieID + 
                            ",\"" + originalTitle + "\"," + releaseYear );

         
         readMovieMasterRecord();

      }
      System.out.println( "MOVIEMASTER COMPLETE: " + movieMasterRowCount + " ROWS" );
    
         
   }


   /**
   * Reads next record from input file
   *
   */
   private void readMovieMasterRecord() {
   

      try {
         movieMasterRecord = movieMasterIn.readLine();
      }
      catch( IOException ioe ) {
         System.out.println( "*************************************************" );
         System.out.println( "INPUT/OUTPUT ERROR HAS OCCURRED" );
         System.out.println( "THE INPUT FILE IS " + movieMasterFileName );
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
       out.println( "TOTAL MOVIEMASTER ROWS:           " + movieMasterRowCount );
       out.println( "MOVIEMASTER ROWS IN ERROR:        " + movieMasterErrors );


       out.close();
       idOut.close();

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
      new MovieMasterLoad( date );
   }

  

}