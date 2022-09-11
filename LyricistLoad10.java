import java.util.*;
import java.sql.*;
import java.io.*;


public class LyricistLoad10 {

   private Connection connection;


//MUSICAL_DIRECTOR TABLE VARIABLES
   private int oldMovieID = 0;
   private int newMovieID = 0;
   private int rank = 0;
   private String lyricistName = "";
   private String lyricistUncredited;
   private String lyricistNotes = "";
   private static final int PERSON_TYPE = 10;

   private int previousID = 0;
   private int lyricistRecordCount = 0;

   
   private int lyricistErrors = 0;
   private int lyricistRowCount = 0;

   private GregorianCalendar currentDate;

   private PrintWriter out;
   private PrintWriter idOut;

   private BufferedReader lyricistIn;

   private File lyricistFile;

   private String lyricistRecord = "";

   private String lyricistFileName = "";


   private String date = "";

   private String schema = "movies";

   private String directory = 
      "C:\\Users\\Shane Dettmer\\OneDrive\\Documents\\Movies\\Backups\\";



   public LyricistLoad10( String inputDate )
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
               new FileWriter("LYRICIST Load Errors.txt")));

      }
      catch( IOException ioe ) {
         
         System.out.println( "**************************************" );
         System.out.println( "AN I/O EXCEPTION HAS OCCURRED " );
         System.out.println( "LYRICIST Load Errors.txt" );
         System.out.println( "**************************************" );
         System.exit( 1 );
      }


      date = inputDate;

      try {

         lyricistFileName = directory + "lyricist " + date + ".txt";
         lyricistFile = new File( lyricistFileName );
         if( lyricistFile.exists() ) {
            lyricistIn = new BufferedReader(
                            new FileReader( lyricistFile ) );
            lyricistRecord = lyricistIn.readLine();
         }

      }
      catch( IOException ioe ) {
  
         System.out.println( "**************************************" );
         System.out.println( "AN I/O EXCEPTION HAS OCCURRED " );
         System.out.println( lyricistFileName );
         System.out.println( "**************************************" );
         System.exit( 1 );
      } 
      catch( NullPointerException npe ) {
         System.out.println( "**************************************" );
         System.out.println( "Null File: " );
         System.out.println( lyricistFileName );
         System.out.println( "**************************************" );
         System.exit( 1 );
      }

      lyricistLoad();


      exitProgram();

   }

// THIS METHOD LOADS THE DATA TO THE TABLE
   public void lyricistLoad()
   {

      IDTranslate idtrans = new IDTranslate();

      String tokens[];


      while ( lyricistRecord != null ) {
         tokens = lyricistRecord.split( "~" );

         if ( tokens.length == 4 ) {
            lyricistRecordCount++;
            oldMovieID = Integer.parseInt( tokens[ 0 ] );
            newMovieID = idtrans.getNewMovieID( connection, oldMovieID );
 //           System.out.println("Old ID = " + oldMovieID + " New ID: " + newMovieID);

            lyricistName = editForApostrophe( tokens[ 1 ] );
            lyricistUncredited = ( tokens[ 2 ] );
            lyricistNotes = editForApostrophe( tokens[ 3 ] );

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
                           lyricistName + "', '" +
                           lyricistNotes + "', '" +
                           lyricistUncredited + "', " +
                           "null)";

            if(newMovieID == -1) {
               lyricistErrors++;
            } else {
               insertTable( query );
               lyricistRowCount++;
            }
         }
         else {
            out.println( "=====================================================" );
            out.println( "ARRAY NOT CORRECT LENGTH FOR LYRICIST: " );
            out.println( lyricistRecord );
            out.println( "=====================================================" );
            lyricistErrors++;
         }

         
         readlyricistRecord();

      }
      System.out.println( "LYRICIST COMPLETE: " + lyricistRowCount + " ROWS" );
    
         
   }



   private void readlyricistRecord() {
   

      try {
         lyricistRecord = lyricistIn.readLine();
      }
      catch( IOException ioe ) {
         System.out.println( "*************************************************" );
         System.out.println( "INPUT/OUTPUT ERROR HAS OCCURRED" );
         System.out.println( "THE INPUT FILE IS " + lyricistFileName );
         ioe.printStackTrace();
         System.out.println( "*************************************************" );
         System.exit( 1 );
      }
   }


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

   public void exitProgram()
   {



       out.println( "MOVIE LIST TABLE RELOAD TOTALS: " );
       out.println( "-------------------------- " );
       out.println( "                           " );
       out.println( "TOTAL INPUT ROWS:               " + lyricistRecordCount );
       out.println( "TOTAL LYRICIST ROWS:            " + lyricistRowCount );
       out.println( "LYRICIST ROWS IN ERROR:         " + lyricistErrors );


       out.close();

       System.exit( 0 );

   }
   

   public static void main( String args[] )
   {
      String date = args[ 0 ];
      new LyricistLoad10( date );
   }

  

}