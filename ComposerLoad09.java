import java.util.*;
import java.sql.*;
import java.io.*;


public class ComposerLoad09 {

   private Connection connection;


//MUSICAL_DIRECTOR TABLE VARIABLES
   private int oldMovieID = 0;
   private int newMovieID = 0;
   private int rank = 0;
   private String composerName = "";
   private String composerUncredited;
   private String composerNotes = "";
   private static final int PERSON_TYPE = 9;

   private int previousID = 0;
   private int composerRecordCount = 0;

   
   private int composerErrors = 0;
   private int composerRowCount = 0;

   private GregorianCalendar currentDate;

   private PrintWriter out;
   private PrintWriter idOut;

   private BufferedReader composerIn;

   private File composerFile;

   private String composerRecord = "";

   private String composerFileName = "";


   private String date = "";

   private String schema = "movies";

   private String directory = 
      "C:\\Users\\Shane Dettmer\\OneDrive\\Documents\\Movies\\Backups\\";



   public ComposerLoad09( String inputDate )
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
               new FileWriter("COMPOSER Load Errors.txt")));

      }
      catch( IOException ioe ) {
         
         System.out.println( "**************************************" );
         System.out.println( "AN I/O EXCEPTION HAS OCCURRED " );
         System.out.println( "COMPOSER Load Errors.txt" );
         System.out.println( "**************************************" );
         System.exit( 1 );
      }


      date = inputDate;

      try {

         composerFileName = directory + "COMPOSER " + date + ".txt";
         composerFile = new File( composerFileName );
         if( composerFile.exists() ) {
            composerIn = new BufferedReader(
                            new FileReader( composerFile ) );
            composerRecord = composerIn.readLine();
         }

      }
      catch( IOException ioe ) {
  
         System.out.println( "**************************************" );
         System.out.println( "AN I/O EXCEPTION HAS OCCURRED " );
         System.out.println( composerFileName );
         System.out.println( "**************************************" );
         System.exit( 1 );
      } 
      catch( NullPointerException npe ) {
         System.out.println( "**************************************" );
         System.out.println( "Null File: " );
         System.out.println( composerFileName );
         System.out.println( "**************************************" );
         System.exit( 1 );
      }

      composerLoad();


      exitProgram();

   }

// THIS METHOD LOADS THE DATA TO THE TABLE
   public void composerLoad()
   {

      IDTranslate idtrans = new IDTranslate();

      String tokens[];


      while ( composerRecord != null ) {
         tokens = composerRecord.split( "~" );

         if ( tokens.length == 4 ) {
            composerRecordCount++;
            oldMovieID = Integer.parseInt( tokens[ 0 ] );
            newMovieID = idtrans.getNewMovieID( connection, oldMovieID );
 //           System.out.println("Old ID = " + oldMovieID + " New ID: " + newMovieID);

            composerName = editForApostrophe( tokens[ 1 ] );
            composerUncredited = ( tokens[ 2 ] );
            composerNotes = editForApostrophe( tokens[ 3 ] );

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
                           composerName + "', '" +
                           composerNotes + "', '" +
                           composerUncredited + "', " +
                           "null)";

            if(newMovieID == -1) {
               composerErrors++;
            } else {
               insertTable( query );
               composerRowCount++;
            }
         }
         else {
            out.println( "=====================================================" );
            out.println( "ARRAY NOT CORRECT LENGTH FOR COMPOSER: " );
            out.println( composerRecord );
            out.println( "=====================================================" );
            composerErrors++;
         }

         
         readcomposerRecord();

      }
      System.out.println( "COMPOSER COMPLETE: " + composerRowCount + " ROWS" );
    
         
   }



   private void readcomposerRecord() {
   

      try {
         composerRecord = composerIn.readLine();
      }
      catch( IOException ioe ) {
         System.out.println( "*************************************************" );
         System.out.println( "INPUT/OUTPUT ERROR HAS OCCURRED" );
         System.out.println( "THE INPUT FILE IS " + composerFileName );
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
       out.println( "TOTAL INPUT ROWS:               " + composerRecordCount );
       out.println( "TOTAL COMPOSER ROWS:            " + composerRowCount );
       out.println( "COMPOSER ROWS IN ERROR:         " + composerErrors );


       out.close();

       System.exit( 0 );

   }
   

   public static void main( String args[] )
   {
      String date = args[ 0 ];
      new ComposerLoad09( date );
   }

  

}