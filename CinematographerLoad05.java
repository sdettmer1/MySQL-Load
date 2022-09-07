import java.util.*;
import java.sql.*;
import java.io.*;


public class CinematographerLoad05 {

   private Connection connection;


//cinematographer TABLE VARIABLES
   private int oldMovieID = 0;
   private int newMovieID = 0;
   private int rank = 0;
   private String cinematographerName = "";
   private String cinematographerUncredited;
   private String cinematographerNotes = "";
   private static final int PERSON_TYPE = 5;

   private int previousID = 0;
   private int cinematographerRecordCount = 0;

   
   private int cinematographerErrors = 0;
   private int cinematographerRowCount = 0;

   private GregorianCalendar currentDate;

   private PrintWriter out;
   private PrintWriter idOut;

   private BufferedReader cinematographerIn;

   private File cinematographerFile;

   private String cinematographerRecord = "";

   private String cinematographerFileName = "";


   private String date = "";

   private String schema = "movies";

   private String directory = 
      "C:\\Users\\Shane Dettmer\\OneDrive\\Documents\\Movies\\Backups\\";



   public CinematographerLoad05( String inputDate )
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
               new FileWriter("CINEMATOGRAPHER Load Errors.txt")));

      }
      catch( IOException ioe ) {
         
         System.out.println( "**************************************" );
         System.out.println( "AN I/O EXCEPTION HAS OCCURRED " );
         System.out.println( "CINEMATOGRAPHER Load Errors.txt" );
         System.out.println( "**************************************" );
         System.exit( 1 );
      }


      date = inputDate;

      try {

         cinematographerFileName = directory + "CINEMATOGRAPHER " + date + ".txt";
         cinematographerFile = new File( cinematographerFileName );
         if( cinematographerFile.exists() ) {
            cinematographerIn = new BufferedReader(
                            new FileReader( cinematographerFile ) );
            cinematographerRecord = cinematographerIn.readLine();
         }

      }
      catch( IOException ioe ) {
  
         System.out.println( "**************************************" );
         System.out.println( "AN I/O EXCEPTION HAS OCCURRED " );
         System.out.println( cinematographerFileName );
         System.out.println( "**************************************" );
         System.exit( 1 );
      } 
      catch( NullPointerException npe ) {
         System.out.println( "**************************************" );
         System.out.println( "Null File: " );
         System.out.println( cinematographerFileName );
         System.out.println( "**************************************" );
         System.exit( 1 );
      }

      cinematographerLoad();


      exitProgram();

   }

// THIS METHOD LOADS THE DATA TO THE TABLE
   public void cinematographerLoad()
   {

      IDTranslate idtrans = new IDTranslate();

      String tokens[];


      while ( cinematographerRecord != null ) {
         tokens = cinematographerRecord.split( "~" );

         if ( tokens.length == 4 ) {
            cinematographerRecordCount++;
            oldMovieID = Integer.parseInt( tokens[ 0 ] );
            newMovieID = idtrans.getNewMovieID( connection, oldMovieID );
 //           System.out.println("Old ID = " + oldMovieID + " New ID: " + newMovieID);

            cinematographerName = editForApostrophe( tokens[ 1 ] );
            cinematographerUncredited = ( tokens[ 2 ] );
            cinematographerNotes = editForApostrophe( tokens[ 3 ] );

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
                           cinematographerName + "', '" +
                           cinematographerNotes + "', '" +
                           cinematographerUncredited + "', " +
                           "null)";

            if(newMovieID == -1) {
               cinematographerErrors++;
            } else {
               insertTable( query );
               cinematographerRowCount++;
            }
         }
         else {
            out.println( "=====================================================" );
            out.println( "ARRAY NOT CORRECT LENGTH FOR CINEMATOGRAPHER: " );
            out.println( cinematographerRecord );
            out.println( "=====================================================" );
            cinematographerErrors++;
         }

         
         readcinematographerRecord();

      }
      System.out.println( "CINEMATOGRAPHER COMPLETE: " + cinematographerRowCount + " ROWS" );
    
         
   }



   private void readcinematographerRecord() {
   

      try {
         cinematographerRecord = cinematographerIn.readLine();
      }
      catch( IOException ioe ) {
         System.out.println( "*************************************************" );
         System.out.println( "INPUT/OUTPUT ERROR HAS OCCURRED" );
         System.out.println( "THE INPUT FILE IS " + cinematographerFileName );
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
       out.println( "TOTAL INPUT ROWS:               " + cinematographerRecordCount );
       out.println( "TOTAL CINEMATOGRAPHER ROWS:     " + cinematographerRowCount );
       out.println( "CINEMATOGRAPHER ROWS IN ERROR:  " + cinematographerErrors );


       out.close();

       System.exit( 0 );

   }
   

   public static void main( String args[] )
   {
      String date = args[ 0 ];
      new CinematographerLoad05( date );
   }

  

}