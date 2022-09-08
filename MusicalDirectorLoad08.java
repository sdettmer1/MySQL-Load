import java.util.*;
import java.sql.*;
import java.io.*;


public class MusicalDirectorLoad08 {

   private Connection connection;


//MUSICAL_DIRECTOR TABLE VARIABLES
   private int oldMovieID = 0;
   private int newMovieID = 0;
   private int rank = 0;
   private String musicalDirectorName = "";
   private String musicalDirectorUncredited;
   private String musicalDirectorNotes = "";
   private static final int PERSON_TYPE = 8;

   private int previousID = 0;
   private int musicalDirectorRecordCount = 0;

   
   private int musicalDirectorErrors = 0;
   private int musicalDirectorRowCount = 0;

   private GregorianCalendar currentDate;

   private PrintWriter out;
   private PrintWriter idOut;

   private BufferedReader musicalDirectorIn;

   private File musicalDirectorFile;

   private String musicalDirectorRecord = "";

   private String musicalDirectorFileName = "";


   private String date = "";

   private String schema = "movies";

   private String directory = 
      "C:\\Users\\Shane Dettmer\\OneDrive\\Documents\\Movies\\Backups\\";



   public MusicalDirectorLoad08( String inputDate )
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
               new FileWriter("MUSICAL_DIRECTOR Load Errors.txt")));

      }
      catch( IOException ioe ) {
         
         System.out.println( "**************************************" );
         System.out.println( "AN I/O EXCEPTION HAS OCCURRED " );
         System.out.println( "MUSICAL_DIRECTOR Load Errors.txt" );
         System.out.println( "**************************************" );
         System.exit( 1 );
      }


      date = inputDate;

      try {

         musicalDirectorFileName = directory + "MUSICAL_DIRECTOR " + date + ".txt";
         musicalDirectorFile = new File( musicalDirectorFileName );
         if( musicalDirectorFile.exists() ) {
            musicalDirectorIn = new BufferedReader(
                            new FileReader( musicalDirectorFile ) );
            musicalDirectorRecord = musicalDirectorIn.readLine();
         }

      }
      catch( IOException ioe ) {
  
         System.out.println( "**************************************" );
         System.out.println( "AN I/O EXCEPTION HAS OCCURRED " );
         System.out.println( musicalDirectorFileName );
         System.out.println( "**************************************" );
         System.exit( 1 );
      } 
      catch( NullPointerException npe ) {
         System.out.println( "**************************************" );
         System.out.println( "Null File: " );
         System.out.println( musicalDirectorFileName );
         System.out.println( "**************************************" );
         System.exit( 1 );
      }

      musicalDirectorLoad();


      exitProgram();

   }

// THIS METHOD LOADS THE DATA TO THE TABLE
   public void musicalDirectorLoad()
   {

      IDTranslate idtrans = new IDTranslate();

      String tokens[];


      while ( musicalDirectorRecord != null ) {
         tokens = musicalDirectorRecord.split( "~" );

         if ( tokens.length == 4 ) {
            musicalDirectorRecordCount++;
            oldMovieID = Integer.parseInt( tokens[ 0 ] );
            newMovieID = idtrans.getNewMovieID( connection, oldMovieID );
 //           System.out.println("Old ID = " + oldMovieID + " New ID: " + newMovieID);

            musicalDirectorName = editForApostrophe( tokens[ 1 ] );
            musicalDirectorUncredited = ( tokens[ 2 ] );
            musicalDirectorNotes = editForApostrophe( tokens[ 3 ] );

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
                           musicalDirectorName + "', '" +
                           musicalDirectorNotes + "', '" +
                           musicalDirectorUncredited + "', " +
                           "null)";

            if(newMovieID == -1) {
               musicalDirectorErrors++;
            } else {
               insertTable( query );
               musicalDirectorRowCount++;
            }
         }
         else {
            out.println( "=====================================================" );
            out.println( "ARRAY NOT CORRECT LENGTH FOR MUSICAL_DIRECTOR: " );
            out.println( musicalDirectorRecord );
            out.println( "=====================================================" );
            musicalDirectorErrors++;
         }

         
         readmusicalDirectorRecord();

      }
      System.out.println( "MUSICAL_DIRECTOR COMPLETE: " + musicalDirectorRowCount + " ROWS" );
    
         
   }



   private void readmusicalDirectorRecord() {
   

      try {
         musicalDirectorRecord = musicalDirectorIn.readLine();
      }
      catch( IOException ioe ) {
         System.out.println( "*************************************************" );
         System.out.println( "INPUT/OUTPUT ERROR HAS OCCURRED" );
         System.out.println( "THE INPUT FILE IS " + musicalDirectorFileName );
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
       out.println( "TOTAL INPUT ROWS:               " + musicalDirectorRecordCount );
       out.println( "TOTAL MUSICAL_DIRECTOR ROWS:    " + musicalDirectorRowCount );
       out.println( "MUSICAL_DIRECTOR ROWS IN ERROR: " + musicalDirectorErrors );


       out.close();

       System.exit( 0 );

   }
   

   public static void main( String args[] )
   {
      String date = args[ 0 ];
      new MusicalDirectorLoad08( date );
   }

  

}