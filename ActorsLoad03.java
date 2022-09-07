import java.util.*;
import java.sql.*;
import java.io.*;


public class ActorsLoad03 {

   private Connection connection;


//ACTORS TABLE VARIABLES
   private int oldMovieID = 0;
   private int newMovieID = 0;
   private int rank = 0;
   private String actorName = "";
   private String actorUncredited;
   private String actorNotes = "";
   private String actorRole = "";

   private static final int PERSON_TYPE = 3;

   private int actorRecordCount = 0;

   
   private int actorErrors = 0;
   private int actorRowCount = 0;

   private GregorianCalendar currentDate;

   private PrintWriter out;
   private PrintWriter idOut;

   private BufferedReader actorIn;

   private File actorFile;

   private String actorRecord = "";

   private String actorFileName = "";


   private String date = "";

   private String schema = "movies";

   private String directory = 
      "C:\\Users\\Shane Dettmer\\OneDrive\\Documents\\Movies\\Backups\\";



   public ActorsLoad03( String inputDate )
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
               new FileWriter("ACTORS Load Errors.txt")));

      }
      catch( IOException ioe ) {
         
         System.out.println( "**************************************" );
         System.out.println( "AN I/O EXCEPTION HAS OCCURRED " );
         System.out.println( "ACTORS Load Errors.txt" );
         System.out.println( "**************************************" );
         System.exit( 1 );
      }


      date = inputDate;

      try {

         actorFileName = directory + "ACTORS " + date + ".txt";
         actorFile = new File( actorFileName );
         if( actorFile.exists() ) {
            actorIn = new BufferedReader(
                            new FileReader( actorFile ) );
            actorRecord = actorIn.readLine();
         }

      }
      catch( IOException ioe ) {
  
         System.out.println( "**************************************" );
         System.out.println( "AN I/O EXCEPTION HAS OCCURRED " );
         System.out.println( actorFileName );
         System.out.println( "**************************************" );
         System.exit( 1 );
      } 
      catch( NullPointerException npe ) {
         System.out.println( "**************************************" );
         System.out.println( "Null File: " );
         System.out.println( actorFileName );
         System.out.println( "**************************************" );
         System.exit( 1 );
      }

      actorLoad();


      exitProgram();

   }

// THIS METHOD LOADS THE DATA TO THE TABLE
   public void actorLoad()
   {

      IDTranslate idtrans = new IDTranslate();

      String tokens[];


      while ( actorRecord != null ) {
         tokens = actorRecord.split( "~" );

         if ( tokens.length == 6 ) {
            actorRecordCount++;
            oldMovieID = Integer.parseInt( tokens[ 0 ] );
            newMovieID = idtrans.getNewMovieID( connection, oldMovieID );
 //           System.out.println("Old ID = " + oldMovieID + " New ID: " + newMovieID);

            actorName = editForApostrophe( tokens[ 1 ] );
            actorUncredited = ( tokens[ 2 ] );
            actorNotes = editForApostrophe( tokens[ 3 ] );
            rank = Integer.parseInt( tokens[ 4 ] );
            actorRole = editForApostrophe( tokens[ 5 ] );



            String query = "INSERT INTO " + schema + ".film_person VALUES(" +
                           newMovieID + ", " +
                           PERSON_TYPE + ", " +
                           rank + ", '" +
                           actorName + "', '" +
                           actorNotes + "', '" +
                           actorUncredited + "', '" +
                           actorRole + "')";

            if(newMovieID == -1) {
               actorErrors++;
            } else {
               insertTable( query );
               actorRowCount++;
            }
         }
         else {
            out.println( "=====================================================" );
            out.println( "ARRAY NOT CORRECT LENGTH FOR ACTORS: " );
            out.println( actorRecord );
            out.println( "=====================================================" );
            actorErrors++;
         }

         
         readactorRecord();

      }
      System.out.println( "ACTORS COMPLETE: " + actorRowCount + " ROWS" );
    
         
   }



   private void readactorRecord() {
   

      try {
         actorRecord = actorIn.readLine();
      }
      catch( IOException ioe ) {
         System.out.println( "*************************************************" );
         System.out.println( "INPUT/OUTPUT ERROR HAS OCCURRED" );
         System.out.println( "THE INPUT FILE IS " + actorFileName );
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
       out.println( "TOTAL INPUT ROWS:             " + actorRecordCount );
       out.println( "TOTAL ACTORS ROWS:            " + actorRowCount );
       out.println( "ACTORS ROWS IN ERROR:         " + actorErrors );


       out.close();

       System.exit( 0 );

   }
   

   public static void main( String args[] )
   {
      String date = args[ 0 ];
      new ActorsLoad03( date );
   }

  

}