import java.util.*;
import java.sql.*;
import java.io.*;


public class ChoreographerLoad11 {

   private Connection connection;


//MUSICAL_DIRECTOR TABLE VARIABLES
   private int oldMovieID = 0;
   private int newMovieID = 0;
   private int rank = 0;
   private String choreographerName = "";
   private String choreographerUncredited;
   private String choreographerNotes = "";
   private static final int PERSON_TYPE = 11;

   private int previousID = 0;
   private int choreographerRecordCount = 0;

   
   private int choreographerErrors = 0;
   private int choreographerRowCount = 0;

   private GregorianCalendar currentDate;

   private PrintWriter out;
   private PrintWriter idOut;

   private BufferedReader choreographerIn;

   private File choreographerFile;

   private String choreographerRecord = "";

   private String choreographerFileName = "";


   private String date = "";

   private String schema = "movies";

   private String directory = 
      "C:\\Users\\Shane Dettmer\\OneDrive\\Documents\\Movies\\Backups\\";



   public ChoreographerLoad11( String inputDate )
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
               new FileWriter("choreographer Load Errors.txt")));

      }
      catch( IOException ioe ) {
         
         System.out.println( "**************************************" );
         System.out.println( "AN I/O EXCEPTION HAS OCCURRED " );
         System.out.println( "CHOREOGRAPHER Load Errors.txt" );
         System.out.println( "**************************************" );
         System.exit( 1 );
      }


      date = inputDate;

      try {

         choreographerFileName = directory + "CHOREOGRAPHER " + date + ".txt";
         choreographerFile = new File( choreographerFileName );
         if( choreographerFile.exists() ) {
            choreographerIn = new BufferedReader(
                            new FileReader( choreographerFile ) );
            choreographerRecord = choreographerIn.readLine();
         }

      }
      catch( IOException ioe ) {
  
         System.out.println( "**************************************" );
         System.out.println( "AN I/O EXCEPTION HAS OCCURRED " );
         System.out.println( choreographerFileName );
         System.out.println( "**************************************" );
         System.exit( 1 );
      } 
      catch( NullPointerException npe ) {
         System.out.println( "**************************************" );
         System.out.println( "Null File: " );
         System.out.println( choreographerFileName );
         System.out.println( "**************************************" );
         System.exit( 1 );
      }

      choreographerLoad();


      exitProgram();

   }

// THIS METHOD LOADS THE DATA TO THE TABLE
   public void choreographerLoad()
   {

      IDTranslate idtrans = new IDTranslate();

      String tokens[];


      while ( choreographerRecord != null ) {
         tokens = choreographerRecord.split( "~" );

         if ( tokens.length == 4 ) {
            choreographerRecordCount++;
            oldMovieID = Integer.parseInt( tokens[ 0 ] );
            newMovieID = idtrans.getNewMovieID( connection, oldMovieID );
 //           System.out.println("Old ID = " + oldMovieID + " New ID: " + newMovieID);

            choreographerName = editForApostrophe( tokens[ 1 ] );
            choreographerUncredited = ( tokens[ 2 ] );
            choreographerNotes = editForApostrophe( tokens[ 3 ] );

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
                           choreographerName + "', '" +
                           choreographerNotes + "', '" +
                           choreographerUncredited + "', " +
                           "null)";

            if(newMovieID == -1) {
               choreographerErrors++;
            } else {
               insertTable( query );
               choreographerRowCount++;
            }
         }
         else {
            out.println( "=====================================================" );
            out.println( "ARRAY NOT CORRECT LENGTH FOR CHOREOGRAPHER: " );
            out.println( choreographerRecord );
            out.println( "=====================================================" );
            choreographerErrors++;
         }

         
         readchoreographerRecord();

      }
      System.out.println( "CHOREOGRAPHER COMPLETE: " + choreographerRowCount + " ROWS" );
    
         
   }



   private void readchoreographerRecord() {
   

      try {
         choreographerRecord = choreographerIn.readLine();
      }
      catch( IOException ioe ) {
         System.out.println( "*************************************************" );
         System.out.println( "INPUT/OUTPUT ERROR HAS OCCURRED" );
         System.out.println( "THE INPUT FILE IS " + choreographerFileName );
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
       out.println( "TOTAL INPUT ROWS:               " + choreographerRecordCount );
       out.println( "TOTAL CHOREOGRAPHER ROWS:       " + choreographerRowCount );
       out.println( "CHOREOGRAPHER ROWS IN ERROR:    " + choreographerErrors );


       out.close();

       System.exit( 0 );

   }
   

   public static void main( String args[] )
   {
      String date = args[ 0 ];
      new ChoreographerLoad11( date );
   }

  

}