import java.util.*;
import java.sql.*;
import java.io.*;


public class FilmEditorLoad07 {

   private Connection connection;


//MUSICAL_DIRECTOR TABLE VARIABLES
   private int oldMovieID = 0;
   private int newMovieID = 0;
   private int rank = 0;
   private String filmEditorName = "";
   private String filmEditorUncredited;
   private String filmEditorNotes = "";
   private static final int PERSON_TYPE = 7;

   private int previousID = 0;
   private int filmEditorRecordCount = 0;

   
   private int filmEditorErrors = 0;
   private int filmEditorRowCount = 0;

   private GregorianCalendar currentDate;

   private PrintWriter out;
   private PrintWriter idOut;

   private BufferedReader filmEditorIn;

   private File filmEditorFile;

   private String filmEditorRecord = "";

   private String filmEditorFileName = "";


   private String date = "";

   private String schema = "movies";

   private String directory = 
      "C:\\Users\\Shane Dettmer\\OneDrive\\Documents\\Movies\\Backups\\";



   public FilmEditorLoad07( String inputDate )
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
         System.out.println( "EDITOR Load Errors.txt" );
         System.out.println( "**************************************" );
         System.exit( 1 );
      }


      date = inputDate;

      try {

         filmEditorFileName = directory + "EDITOR " + date + ".txt";
         filmEditorFile = new File( filmEditorFileName );
         if( filmEditorFile.exists() ) {
            filmEditorIn = new BufferedReader(
                            new FileReader( filmEditorFile ) );
            filmEditorRecord = filmEditorIn.readLine();
         }

      }
      catch( IOException ioe ) {
  
         System.out.println( "**************************************" );
         System.out.println( "AN I/O EXCEPTION HAS OCCURRED " );
         System.out.println( filmEditorFileName );
         System.out.println( "**************************************" );
         System.exit( 1 );
      } 
      catch( NullPointerException npe ) {
         System.out.println( "**************************************" );
         System.out.println( "Null File: " );
         System.out.println( filmEditorFileName );
         System.out.println( "**************************************" );
         System.exit( 1 );
      }

      filmEditorLoad();


      exitProgram();

   }

// THIS METHOD LOADS THE DATA TO THE TABLE
   public void filmEditorLoad()
   {

      IDTranslate idtrans = new IDTranslate();

      String tokens[];


      while ( filmEditorRecord != null ) {
         tokens = filmEditorRecord.split( "~" );

         if ( tokens.length == 4 ) {
            filmEditorRecordCount++;
            oldMovieID = Integer.parseInt( tokens[ 0 ] );
            newMovieID = idtrans.getNewMovieID( connection, oldMovieID );
 //           System.out.println("Old ID = " + oldMovieID + " New ID: " + newMovieID);

            filmEditorName = editForApostrophe( tokens[ 1 ] );
            filmEditorUncredited = ( tokens[ 2 ] );
            filmEditorNotes = editForApostrophe( tokens[ 3 ] );

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
                           filmEditorName + "', '" +
                           filmEditorNotes + "', '" +
                           filmEditorUncredited + "', " +
                           "null)";

            if(newMovieID == -1) {
               filmEditorErrors++;
            } else {
               insertTable( query );
               filmEditorRowCount++;
            }
         }
         else {
            out.println( "=====================================================" );
            out.println( "ARRAY NOT CORRECT LENGTH FOR EDITOR: " );
            out.println( filmEditorRecord );
            out.println( "=====================================================" );
            filmEditorErrors++;
         }

         
         readfilmEditorRecord();

      }
      System.out.println( "EDITOR COMPLETE: " + filmEditorRowCount + " ROWS" );
    
         
   }



   private void readfilmEditorRecord() {
   

      try {
         filmEditorRecord = filmEditorIn.readLine();
      }
      catch( IOException ioe ) {
         System.out.println( "*************************************************" );
         System.out.println( "INPUT/OUTPUT ERROR HAS OCCURRED" );
         System.out.println( "THE INPUT FILE IS " + filmEditorFileName );
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
       out.println( "TOTAL INPUT ROWS:               " + filmEditorRecordCount );
       out.println( "TOTAL EDITOR ROWS:              " + filmEditorRowCount );
       out.println( "EDITOR ROWS IN ERROR:           " + filmEditorErrors );


       out.close();

       System.exit( 0 );

   }
   

   public static void main( String args[] )
   {
      String date = args[ 0 ];
      new FilmEditorLoad07( date );
   }

  

}