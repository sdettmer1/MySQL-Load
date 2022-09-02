import java.util.*;
import java.sql.*;
import java.io.*;


public class LastViewedLoad{

   private Connection connection;


//lastViewed TABLE VARIABLES
   private int oldMovieID = 0;
   private int newMovieID = 0;
   private String title = "";
   private int releaseYear = 0;
   private int lastViewedYear = 0;
   private int lastViewedMonth = 0;

   private int lastViewedRecordCount = 0;

   
   private int lastViewedErrors = 0;
   private int lastViewedRowCount = 0;

   private GregorianCalendar currentDate;

   private PrintWriter out;

   private BufferedReader lastViewedIn;

   private File lastViewedFile;

   private String lastViewedRecord = "";

   private String lastViewedFileName = "";


   private String date = "";

   private String schema = "movies";

   private String directory = 
      "C:\\Users\\Shane Dettmer\\OneDrive\\Documents\\Movies\\Backups\\";



   public LastViewedLoad( String inputDate )
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

         lastViewedFileName = directory + "LAST_VIEWED " + date + ".txt";
         lastViewedFile = new File( lastViewedFileName );
         if( lastViewedFile.exists() ) {
            lastViewedIn = new BufferedReader(
                            new FileReader( lastViewedFile ) );
            lastViewedRecord = lastViewedIn.readLine();
         }

      }
      catch( IOException ioe ) {
  
         System.out.println( "**************************************" );
         System.out.println( "AN I/O EXCEPTION HAS OCCURRED " );
         System.out.println( lastViewedFileName );
         System.out.println( "**************************************" );
         System.exit( 1 );
      } 
      catch( NullPointerException npe ) {
         System.out.println( "**************************************" );
         System.out.println( "Null File: " );
         System.out.println( lastViewedFileName );
         System.out.println( "**************************************" );
         System.exit( 1 );
      }

      System.out.println("File Name: " + lastViewedFileName);
      lastViewedLoad();


      exitProgram();

   }

   public void lastViewedLoad()
   {

      IDTranslate idtrans = new IDTranslate();

      String tokens[];


      while ( lastViewedRecord != null ) {
         tokens = lastViewedRecord.split( "~" );

         if ( tokens.length == 5 ) {
            lastViewedRecordCount++;
            lastViewedYear = Integer.parseInt( tokens [ 0 ] );
            lastViewedMonth = Integer.parseInt( tokens [ 1 ] );
            title = editForApostrophe( tokens [ 2 ] );
            releaseYear = Integer.parseInt( tokens [ 3 ] );
            oldMovieID = Integer.parseInt( tokens[ 4 ] );
       
            newMovieID = idtrans.getNewMovieID( connection, oldMovieID );
            
//            System.out.println("Old ID = " + oldMovieID + " New ID: " + newMovieID);



            String query = "INSERT INTO " + schema + ".film_last_viewed VALUES(" +
                           lastViewedYear + ", " +
                           lastViewedMonth + ", " +
                           title + "', " +
                           releaseYear + ", " +
                           newMovieID + ")";

            if(newMovieID == -1) {
               out.println("Error: " + oldMovieID  + "  " + title);
               lastViewedErrors++;
            } else {
               insertTable( query );

            }
         }
         else {
            out.println( "=====================================================" );
            out.println( "ARRAY NOT CORRECT LENGTH FOR LAST_VIEWED: " );
            out.println( lastViewedRecord );
            out.println( "=====================================================" );
            lastViewedErrors++;
         }

         
         readlastViewedRecord();

      }
      System.out.println( "film_last_viewed COMPLETE: " + lastViewedRowCount + " ROWS" );
    
         
   }



   private void readlastViewedRecord() {
   

      try {
         lastViewedRecord = lastViewedIn.readLine();
      }
      catch( IOException ioe ) {
         System.out.println( "*************************************************" );
         System.out.println( "INPUT/OUTPUT ERROR HAS OCCURRED" );
         System.out.println( "THE INPUT FILE IS " + lastViewedFileName );
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
         lastViewedErrors++;
         return;

      }
        lastViewedRowCount++;


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
       out.println( "TOTAL INPUT ROWS:                    " + lastViewedRecordCount );
       out.println( "TOTAL awards_film ROWS:              " + lastViewedRowCount );
       out.println( "awards_film ROWS IN ERROR:           " + lastViewedErrors );


       out.close();

       System.exit( 0 );

   }
   

   public static void main( String args[] )
   {
      String date = args[ 0 ];
      new LastViewedLoad( date );
   }

  

}