import java.util.*;
import java.sql.*;
import java.io.*;


public class WritersLoad02 {

   private Connection connection;


//writer TABLE VARIABLES
   private int oldMovieID = 0;
   private int newMovieID = 0;
   private int rank = 0;
   private String writerName = "";
   private String writerUncredited;
   private String writerNotes = "";

   private static final int PERSON_TYPE = 2;


   private int previousID = 0;
   private int writerRecordCount = 0;

   
   private int writerErrors = 0;
   private int writerRowCount = 0;

   private GregorianCalendar currentDate;

   private PrintWriter out;
   private PrintWriter idOut;

   private BufferedReader writerIn;

   private File writerFile;

   private String writerRecord = "";

   private String writerFileName = "";


   private String date = "";

   private String schema = "movies";

   private String writery = 
      "C:\\Users\\Shane Dettmer\\OneDrive\\Documents\\Movies\\Backups\\";



   public WritersLoad02( String inputDate )
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
               new FileWriter("Writers Load Errors.txt")));

      }
      catch( IOException ioe ) {
         
         System.out.println( "**************************************" );
         System.out.println( "AN I/O EXCEPTION HAS OCCURRED " );
         System.out.println( "WRITERS Load Errors.txt" );
         System.out.println( "**************************************" );
         System.exit( 1 );
      }


      date = inputDate;

      try {

         writerFileName = writery + "WRITERS " + date + ".txt";
         writerFile = new File( writerFileName );
         if( writerFile.exists() ) {
            writerIn = new BufferedReader(
                            new FileReader( writerFile ) );
            writerRecord = writerIn.readLine();
         }

      }
      catch( IOException ioe ) {
  
         System.out.println( "**************************************" );
         System.out.println( "AN I/O EXCEPTION HAS OCCURRED " );
         System.out.println( writerFileName );
         System.out.println( "**************************************" );
         System.exit( 1 );
      } 
      catch( NullPointerException npe ) {
         System.out.println( "**************************************" );
         System.out.println( "Null File: " );
         System.out.println( writerFileName );
         System.out.println( "**************************************" );
         System.exit( 1 );
      }

      writerLoad();


      exitProgram();

   }

// THIS METHOD LOADS THE DATA TO THE TABLE
   public void writerLoad()
   {

      IDTranslate idtrans = new IDTranslate();

      String tokens[];


      while ( writerRecord != null ) {
         tokens = writerRecord.split( "~" );

         if ( tokens.length == 4 ) {
            writerRecordCount++;
            oldMovieID = Integer.parseInt( tokens[ 0 ] );
            newMovieID = idtrans.getNewMovieID( connection, oldMovieID );
 //           System.out.println("Old ID = " + oldMovieID + " New ID: " + newMovieID);

            writerName = editForApostrophe( tokens[ 1 ] );
            writerUncredited = ( tokens[ 2 ] );
            writerNotes = editForApostrophe( tokens[ 3 ] );

        

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
                           writerName + "', '" +
                           writerNotes + "', '" +
                           writerUncredited + "', " +
                           "null)";

            if(newMovieID == -1) {
               writerErrors++;
            } else {
               insertTable( query );
               writerRowCount++;
            }
         }
         else {
            out.println( "=====================================================" );
            out.println( "ARRAY NOT CORRECT LENGTH FOR writer: " );
            out.println( writerRecord );
            out.println( "=====================================================" );
            writerErrors++;
         }

         
         readwriterRecord();

      }
      System.out.println( "WRITERS COMPLETE: " + writerRowCount + " ROWS" );
    
         
   }



   private void readwriterRecord() {
   

      try {
         writerRecord = writerIn.readLine();
      }
      catch( IOException ioe ) {
         System.out.println( "*************************************************" );
         System.out.println( "INPUT/OUTPUT ERROR HAS OCCURRED" );
         System.out.println( "THE INPUT FILE IS " + writerFileName );
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
       out.println( "TOTAL INPUT ROWS:             " + writerRecordCount );
       out.println( "TOTAL WRITERS ROWS:           " + writerRowCount );
       out.println( "WRITERS ROWS IN ERROR:        " + writerErrors );


       out.close();

       System.exit( 0 );

   }
   

   public static void main( String args[] )
   {
      String date = args[ 0 ];
      new WritersLoad02( date );
   }

  

}