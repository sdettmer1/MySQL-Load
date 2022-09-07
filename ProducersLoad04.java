import java.util.*;
import java.sql.*;
import java.io.*;


public class ProducersLoad04 {

   private Connection connection;


//producers TABLE VARIABLES
   private int oldMovieID = 0;
   private int newMovieID = 0;
   private int rank = 0;
   private String producersName = "";
   private String producersNotes = "";
   private String producersUncredited;
   private static final int PERSON_TYPE = 4;

   private int previousID = 0;
   private int producersRecordCount = 0;

   
   private int producersErrors = 0;
   private int producersRowCount = 0;

   private GregorianCalendar currentDate;

   private PrintWriter out;
   private PrintWriter idOut;

   private BufferedReader producersIn;

   private File producersFile;

   private String producersRecord = "";

   private String producersFileName = "";


   private String date = "";

   private String schema = "movies";

   private String directory = 
      "C:\\Users\\Shane Dettmer\\OneDrive\\Documents\\Movies\\Backups\\";



   public ProducersLoad04( String inputDate )
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
               new FileWriter("PRODUCERS Load Errors.txt")));

      }
      catch( IOException ioe ) {
         
         System.out.println( "**************************************" );
         System.out.println( "AN I/O EXCEPTION HAS OCCURRED " );
         System.out.println( "PRODUCERS Load Errors.txt" );
         System.out.println( "**************************************" );
         System.exit( 1 );
      }


      date = inputDate;

      try {

         producersFileName = directory + "PRODUCER " + date + ".txt";
         producersFile = new File( producersFileName );
         if( producersFile.exists() ) {
            producersIn = new BufferedReader(
                            new FileReader( producersFile ) );
            producersRecord = producersIn.readLine();
         }

      }
      catch( IOException ioe ) {
  
         System.out.println( "**************************************" );
         System.out.println( "AN I/O EXCEPTION HAS OCCURRED " );
         System.out.println( producersFileName );
         System.out.println( "**************************************" );
         System.exit( 1 );
      } 
      catch( NullPointerException npe ) {
         System.out.println( "**************************************" );
         System.out.println( "Null File: " );
         System.out.println( producersFileName );
         System.out.println( "**************************************" );
         System.exit( 1 );
      }

      producersLoad();


      exitProgram();

   }

// THIS METHOD LOADS THE DATA TO THE TABLE
   public void producersLoad()
   {

      IDTranslate idtrans = new IDTranslate();

      String tokens[];


      while ( producersRecord != null ) {
         tokens = producersRecord.split( "~" );

         if ( tokens.length == 5 ) {
            producersRecordCount++;
            oldMovieID = Integer.parseInt( tokens[ 0 ] );
            newMovieID = idtrans.getNewMovieID( connection, oldMovieID );
 //           System.out.println("Old ID = " + oldMovieID + " New ID: " + newMovieID);

            producersName = editForApostrophe( tokens[ 1 ] );
            producersUncredited = ( tokens[ 2 ] );
            producersNotes = editForApostrophe( tokens[ 3 ] );
            rank = Integer.parseInt( tokens[ 4 ] );



            String query = "INSERT INTO " + schema + ".film_person VALUES(" +
                           newMovieID + ", " +
                           PERSON_TYPE + ", " +
                           rank + ", '" +
                           producersName + "', '" +
                           producersNotes + "', '" +
                           producersUncredited + "', " +
                           "null)";

            if(newMovieID == -1) {
               producersErrors++;
            } else {
               insertTable( query );
               producersRowCount++;
            }
         }
         else {
            out.println( "=====================================================" );
            out.println( "ARRAY NOT CORRECT LENGTH FOR PRODUCERS: " );
            out.println( producersRecord );
            out.println( "=====================================================" );
            producersErrors++;
         }

         
         readproducersRecord();

      }
      System.out.println( "PRODUCERS COMPLETE: " + producersRowCount + " ROWS" );
    
         
   }



   private void readproducersRecord() {
   

      try {
         producersRecord = producersIn.readLine();
      }
      catch( IOException ioe ) {
         System.out.println( "*************************************************" );
         System.out.println( "INPUT/OUTPUT ERROR HAS OCCURRED" );
         System.out.println( "THE INPUT FILE IS " + producersFileName );
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
       out.println( "TOTAL INPUT ROWS:               " + producersRecordCount );
       out.println( "TOTAL PRODUCER ROWS:            " + producersRowCount );
       out.println( "PRODUCER ROWS IN ERROR:         " + producersErrors );


       out.close();

       System.exit( 0 );

   }
   

   public static void main( String args[] )
   {
      String date = args[ 0 ];
      new ProducersLoad04( date );
   }

  

}