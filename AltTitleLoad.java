import java.util.*;
import java.sql.*;
import java.io.*;


public class AltTitleLoad{

   private Connection connection;


//altTitle TABLE VARIABLES
   private int oldMovieID = 0;
   private int newMovieID = 0;
   private String altTitle = "";
   private static final String altTitleNotes = "";
   private int rank;

   private int previousID = 0;
   private int altTitleCount = 0;

   
   private int altTitleErrors = 0;
   private int altTitleRowCount = 0;

   private GregorianCalendar currentDate;

   private PrintWriter out;

   private BufferedReader altTitleIn;

   private File altTitleFile;

   private String altTitleRecord = "";

   private String altTitleFileName = "";


   private String date = "";

   private String schema = "movies";

   private String directory = 
      "C:\\Users\\Shane Dettmer\\OneDrive\\Documents\\Movies\\Backups\\";



   public AltTitleLoad( String inputDate )
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
               new FileWriter("film_altTitle_language Load Errors.txt")));

      }
      catch( IOException ioe ) {
         
         System.out.println( "**************************************" );
         System.out.println( "AN I/O EXCEPTION HAS OCCURRED " );
         System.out.println( "film_alternate_title Load Errors.txt" );
         System.out.println( "**************************************" );
         System.exit( 1 );
      }


      date = inputDate;

      try {

         altTitleFileName = directory + "ALTERNATE_TITLE " + date + ".txt";
         altTitleFile = new File( altTitleFileName );
         if( altTitleFile.exists() ) {
            altTitleIn = new BufferedReader(
                            new FileReader( altTitleFile ) );
            altTitleRecord = altTitleIn.readLine();
         }

      }
      catch( IOException ioe ) {
  
         System.out.println( "**************************************" );
         System.out.println( "AN I/O EXCEPTION HAS OCCURRED " );
         System.out.println( altTitleFileName );
         System.out.println( "**************************************" );
         System.exit( 1 );
      } 
      catch( NullPointerException npe ) {
         System.out.println( "**************************************" );
         System.out.println( "Null File: " );
         System.out.println( altTitleFileName );
         System.out.println( "**************************************" );
         System.exit( 1 );
      }

      altTitleLoad();


      exitProgram();

   }

   public void altTitleLoad()
   {

      IDTranslate idtrans = new IDTranslate();

      String tokens[];


      while ( altTitleRecord != null ) {
         tokens = altTitleRecord.split( "~" );

         if ( tokens.length == 2 ) {
            altTitleCount++;
            oldMovieID = Integer.parseInt( tokens[ 0 ] );
            newMovieID = idtrans.getNewMovieID( connection, oldMovieID );
//            System.out.println("Old ID = " + oldMovieID + " New ID: " + newMovieID);
 

            altTitle = editForApostrophe( tokens[ 1 ] );

            if(newMovieID == previousID) {
               rank++;
            } else {
               rank = 1;
               previousID = newMovieID;
            }


            String query = "INSERT INTO " + schema + ".film_alternate_title VALUES(" +
                           newMovieID + ", " +
                           rank + ",'" +
                           altTitle + "', '" +
                           altTitleNotes + "')";

            if(newMovieID == -1) {
               altTitleErrors++;
            } else {
               insertTable( query );
               altTitleRowCount++;
            }
         }
         else {
            out.println( "=====================================================" );
            out.println( "ARRAY NOT CORRECT LENGTH FOR altTitle: " );
            out.println( altTitleRecord );
            out.println( "=====================================================" );
            altTitleErrors++;
         }

         
         readaltTitleRecord();

      }
      System.out.println( "film_alternate_title COMPLETE: " + altTitleRowCount + " ROWS" );
    
         
   }



   private void readaltTitleRecord() {
   

      try {
         altTitleRecord = altTitleIn.readLine();
      }
      catch( IOException ioe ) {
         System.out.println( "*************************************************" );
         System.out.println( "INPUT/OUTPUT ERROR HAS OCCURRED" );
         System.out.println( "THE INPUT FILE IS " + altTitleFileName );
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
       out.println( "TOTAL INPUT ROWS:                    " + altTitleCount );
       out.println( "TOTAL film_altTitle_language ROWS:           " + altTitleRowCount );
       out.println( "film_altTitle_language ROWS IN ERROR:        " + altTitleErrors );


       out.close();

       System.exit( 0 );

   }
   

   public static void main( String args[] )
   {
      String date = args[ 0 ];
      new AltTitleLoad( date );
   }

  

}