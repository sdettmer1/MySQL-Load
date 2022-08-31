import java.util.*;
import java.sql.*;
import java.io.*;


public class CountryLoad{

   private Connection connection;


//country TABLE VARIABLES
   private int oldMovieID = 0;
   private int newMovieID = 0;
   private int rank = 0;
   private String countryName = "";
   private static final int ENTITY_TYPE = 1;
   private static final String countryNotes = "";

   private int previousID = 0;
   private int countryCount = 0;

   
   private int countryErrors = 0;
   private int countryRowCount = 0;

   private GregorianCalendar currentDate;

   private PrintWriter out;

   private BufferedReader countryIn;

   private File countryFile;

   private String countryRecord = "";

   private String countryFileName = "";


   private String date = "";

   private String schema = "movies";

   private String directory = 
      "C:\\Users\\Shane Dettmer\\OneDrive\\Documents\\Movies\\Backups\\";



   public CountryLoad( String inputDate )
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
               new FileWriter("film_country_language Load Errors.txt")));

      }
      catch( IOException ioe ) {
         
         System.out.println( "**************************************" );
         System.out.println( "AN I/O EXCEPTION HAS OCCURRED " );
         System.out.println( "film_country_language Load Errors.txt" );
         System.out.println( "**************************************" );
         System.exit( 1 );
      }


      date = inputDate;

      try {

         countryFileName = directory + "country " + date + ".txt";
         countryFile = new File( countryFileName );
         if( countryFile.exists() ) {
            countryIn = new BufferedReader(
                            new FileReader( countryFile ) );
            countryRecord = countryIn.readLine();
         }

      }
      catch( IOException ioe ) {
  
         System.out.println( "**************************************" );
         System.out.println( "AN I/O EXCEPTION HAS OCCURRED " );
         System.out.println( countryFileName );
         System.out.println( "**************************************" );
         System.exit( 1 );
      } 
      catch( NullPointerException npe ) {
         System.out.println( "**************************************" );
         System.out.println( "Null File: " );
         System.out.println( countryFileName );
         System.out.println( "**************************************" );
         System.exit( 1 );
      }

      countryLoad();


      exitProgram();

   }

   public void countryLoad()
   {

      IDTranslate idtrans = new IDTranslate();

      String tokens[];


      while ( countryRecord != null ) {
         tokens = countryRecord.split( "~" );

         if ( tokens.length == 2 ) {
            countryCount++;
            oldMovieID = Integer.parseInt( tokens[ 0 ] );
            newMovieID = idtrans.getNewMovieID( connection, oldMovieID );
//            System.out.println("Old ID = " + oldMovieID + " New ID: " + newMovieID);
 

            countryName = editForApostrophe( tokens[ 1 ] );

            if(newMovieID == previousID) {
               rank++;
            } else {
               rank = 1;
               previousID = newMovieID;
            }



            String query = "INSERT INTO " + schema + ".film_country_language VALUES(" +
                           newMovieID + ", " +
                           ENTITY_TYPE + ", " +
                           rank + ", '" +
                           countryName + "', '" +
                           countryNotes + "')";

            if(newMovieID == -1) {
               countryErrors++;
            } else {
               insertTable( query );
               countryRowCount++;
            }
         }
         else {
            out.println( "=====================================================" );
            out.println( "ARRAY NOT CORRECT LENGTH FOR COUNTRY: " );
            out.println( countryRecord );
            out.println( "=====================================================" );
            countryErrors++;
         }

         
         readCountryRecord();

      }
      System.out.println( "film_country_language COMPLETE: " + countryRowCount + " ROWS" );
    
         
   }



   private void readCountryRecord() {
   

      try {
         countryRecord = countryIn.readLine();
      }
      catch( IOException ioe ) {
         System.out.println( "*************************************************" );
         System.out.println( "INPUT/OUTPUT ERROR HAS OCCURRED" );
         System.out.println( "THE INPUT FILE IS " + countryFileName );
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
       out.println( "TOTAL INPUT ROWS:                    " + countryCount );
       out.println( "TOTAL film_country_language ROWS:           " + countryRowCount );
       out.println( "film_country_language ROWS IN ERROR:        " + countryErrors );


       out.close();

       System.exit( 0 );

   }
   

   public static void main( String args[] )
   {
      String date = args[ 0 ];
      new CountryLoad( date );
   }

  

}