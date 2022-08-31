import java.util.*;
import java.sql.*;
import java.io.*;


public class LanguageLoad{

   private Connection connection;


//language TABLE VARIABLES
   private int oldMovieID = 0;
   private int newMovieID = 0;
   private int rank = 0;
   private String languageName = "";
   private static final int ENTITY_TYPE = 2;
   private static final String languageNotes = "";

   private int previousID = 0;
   private int languageCount = 0;

   
   private int languageErrors = 0;
   private int languageRowCount = 0;

   private GregorianCalendar currentDate;

   private PrintWriter out;

   private BufferedReader languageIn;

   private File languageFile;

   private String languageRecord = "";

   private String languageFileName = "";


   private String date = "";

   private String schema = "movies";

   private String directory = 
      "C:\\Users\\Shane Dettmer\\OneDrive\\Documents\\Movies\\Backups\\";



   public LanguageLoad( String inputDate )
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
               new FileWriter("film_language_language Load Errors.txt")));

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

         languageFileName = directory + "LANGUAGE " + date + ".txt";
         languageFile = new File( languageFileName );
         if( languageFile.exists() ) {
            languageIn = new BufferedReader(
                            new FileReader( languageFile ) );
            languageRecord = languageIn.readLine();
         }

      }
      catch( IOException ioe ) {
  
         System.out.println( "**************************************" );
         System.out.println( "AN I/O EXCEPTION HAS OCCURRED " );
         System.out.println( languageFileName );
         System.out.println( "**************************************" );
         System.exit( 1 );
      } 
      catch( NullPointerException npe ) {
         System.out.println( "**************************************" );
         System.out.println( "Null File: " );
         System.out.println( languageFileName );
         System.out.println( "**************************************" );
         System.exit( 1 );
      }

      loadLanguage();


      exitProgram();

   }

   public void loadLanguage()
   {

      IDTranslate idtrans = new IDTranslate();

      String tokens[];


      while ( languageRecord != null ) {
         tokens = languageRecord.split( "~" );

         if ( tokens.length == 2 ) {
            languageCount++;
            oldMovieID = Integer.parseInt( tokens[ 0 ] );
            newMovieID = idtrans.getNewMovieID( connection, oldMovieID );
//            System.out.println("Old ID = " + oldMovieID + " New ID: " + newMovieID);
 

            languageName = editForApostrophe( tokens[ 1 ] );

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
                           languageName + "', '" +
                           languageNotes + "')";

            if(newMovieID == -1) {
               languageErrors++;
            } else {
               insertTable( query );
               languageRowCount++;
            }
         }
         else {
            out.println( "=====================================================" );
            out.println( "ARRAY NOT CORRECT LENGTH FOR language: " );
            out.println( languageRecord );
            out.println( "=====================================================" );
            languageErrors++;
         }

         
         readLanguageRecord();

      }
      System.out.println( "film_language_language COMPLETE: " + languageRowCount + " ROWS" );
    
         
   }



   private void readLanguageRecord() {
   

      try {
         languageRecord = languageIn.readLine();
      }
      catch( IOException ioe ) {
         System.out.println( "*************************************************" );
         System.out.println( "INPUT/OUTPUT ERROR HAS OCCURRED" );
         System.out.println( "THE INPUT FILE IS " + languageFileName );
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



       out.println( "film_country_language TABLE RELOAD TOTALS: " );
       out.println( "-------------------------- " );
       out.println( "                           " );
       out.println( "TOTAL INPUT ROWS:                    " + languageCount );
       out.println( "TOTAL film_language_language ROWS:           " + languageRowCount );
       out.println( "film_language_language ROWS IN ERROR:        " + languageErrors );


       out.close();

       System.exit( 0 );

   }
   

   public static void main( String args[] )
   {
      String date = args[ 0 ];
      new LanguageLoad( date );
   }

  

}