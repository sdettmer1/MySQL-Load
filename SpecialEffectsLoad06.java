import java.util.*;
import java.sql.*;
import java.io.*;


public class SpecialEffectsLoad06 {

   private Connection connection;


//specialEffects TABLE VARIABLES
   private int oldMovieID = 0;
   private int newMovieID = 0;
   private int rank = 0;
   private String specialEffectsName = "";
   private String specialEffectsUncredited;
   private String specialEffectsNotes = "";
   private static final int PERSON_TYPE = 6;

   private int previousID = 0;
   private int specialEffectsRecordCount = 0;

   
   private int specialEffectsErrors = 0;
   private int specialEffectsRowCount = 0;

   private GregorianCalendar currentDate;

   private PrintWriter out;
   private PrintWriter idOut;

   private BufferedReader specialEffectsIn;

   private File specialEffectsFile;

   private String specialEffectsRecord = "";

   private String specialEffectsFileName = "";


   private String date = "";

   private String schema = "movies";

   private String directory = 
      "C:\\Users\\Shane Dettmer\\OneDrive\\Documents\\Movies\\Backups\\";



   public SpecialEffectsLoad06( String inputDate )
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
               new FileWriter("SPECIAL_EFFECTS Load Errors.txt")));

      }
      catch( IOException ioe ) {
         
         System.out.println( "**************************************" );
         System.out.println( "AN I/O EXCEPTION HAS OCCURRED " );
         System.out.println( "SPECIAL_EFFECTS Load Errors.txt" );
         System.out.println( "**************************************" );
         System.exit( 1 );
      }


      date = inputDate;

      try {

         specialEffectsFileName = directory + "SPECIAL_EFFECTS " + date + ".txt";
         specialEffectsFile = new File( specialEffectsFileName );
         if( specialEffectsFile.exists() ) {
            specialEffectsIn = new BufferedReader(
                            new FileReader( specialEffectsFile ) );
            specialEffectsRecord = specialEffectsIn.readLine();
         }

      }
      catch( IOException ioe ) {
  
         System.out.println( "**************************************" );
         System.out.println( "AN I/O EXCEPTION HAS OCCURRED " );
         System.out.println( specialEffectsFileName );
         System.out.println( "**************************************" );
         System.exit( 1 );
      } 
      catch( NullPointerException npe ) {
         System.out.println( "**************************************" );
         System.out.println( "Null File: " );
         System.out.println( specialEffectsFileName );
         System.out.println( "**************************************" );
         System.exit( 1 );
      }

      specialEffectsLoad();


      exitProgram();

   }

// THIS METHOD LOADS THE DATA TO THE TABLE
   public void specialEffectsLoad()
   {

      IDTranslate idtrans = new IDTranslate();

      String tokens[];


      while ( specialEffectsRecord != null ) {
         tokens = specialEffectsRecord.split( "~" );

         if ( tokens.length == 4 ) {
            specialEffectsRecordCount++;
            oldMovieID = Integer.parseInt( tokens[ 0 ] );
            newMovieID = idtrans.getNewMovieID( connection, oldMovieID );
 //           System.out.println("Old ID = " + oldMovieID + " New ID: " + newMovieID);

            specialEffectsName = editForApostrophe( tokens[ 1 ] );
            specialEffectsUncredited = ( tokens[ 2 ] );
            specialEffectsNotes = editForApostrophe( tokens[ 3 ] );

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
                           specialEffectsName + "', '" +
                           specialEffectsNotes + "', '" +
                           specialEffectsUncredited + "', " +
                           "null)";

            if(newMovieID == -1) {
               specialEffectsErrors++;
            } else {
               insertTable( query );
               specialEffectsRowCount++;
            }
         }
         else {
            out.println( "=====================================================" );
            out.println( "ARRAY NOT CORRECT LENGTH FOR specialEffects: " );
            out.println( specialEffectsRecord );
            out.println( "=====================================================" );
            specialEffectsErrors++;
         }

         
         readspecialEffectsRecord();

      }
      System.out.println( "SPECIAL_EFFECTS COMPLETE: " + specialEffectsRowCount + " ROWS" );
    
         
   }



   private void readspecialEffectsRecord() {
   

      try {
         specialEffectsRecord = specialEffectsIn.readLine();
      }
      catch( IOException ioe ) {
         System.out.println( "*************************************************" );
         System.out.println( "INPUT/OUTPUT ERROR HAS OCCURRED" );
         System.out.println( "THE INPUT FILE IS " + specialEffectsFileName );
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
       out.println( "TOTAL INPUT ROWS:               " + specialEffectsRecordCount );
       out.println( "TOTAL SPECIAL_EFFECTS ROWS:     " + specialEffectsRowCount );
       out.println( "SPECIAL_EFFECTS ROWS IN ERROR:  " + specialEffectsErrors );


       out.close();

       System.exit( 0 );

   }
   

   public static void main( String args[] )
   {
      String date = args[ 0 ];
      new SpecialEffectsLoad06( date );
   }

  

}