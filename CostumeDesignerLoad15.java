import java.util.*;
import java.sql.*;
import java.io.*;


public class CostumeDesignerLoad15 {

   private Connection connection;


//MUSICAL_DIRECTOR TABLE VARIABLES
   private int oldMovieID = 0;
   private int newMovieID = 0;
   private int rank = 0;
   private String costumeDesignerName = "";
   private String costumeDesignerUncredited;
   private String costumeDesignerNotes = "";
   private static final int PERSON_TYPE = 15;

   private int previousID = 0;
   private int costumeDesignerRecordCount = 0;

   
   private int costumeDesignerErrors = 0;
   private int costumeDesignerRowCount = 0;

   private GregorianCalendar currentDate;

   private PrintWriter out;
   private PrintWriter idOut;

   private BufferedReader costumeDesignerIn;

   private File costumeDesignerFile;

   private String costumeDesignerRecord = "";

   private String costumeDesignerFileName = "";


   private String date = "";

   private String schema = "movies";

   private String directory = 
      "C:\\Users\\Shane Dettmer\\OneDrive\\Documents\\Movies\\Backups\\";



   public CostumeDesignerLoad15( String inputDate )
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
               new FileWriter("COSTUME_DESIGNER Load Errors.txt")));

      }
      catch( IOException ioe ) {
         
         System.out.println( "**************************************" );
         System.out.println( "AN I/O EXCEPTION HAS OCCURRED " );
         System.out.println( "COSTUME_DESIGNER Load Errors.txt" );
         System.out.println( "**************************************" );
         System.exit( 1 );
      }


      date = inputDate;

      try {

         costumeDesignerFileName = directory + "COSTUME_DESIGNER " + date + ".txt";
         costumeDesignerFile = new File( costumeDesignerFileName );
         if( costumeDesignerFile.exists() ) {
            costumeDesignerIn = new BufferedReader(
                            new FileReader( costumeDesignerFile ) );
            costumeDesignerRecord = costumeDesignerIn.readLine();
         }

      }
      catch( IOException ioe ) {
  
         System.out.println( "**************************************" );
         System.out.println( "AN I/O EXCEPTION HAS OCCURRED " );
         System.out.println( costumeDesignerFileName );
         System.out.println( "**************************************" );
         System.exit( 1 );
      } 
      catch( NullPointerException npe ) {
         System.out.println( "**************************************" );
         System.out.println( "Null File: " );
         System.out.println( costumeDesignerFileName );
         System.out.println( "**************************************" );
         System.exit( 1 );
      }

      costumeDesignerLoad();


      exitProgram();

   }

// THIS METHOD LOADS THE DATA TO THE TABLE
   public void costumeDesignerLoad()
   {

      IDTranslate idtrans = new IDTranslate();

      String tokens[];


      while ( costumeDesignerRecord != null ) {
         tokens = costumeDesignerRecord.split( "~" );

         if ( tokens.length == 4 ) {
            costumeDesignerRecordCount++;
            oldMovieID = Integer.parseInt( tokens[ 0 ] );
            newMovieID = idtrans.getNewMovieID( connection, oldMovieID );
 //           System.out.println("Old ID = " + oldMovieID + " New ID: " + newMovieID);

            costumeDesignerName = editForApostrophe( tokens[ 1 ] );
            costumeDesignerUncredited = ( tokens[ 2 ] );
            costumeDesignerNotes = editForApostrophe( tokens[ 3 ] );

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
                           costumeDesignerName + "', '" +
                           costumeDesignerNotes + "', '" +
                           costumeDesignerUncredited + "', " +
                           "null)";

            if(newMovieID == -1) {
               costumeDesignerErrors++;
            } else {
               insertTable( query );
               costumeDesignerRowCount++;
            }
         }
         else {
            out.println( "=====================================================" );
            out.println( "ARRAY NOT CORRECT LENGTH FOR COSTUME_DESIGNER: " );
            out.println( costumeDesignerRecord );
            out.println( "=====================================================" );
            costumeDesignerErrors++;
         }

         
         readcostumeDesignerRecord();

      }
      System.out.println( "COSTUME_DESIGNER COMPLETE: " + costumeDesignerRowCount + " ROWS" );
    
         
   }



   private void readcostumeDesignerRecord() {
   

      try {
         costumeDesignerRecord = costumeDesignerIn.readLine();
      }
      catch( IOException ioe ) {
         System.out.println( "*************************************************" );
         System.out.println( "INPUT/OUTPUT ERROR HAS OCCURRED" );
         System.out.println( "THE INPUT FILE IS " + costumeDesignerFileName );
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
       out.println( "TOTAL INPUT ROWS:                  " + costumeDesignerRecordCount );
       out.println( "TOTAL COSTUME_DESIGNER ROWS:       " + costumeDesignerRowCount );
       out.println( "COSTUME_DESIGNER ROWS IN ERROR:    " + costumeDesignerErrors );


       out.close();

       System.exit( 0 );

   }
   

   public static void main( String args[] )
   {
      String date = args[ 0 ];
      new CostumeDesignerLoad15( date );
   }

  

}