import java.util.*;
import java.sql.*;
import java.io.*;


public class ProductionDesignerLoad12 {

   private Connection connection;


//MUSICAL_DIRECTOR TABLE VARIABLES
   private int oldMovieID = 0;
   private int newMovieID = 0;
   private int rank = 0;
   private String productionDesignerName = "";
   private String productionDesignerUncredited;
   private String productionDesignerNotes = "";
   private static final int PERSON_TYPE = 12;

   private int previousID = 0;
   private int productionDesignerRecordCount = 0;

   
   private int productionDesignerErrors = 0;
   private int productionDesignerRowCount = 0;

   private GregorianCalendar currentDate;

   private PrintWriter out;
   private PrintWriter idOut;

   private BufferedReader productionDesignerIn;

   private File productionDesignerFile;

   private String productionDesignerRecord = "";

   private String productionDesignerFileName = "";


   private String date = "";

   private String schema = "movies";

   private String directory = 
      "C:\\Users\\Shane Dettmer\\OneDrive\\Documents\\Movies\\Backups\\";



   public ProductionDesignerLoad12( String inputDate )
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
               new FileWriter("PROD_DESIGNER Load Errors.txt")));

      }
      catch( IOException ioe ) {
         
         System.out.println( "**************************************" );
         System.out.println( "AN I/O EXCEPTION HAS OCCURRED " );
         System.out.println( "PROD_DESIGNER Load Errors.txt" );
         System.out.println( "**************************************" );
         System.exit( 1 );
      }


      date = inputDate;

      try {

         productionDesignerFileName = directory + "PROD_DESIGNER " + date + ".txt";
         productionDesignerFile = new File( productionDesignerFileName );
         if( productionDesignerFile.exists() ) {
            productionDesignerIn = new BufferedReader(
                            new FileReader( productionDesignerFile ) );
            productionDesignerRecord = productionDesignerIn.readLine();
         }

      }
      catch( IOException ioe ) {
  
         System.out.println( "**************************************" );
         System.out.println( "AN I/O EXCEPTION HAS OCCURRED " );
         System.out.println( productionDesignerFileName );
         System.out.println( "**************************************" );
         System.exit( 1 );
      } 
      catch( NullPointerException npe ) {
         System.out.println( "**************************************" );
         System.out.println( "Null File: " );
         System.out.println( productionDesignerFileName );
         System.out.println( "**************************************" );
         System.exit( 1 );
      }

      productionDesignerLoad();


      exitProgram();

   }

// THIS METHOD LOADS THE DATA TO THE TABLE
   public void productionDesignerLoad()
   {

      IDTranslate idtrans = new IDTranslate();

      String tokens[];


      while ( productionDesignerRecord != null ) {
         tokens = productionDesignerRecord.split( "~" );

         if ( tokens.length == 4 ) {
            productionDesignerRecordCount++;
            oldMovieID = Integer.parseInt( tokens[ 0 ] );
            newMovieID = idtrans.getNewMovieID( connection, oldMovieID );
 //           System.out.println("Old ID = " + oldMovieID + " New ID: " + newMovieID);

            productionDesignerName = editForApostrophe( tokens[ 1 ] );
            productionDesignerUncredited = ( tokens[ 2 ] );
            productionDesignerNotes = editForApostrophe( tokens[ 3 ] );

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
                           productionDesignerName + "', '" +
                           productionDesignerNotes + "', '" +
                           productionDesignerUncredited + "', " +
                           "null)";

            if(newMovieID == -1) {
               productionDesignerErrors++;
            } else {
               insertTable( query );
               productionDesignerRowCount++;
            }
         }
         else {
            out.println( "=====================================================" );
            out.println( "ARRAY NOT CORRECT LENGTH FOR PROD_DESIGNER: " );
            out.println( productionDesignerRecord );
            out.println( "=====================================================" );
            productionDesignerErrors++;
         }

         
         readproductionDesignerRecord();

      }
      System.out.println( "productionDesigner COMPLETE: " + productionDesignerRowCount + " ROWS" );
    
         
   }



   private void readproductionDesignerRecord() {
   

      try {
         productionDesignerRecord = productionDesignerIn.readLine();
      }
      catch( IOException ioe ) {
         System.out.println( "*************************************************" );
         System.out.println( "INPUT/OUTPUT ERROR HAS OCCURRED" );
         System.out.println( "THE INPUT FILE IS " + productionDesignerFileName );
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
       out.println( "TOTAL INPUT ROWS:               " + productionDesignerRecordCount );
       out.println( "TOTAL PROD_DESIGNER ROWS:       " + productionDesignerRowCount );
       out.println( "PROD_DESIGNER ROWS IN ERROR:    " + productionDesignerErrors );


       out.close();

       System.exit( 0 );

   }
   

   public static void main( String args[] )
   {
      String date = args[ 0 ];
      new ProductionDesignerLoad12( date );
   }

  

}