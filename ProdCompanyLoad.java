import java.util.*;
import java.sql.*;
import java.io.*;


public class ProdCompanyLoad{

   private Connection connection;


//PRODCOMPANY TABLE VARIABLES
   private int oldMovieID = 0;
   private int newMovieID = 0;
   private int rank = 0;
   private String prodCompanyName = "";
   private static final String COMPANY_TYPE = "01";
   private static final String companyNotes = "";

   private int previousID = 0;
   private int prodRecordCount = 0;

   
   private int prodCompanyErrors = 0;
   private int prodCompanyRowCount = 0;

   private GregorianCalendar currentDate;

   private PrintWriter out;
   private PrintWriter idOut;

   private BufferedReader prodCompanyIn;

   private File prodCompanyFile;

   private String prodCompanyRecord = "";

   private String prodCompanyFileName = "";


   private String date = "";

   private String schema = "movies";

   private String directory = 
      "C:\\Users\\Shane Dettmer\\OneDrive\\Documents\\Movies\\Backups\\";



   public ProdCompanyLoad( String inputDate )
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
               new FileWriter("film_company Load Errors.txt")));

      }
      catch( IOException ioe ) {
         
         System.out.println( "**************************************" );
         System.out.println( "AN I/O EXCEPTION HAS OCCURRED " );
         System.out.println( "Prod Company Load Errors.txt" );
         System.out.println( "**************************************" );
         System.exit( 1 );
      }


      date = inputDate;

      try {

         prodCompanyFileName = directory + "PRODCOMPANY " + date + ".txt";
         prodCompanyFile = new File( prodCompanyFileName );
         if( prodCompanyFile.exists() ) {
            prodCompanyIn = new BufferedReader(
                            new FileReader( prodCompanyFile ) );
            prodCompanyRecord = prodCompanyIn.readLine();
         }

      }
      catch( IOException ioe ) {
  
         System.out.println( "**************************************" );
         System.out.println( "AN I/O EXCEPTION HAS OCCURRED " );
         System.out.println( prodCompanyFileName );
         System.out.println( "**************************************" );
         System.exit( 1 );
      } 
      catch( NullPointerException npe ) {
         System.out.println( "**************************************" );
         System.out.println( "Null File: " );
         System.out.println( prodCompanyFileName );
         System.out.println( "**************************************" );
         System.exit( 1 );
      }

      prodCompanyLoad();


      exitProgram();

   }

// THIS METHOD LOADS THE DATA TO THE TABLE
   public void prodCompanyLoad()
   {

      IDTranslate idtrans = new IDTranslate();

      String tokens[];


      while ( prodCompanyRecord != null ) {
         tokens = prodCompanyRecord.split( "~" );

         if ( tokens.length == 2 ) {
            prodRecordCount++;
            oldMovieID = Integer.parseInt( tokens[ 0 ] );
            newMovieID = idtrans.getNewMovieID( connection, oldMovieID );
            System.out.println("Old ID = " + oldMovieID + " New ID: " + newMovieID);
  //          newMovieID = getNewMovieID( oldMovieID);

            prodCompanyName = editForApostrophe( tokens[ 1 ] );

            if(newMovieID == previousID) {
               rank++;
            } else {
               rank = 1;
               previousID = newMovieID;
            }



            String query = "INSERT INTO " + schema + ".film_company VALUES(" +
                           newMovieID + ", '" +
                           COMPANY_TYPE + "', " +
                           rank + ", '" +
                           prodCompanyName + "', '" +
                           companyNotes + "')";

            if(newMovieID == -1) {
               prodCompanyErrors++;
            } else {
//               insertTable( query );
               prodCompanyRowCount++;
            }
         }
         else {
            out.println( "=====================================================" );
            out.println( "ARRAY NOT CORRECT LENGTH FOR PRODCOMPANY: " );
            out.println( prodCompanyRecord );
            out.println( "=====================================================" );
            prodCompanyErrors++;
         }

         
         readProdCompanyRecord();

      }
      System.out.println( "film_company COMPLETE: " + prodCompanyRowCount + " ROWS" );
    
         
   }



   private void readProdCompanyRecord() {
   

      try {
         prodCompanyRecord = prodCompanyIn.readLine();
      }
      catch( IOException ioe ) {
         System.out.println( "*************************************************" );
         System.out.println( "INPUT/OUTPUT ERROR HAS OCCURRED" );
         System.out.println( "THE INPUT FILE IS " + prodCompanyFileName );
         ioe.printStackTrace();
         System.out.println( "*************************************************" );
         System.exit( 1 );
      }
   }

/*
   public int getNewMovieID( int ID ) {

      String query = "SELECT new_id FROM movies.id_translation WHERE old_id=" + ID;
      int newID = 0;

      Statement statement = null;
      ResultSet results = null;

      try {
         statement = connection.createStatement();
         results = statement.executeQuery( query );

         if( results.next() ) {
            do {
               newID = results.getInt( 1 );
            } while( results.next() );
            return newID;

         }
      } catch(SQLException e) {
         System.out.println("SQL Exception Occurred...");
         System.out.println("SQLState: " + e.getSQLState());
         System.out.println(e.getStackTrace());
      } finally {
         try {
            if(statement != null) {
               statement.close();
            }
            if(results != null) {
               results.close();
            }
         } catch(SQLException e2) {
            System.out.println("Error closing resources...");
            System.out.println(e2.getMessage());
            System.out.println(e2.getStackTrace());
         }
      }
      return -1;
   }
*/

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
       out.println( "TOTAL INPUT ROWS:                    " + prodRecordCount );
       out.println( "TOTAL film_company ROWS:           " + prodCompanyRowCount );
       out.println( "film_company ROWS IN ERROR:        " + prodCompanyErrors );


       out.close();

       System.exit( 0 );

   }
   

   public static void main( String args[] )
   {
      String date = args[ 0 ];
      new ProdCompanyLoad( date );
   }

  

}