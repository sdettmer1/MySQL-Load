import java.util.*;
import java.sql.*;
import java.io.*;

/**
* This class adds data from a ~ delimited text file to the film_company
* table (type = 2) in the movies database
*
* @author Shane Dettmer
* @version 1.1
* @since 2022-08-29
*/
public class DistCompanyLoad{

   private Connection connection;


//distCompany TABLE VARIABLES
   private int oldMovieID = 0;
   private int newMovieID = 0;
   private int rank = 0;
   private String distCompanyName = "";
   private static final int COMPANY_TYPE = 2;
   private static final String companyNotes = "";

   private int previousID = 0;
   private int distRecordCount = 0;

   
   private int distCompanyErrors = 0;
   private int distCompanyRowCount = 0;

   private GregorianCalendar currentDate;

   private PrintWriter out;

   private BufferedReader distCompanyIn;

   private File distCompanyFile;

   private String distCompanyRecord = "";

   private String distCompanyFileName = "";


   private String date = "";

   private String schema = "movies";

   private String directory = 
      "C:\\Users\\Shane Dettmer\\OneDrive\\Documents\\Movies\\Backups\\";


   /**
   * Accepts the input date of the text file
   * from the command line.  It will establish the MySQL connection
   * create the needed files and initiate the loading process
   *
   * @param inputDate This is the date of the text file entered on command line
   */
   public DistCompanyLoad( String inputDate )
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
         System.out.println( "Dist Company Load Errors.txt" );
         System.out.println( "**************************************" );
         System.exit( 1 );
      }


      date = inputDate;

      try {

         distCompanyFileName = directory + "DISTCOMPANY " + date + ".txt";
         distCompanyFile = new File( distCompanyFileName );
         if( distCompanyFile.exists() ) {
            distCompanyIn = new BufferedReader(
                            new FileReader( distCompanyFile ) );
            distCompanyRecord = distCompanyIn.readLine();
         }

      }
      catch( IOException ioe ) {
  
         System.out.println( "**************************************" );
         System.out.println( "AN I/O EXCEPTION HAS OCCURRED " );
         System.out.println( distCompanyFileName );
         System.out.println( "**************************************" );
         System.exit( 1 );
      } 
      catch( NullPointerException npe ) {
         System.out.println( "**************************************" );
         System.out.println( "Null File: " );
         System.out.println( distCompanyFileName );
         System.out.println( "**************************************" );
         System.exit( 1 );
      }

      distCompanyLoad();


      exitProgram();

   }

   /**
   * This method loops through the text file, extracting the 
   * data from each record and then INSERTs a new record into the 
   * table.
   * 
   */
   public void distCompanyLoad()
   {

      IDTranslate idtrans = new IDTranslate();

      String tokens[];


      while ( distCompanyRecord != null ) {
         tokens = distCompanyRecord.split( "~" );

         if ( tokens.length == 2 ) {
            distRecordCount++;
            oldMovieID = Integer.parseInt( tokens[ 0 ] );
            newMovieID = idtrans.getNewMovieID( connection, oldMovieID );
//            System.out.println("Old ID = " + oldMovieID + " New ID: " + newMovieID);
 

            distCompanyName = editForApostrophe( tokens[ 1 ] );

            if(newMovieID == previousID) {
               rank++;
            } else {
               rank = 1;
               previousID = newMovieID;
            }



            String query = "INSERT INTO " + schema + ".film_company VALUES(" +
                           newMovieID + ", " +
                           COMPANY_TYPE + ", " +
                           rank + ", '" +
                           distCompanyName + "', '" +
                           companyNotes + "')";

            if(newMovieID == -1) {
               distCompanyErrors++;
            } else {
               insertTable( query );
               distCompanyRowCount++;
            }
         }
         else {
            out.println( "=====================================================" );
            out.println( "ARRAY NOT CORRECT LENGTH FOR distCompany: " );
            out.println( distCompanyRecord );
            out.println( "=====================================================" );
            distCompanyErrors++;
         }

         
         readdistCompanyRecord();

      }
      System.out.println( "film_company COMPLETE: " + distCompanyRowCount + " ROWS" );
    
         
   }


   /**
   * Reads next record from input file
   *
   */
   private void readdistCompanyRecord() {
   

      try {
         distCompanyRecord = distCompanyIn.readLine();
      }
      catch( IOException ioe ) {
         System.out.println( "*************************************************" );
         System.out.println( "INPUT/OUTPUT ERROR HAS OCCURRED" );
         System.out.println( "THE INPUT FILE IS " + distCompanyFileName );
         ioe.printStackTrace();
         System.out.println( "*************************************************" );
         System.exit( 1 );
      }
   }

   /**
   * Inserts a record into the database table
   *
   *@param query SQL statement that will be executed
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

   /**
   * Searches for an apostrophe in a given String and then
   * adds a new apostrophe next to it to prevent SQL errors
   * 
   * @param name the String to be edited
   * @return String Returms the edited version of the input String
   */
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

   /**
   * Writes control totals to the output file and
   * closes all resources and exits the program
   *
   */
   public void exitProgram()
   {



       out.println( "MOVIE LIST TABLE RELOAD TOTALS: " );
       out.println( "-------------------------- " );
       out.println( "                           " );
       out.println( "TOTAL INPUT ROWS:                    " + distRecordCount );
       out.println( "TOTAL film_company ROWS:           " + distCompanyRowCount );
       out.println( "film_company ROWS IN ERROR:        " + distCompanyErrors );


       out.close();

      try {
         connection.close();
      } catch(SQLException e) {
         System.out.println( "Error occurred when closing SQL connection:" );
         System.out.println( "Error Code " + e.getErrorCode() );
         System.out.println( "SQL State: " + e.getSQLState() );
         e.printStackTrace();
         System.exit(-1);
      }

       System.exit( 0 );

   }
   
   /**
   * Creates an instance of the class and runs the application
   * One argument is necessary on the command line
   *
   * @param args The date portion of the file name in yyyy-mm-dd format
   */
   public static void main( String args[] )
   {
      String date = args[ 0 ];
      new DistCompanyLoad( date );
   }

  

}