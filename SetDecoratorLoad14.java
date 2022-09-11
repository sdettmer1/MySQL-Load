import java.util.*;
import java.sql.*;
import java.io.*;


public class SetDecoratorLoad14 {

   private Connection connection;


//MUSICAL_DIRECTOR TABLE VARIABLES
   private int oldMovieID = 0;
   private int newMovieID = 0;
   private int rank = 0;
   private String setDecoratorName = "";
   private String setDecoratorUncredited;
   private String setDecoratorNotes = "";
   private static final int PERSON_TYPE = 14;

   private int previousID = 0;
   private int setDecoratorRecordCount = 0;

   
   private int setDecoratorErrors = 0;
   private int setDecoratorRowCount = 0;

   private GregorianCalendar currentDate;

   private PrintWriter out;
   private PrintWriter idOut;

   private BufferedReader setDecoratorIn;

   private File setDecoratorFile;

   private String setDecoratorRecord = "";

   private String setDecoratorFileName = "";


   private String date = "";

   private String schema = "movies";

   private String directory = 
      "C:\\Users\\Shane Dettmer\\OneDrive\\Documents\\Movies\\Backups\\";



   public SetDecoratorLoad14( String inputDate )
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
               new FileWriter("SET_DECORATOR Load Errors.txt")));

      }
      catch( IOException ioe ) {
         
         System.out.println( "**************************************" );
         System.out.println( "AN I/O EXCEPTION HAS OCCURRED " );
         System.out.println( "SET_DECORATOR Load Errors.txt" );
         System.out.println( "**************************************" );
         System.exit( 1 );
      }


      date = inputDate;

      try {

         setDecoratorFileName = directory + "SET_DECORATOR " + date + ".txt";
         setDecoratorFile = new File( setDecoratorFileName );
         if( setDecoratorFile.exists() ) {
            setDecoratorIn = new BufferedReader(
                            new FileReader( setDecoratorFile ) );
            setDecoratorRecord = setDecoratorIn.readLine();
         }

      }
      catch( IOException ioe ) {
  
         System.out.println( "**************************************" );
         System.out.println( "AN I/O EXCEPTION HAS OCCURRED " );
         System.out.println( setDecoratorFileName );
         System.out.println( "**************************************" );
         System.exit( 1 );
      } 
      catch( NullPointerException npe ) {
         System.out.println( "**************************************" );
         System.out.println( "Null File: " );
         System.out.println( setDecoratorFileName );
         System.out.println( "**************************************" );
         System.exit( 1 );
      }

      setDecoratorLoad();


      exitProgram();

   }

// THIS METHOD LOADS THE DATA TO THE TABLE
   public void setDecoratorLoad()
   {

      IDTranslate idtrans = new IDTranslate();

      String tokens[];


      while ( setDecoratorRecord != null ) {
         tokens = setDecoratorRecord.split( "~" );

         if ( tokens.length == 4 ) {
            setDecoratorRecordCount++;
            oldMovieID = Integer.parseInt( tokens[ 0 ] );
            newMovieID = idtrans.getNewMovieID( connection, oldMovieID );
 //           System.out.println("Old ID = " + oldMovieID + " New ID: " + newMovieID);

            setDecoratorName = editForApostrophe( tokens[ 1 ] );
            setDecoratorUncredited = ( tokens[ 2 ] );
            setDecoratorNotes = editForApostrophe( tokens[ 3 ] );

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
                           setDecoratorName + "', '" +
                           setDecoratorNotes + "', '" +
                           setDecoratorUncredited + "', " +
                           "null)";

            if(newMovieID == -1) {
               setDecoratorErrors++;
            } else {
               insertTable( query );
               setDecoratorRowCount++;
            }
         }
         else {
            out.println( "=====================================================" );
            out.println( "ARRAY NOT CORRECT LENGTH FOR SET_DECORATOR: " );
            out.println( setDecoratorRecord );
            out.println( "=====================================================" );
            setDecoratorErrors++;
         }

         
         readsetDecoratorRecord();

      }
      System.out.println( "SET_DECORATOR COMPLETE: " + setDecoratorRowCount + " ROWS" );
    
         
   }



   private void readsetDecoratorRecord() {
   

      try {
         setDecoratorRecord = setDecoratorIn.readLine();
      }
      catch( IOException ioe ) {
         System.out.println( "*************************************************" );
         System.out.println( "INPUT/OUTPUT ERROR HAS OCCURRED" );
         System.out.println( "THE INPUT FILE IS " + setDecoratorFileName );
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
       out.println( "TOTAL INPUT ROWS:               " + setDecoratorRecordCount );
       out.println( "TOTAL SET_DECORATOR ROWS:       " + setDecoratorRowCount );
       out.println( "SET_DECORATOR ROWS IN ERROR:    " + setDecoratorErrors );


       out.close();

       System.exit( 0 );

   }
   

   public static void main( String args[] )
   {
      String date = args[ 0 ];
      new SetDecoratorLoad14( date );
   }

  

}