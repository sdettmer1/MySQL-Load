import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Connection;

/**
* This class translates the old movie id to 
* the new movie id number via the id_translation table
*
* @author Shane Dettmer
* @version 1.1
* @since 2022-08-09
*/
public class IDTranslate {

   private int newID;

   /**
   * This is a getter to return the new movieID value
   *
   * @return int The new movieID value
   */
   public int getNewID() {

      return newID;
   }

   /**
   * This is a setter to for the new movieID value
   *
   * @param newID The new movieID value
   */
   public void setNewID(int newID) {
      this.newID = newID;   
      
   }

   /**
   * Reads the id_translation table to convert the old movieID
   * value to the new movieID value
   * 
   * @param connection The RDBMS connection
   * @param ID The old movie ID value to be converted
   * @return int The new movie ID value
   */
   public int getNewMovieID( Connection connection, int ID ) {

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


}