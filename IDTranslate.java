import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Connection;


public class IDTranslate {

   private int newID;

   public int getNewID() {

      return newID;
   }

   public void setNewID(int newID) {
      this.newID = newID;   
      
   }

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