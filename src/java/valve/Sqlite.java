package valve;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.regex.Pattern;
import org.sqlite.Function;

public class Sqlite {
  public static void createRegexReplaceFunc(Connection conn) throws SQLException {
    Function.create(conn, "regexp_replace", new Function() {
        protected void xFunc() throws SQLException {
        }
      });
  }

  public static void createRegexMatchesFunc(Connection conn) throws SQLException {
    Function.create(conn, "regexp_matches", new Function() {
        protected void xFunc() throws SQLException {
          if (args() < 2 || args() > 3) {
            throw new SQLException("number of arguments to regexp_matches must be between 2 and 3.");
          }

          String stringToMatch = value_text(0);
          String regexp = value_text(1);
          if (args() == 3) {
            regexp = "(?" + value_text(2) + ")" + regexp;
          }

          if (Pattern.matches(regexp, stringToMatch)) {
            result(1);
          }
          else {
            result(0);
          }
        }
      });
  }
}
