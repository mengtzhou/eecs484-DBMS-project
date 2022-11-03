package project2;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;
import java.util.ArrayList;

/*
    The StudentFakebookOracle class is derived from the FakebookOracle class and implements
    the abstract query functions that investigate the database provided via the <connection>
    parameter of the constructor to discover specific information.
*/
public final class StudentFakebookOracle extends FakebookOracle {
    // [Constructor]
    // REQUIRES: <connection> is a valid JDBC connection
    public StudentFakebookOracle(Connection connection) {
        oracle = connection;
    }

    @Override
    // Query 0
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the total number of users for which a birth month is listed
    //        (B) Find the birth month in which the most users were born
    //        (C) Find the birth month in which the fewest users (at least one) were born
    //        (D) Find the IDs, first names, and last names of users born in the month
    //            identified in (B)
    //        (E) Find the IDs, first names, and last name of users born in the month
    //            identified in (C)
    //
    // This query is provided to you completed for reference. Below you will find the appropriate
    // mechanisms for opening up a statement, executing a query, walking through results, extracting
    // data, and more things that you will need to do for the remaining nine queries
    public BirthMonthInfo findMonthOfBirthInfo() throws SQLException {
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly)) {
            // Step 1
            // ------------
            // * Find the total number of users with birth month info
            // * Find the month in which the most users were born
            // * Find the month in which the fewest (but at least 1) users were born
            ResultSet rst = stmt.executeQuery(
                    "SELECT COUNT(*) AS Birthed, Month_of_Birth " + // select birth months and number of uses with that birth month
                            "FROM " + UsersTable + " " + // from all users
                            "WHERE Month_of_Birth IS NOT NULL " + // for which a birth month is available
                            "GROUP BY Month_of_Birth " + // group into buckets by birth month
                            "ORDER BY Birthed DESC, Month_of_Birth ASC"); // sort by users born in that month, descending; break ties by birth month

            int mostMonth = 0;
            int leastMonth = 0;
            int total = 0;
            while (rst.next()) { // step through result rows/records one by one
                if (rst.isFirst()) { // if first record
                    mostMonth = rst.getInt(2); //   it is the month with the most
                }
                if (rst.isLast()) { // if last record
                    leastMonth = rst.getInt(2); //   it is the month with the least
                }
                total += rst.getInt(1); // get the first field's value as an integer
            }
            BirthMonthInfo info = new BirthMonthInfo(total, mostMonth, leastMonth);

            // Step 2
            // ------------
            // * Get the names of users born in the most popular birth month
            rst = stmt.executeQuery(
                    "SELECT User_ID, First_Name, Last_Name " + // select ID, first name, and last name
                            "FROM " + UsersTable + " " + // from all users
                            "WHERE Month_of_Birth = " + mostMonth + " " + // born in the most popular birth month
                            "ORDER BY User_ID"); // sort smaller IDs first

            while (rst.next()) {
                info.addMostPopularBirthMonthUser(new UserInfo(rst.getLong(1), rst.getString(2), rst.getString(3)));
            }

            // Step 3
            // ------------
            // * Get the names of users born in the least popular birth month
            rst = stmt.executeQuery(
                    "SELECT User_ID, First_Name, Last_Name " + // select ID, first name, and last name
                            "FROM " + UsersTable + " " + // from all users
                            "WHERE Month_of_Birth = " + leastMonth + " " + // born in the least popular birth month
                            "ORDER BY User_ID"); // sort smaller IDs first

            while (rst.next()) {
                info.addLeastPopularBirthMonthUser(new UserInfo(rst.getLong(1), rst.getString(2), rst.getString(3)));
            }

            // Step 4
            // ------------
            // * Close resources being used
            rst.close();
            stmt.close(); // if you close the statement first, the result set gets closed automatically

            return info;

        } catch (SQLException e) {
            System.err.println(e.getMessage());
            return new BirthMonthInfo(-1, -1, -1);
        }
    }

    @Override
    // Query 1
    // -----------------------------------------------------------------------------------
    // GOALS: (A) The first name(s) with the most letters
    //        (B) The first name(s) with the fewest letters
    //        (C) The first name held by the most users
    //        (D) The number of users whose first name is that identified in (C)
    public FirstNameInfo findNameInfo() throws SQLException {
         try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                FirstNameInfo info = new FirstNameInfo();
                info.addLongName("Aristophanes");
                info.addLongName("Michelangelo");
                info.addLongName("Peisistratos");
                info.addShortName("Bob");
                info.addShortName("Sue");
                info.addCommonName("Harold");
                info.addCommonName("Jessica");
                info.setCommonNameCount(42);
                return info;
            */
            FirstNameInfo info = new FirstNameInfo();

            ResultSet rst = stmt.executeQuery(
                "SELECT DISTINCT First_Name " +
                "FROM " + UsersTable + " " +
                "WHERE LENGTH(First_Name) = " +
                "(SELECT MAX(LENGTH(First_Name)) FROM " +
                UsersTable + ")  ORDER BY First_Name ASC");
            
            while(rst.next()){
                info.addLongName(rst.getString(1));
            }

            rst = stmt.executeQuery(
                "SELECT DISTINCT First_Name " +
                "FROM " + UsersTable + " " +
                "WHERE LENGTH(First_Name) = " +
                "(SELECT MIN(LENGTH(First_Name)) FROM " +
                UsersTable + ") ORDER BY First_Name ASC");
            
            while(rst.next()){
                info.addShortName(rst.getString(1));
            }


            int maxLen = Integer.MIN_VALUE;
            rst = stmt.executeQuery(
                "SELECT MAX(COUNT(*)) FROM " + UsersTable + " " +
                "GROUP BY First_Name");
            
            while(rst.next()){
                maxLen = rst.getInt(1);
            }


            int numComm = Integer.MIN_VALUE;
            
            rst = stmt.executeQuery(
                "SELECT COUNT(*), First_Name " +
                "FROM " + UsersTable + " " +
                "GROUP BY First_Name " +
                "HAVING COUNT(*) = " + maxLen + " " + 
                "ORDER BY First_Name ASC");
            
            while(rst.next()){
                numComm = rst.getInt(1);
                info.setCommonNameCount(numComm);
                info.addCommonName(rst.getString(2));
            }

            rst.close();
            stmt.close();
            return info; // placeholder for compilation
        } catch (SQLException e) {
            System.err.println(e.getMessage());
            return new FirstNameInfo();
        }
    }

    @Override
    // Query 2
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the IDs, first names, and last names of users without any friends
    //
    // Be careful! Remember that if two users are friends, the Friends table only contains
    // the one entry (U1, U2) where U1 < U2.
    public FakebookArrayList<UserInfo> lonelyUsers() throws SQLException {
        FakebookArrayList<UserInfo> results = new FakebookArrayList<UserInfo>(", ");

        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                UserInfo u1 = new UserInfo(15, "Abraham", "Lincoln");
                UserInfo u2 = new UserInfo(39, "Margaret", "Thatcher");
                results.add(u1);
                results.add(u2);
            */

            ResultSet rst = stmt.executeQuery(
                "SELECT USER_ID, First_Name, Last_Name " +
                "FROM " + UsersTable + " " +
                "WHERE USER_ID NOT IN (SELECT USER1_ID FROM " + FriendsTable + 
                " UNION SELECT User2_ID FROM " + FriendsTable + " ) " +
                "ORDER BY USER_ID ASC ");

            while(rst.next()){
                results.add(new UserInfo(rst.getInt(1), rst.getString(2), rst.getString(3)));
            }
            rst.close();
            stmt.close();

        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }

        return results;
    }

    @Override
    // Query 3
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the IDs, first names, and last names of users who no longer live
    //            in their hometown (i.e. their current city and their hometown are different)
    public FakebookArrayList<UserInfo> liveAwayFromHome() throws SQLException {
        FakebookArrayList<UserInfo> results = new FakebookArrayList<UserInfo>(", ");

        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                UserInfo u1 = new UserInfo(9, "Meryl", "Streep");
                UserInfo u2 = new UserInfo(104, "Tom", "Hanks");
                results.add(u1);
                results.add(u2);
            */
            ResultSet rst = stmt.executeQuery(
                "SELECT u.user_id, first_name, last_name " + 
                "FROM "+ UsersTable + " u, " + CurrentCitiesTable + " cc, " + HometownCitiesTable + " hc " + 
                "WHERE u.user_id = cc.user_id " + 
                "AND u.user_id = hc.user_id " + 
                "AND cc.current_city_id IS NOT NULL " + 
                "AND hc.hometown_city_id IS NOT NULL " + 
                "AND cc.current_city_id <> hc.hometown_city_id " + 
                "ORDER BY u.user_id"
            );
            while (rst.next()) {
                results.add(new UserInfo(rst.getLong(1), rst.getString(2), rst.getString(3)));
            }

            rst.close();
            stmt.close();

        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }

        return results;
    }

    @Override
    // Query 4
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the IDs, links, and IDs and names of the containing album of the top
    //            <num> photos with the most tagged users
    //        (B) For each photo identified in (A), find the IDs, first names, and last names
    //            of the users therein tagged
    public FakebookArrayList<TaggedPhotoInfo> findPhotosWithMostTags(int num) throws SQLException {
        FakebookArrayList<TaggedPhotoInfo> results = new FakebookArrayList<TaggedPhotoInfo>("\n");

        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly)) {
            
            ResultSet rst = stmt.executeQuery(
                    "SELECT * FROM " +
                    "(SELECT P.PHOTO_ID, P.ALBUM_ID, P.PHOTO_LINK, A.ALBUM_NAME " + 
                    "FROM " + PhotosTable + " P " +
                    "JOIN " + AlbumsTable + " A ON P.ALBUM_ID = A.ALBUM_ID " + 
                    "JOIN ( " + 
                        "SELECT T.TAG_PHOTO_ID, COUNT(*) SUM_COUNT " + 
                        "FROM " + TagsTable + " T " +
                        "GROUP BY T.TAG_PHOTO_ID) T1 " +
                        "ON P.PHOTO_ID = T1.TAG_PHOTO_ID " +
                        "ORDER BY T1.SUM_COUNT DESC, T1.TAG_PHOTO_ID ASC) " + 
                        "WHERE ROWNUM <= " + num);
            Statement stmt1 = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly);
            while (rst.next()) {
                Long photoA = rst.getLong(1);
                PhotoInfo p = new PhotoInfo(photoA, rst.getLong(2), rst.getString(3), rst.getString(4));
                TaggedPhotoInfo tp = new TaggedPhotoInfo(p);
                ResultSet rst1 = stmt1.executeQuery(
                    "SELECT T.TAG_SUBJECT_ID, U.FIRST_NAME, U.LAST_NAME " +
                    "FROM " + UsersTable + " U " +
                    "JOIN " + TagsTable + " T " + " ON U.USER_ID = T.TAG_SUBJECT_ID " +
                    "WHERE T.TAG_PHOTO_ID = " + photoA + " " +
                    "ORDER BY U.USER_ID ASC ");
                while (rst1.next()) {
                    tp.addTaggedUser(new UserInfo(rst1.getLong(1), rst1.getString(2), rst1.getString(3)));
                }

                results.add(tp);
                rst1.close();
            }

            rst.close();
            stmt.close();
            stmt1.close();

            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                PhotoInfo p = new PhotoInfo(80, 5, "www.photolink.net", "Winterfell S1");
                UserInfo u1 = new UserInfo(3901, "Jon", "Snow");
                UserInfo u2 = new UserInfo(3902, "Arya", "Stark");
                UserInfo u3 = new UserInfo(3903, "Sansa", "Stark");
                TaggedPhotoInfo tp = new TaggedPhotoInfo(p);
                tp.addTaggedUser(u1);
                tp.addTaggedUser(u2);
                tp.addTaggedUser(u3);
                results.add(tp);
            */
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }

        return results;
    }

    @Override
    // Query 5
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the IDs, first names, last names, and birth years of each of the two
    //            users in the top <num> pairs of users that meet each of the following
    //            criteria:
    //              (i) same gender
    //              (ii) tagged in at least one common photo
    //              (iii) difference in birth years is no more than <yearDiff>
    //              (iv) not friends
    //        (B) For each pair identified in (A), find the IDs, links, and IDs and names of
    //            the containing album of each photo in which they are tagged together
    public FakebookArrayList<MatchPair> matchMaker(int num, int yearDiff) throws SQLException {
        FakebookArrayList<MatchPair> results = new FakebookArrayList<MatchPair>("\n");

        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly)) {

            stmt.executeUpdate(
                "CREATE OR REPLACE VIEW USER_YEAR_TABLE AS " +
                "SELECT U.USER_ID AS USER1, U1.USER_ID AS USER2 FROM " +
                UsersTable + " U, " + UsersTable + " U1, " + TagsTable + " T, " + TagsTable + " T1 " +
                "WHERE U.GENDER = U1.GENDER "+
                "AND U.USER_ID < U1.USER_ID "+
                "AND ABS(U.YEAR_OF_BIRTH-U1.YEAR_OF_BIRTH) <= " + yearDiff + " " +
                "AND NOT EXISTS (SELECT * FROM " + FriendsTable + " F " +
                "WHERE F.USER1_ID = U.USER_ID AND F.USER2_ID = U1.USER_ID) " +
                "AND U.USER_ID = T.TAG_SUBJECT_ID "+
                "AND U1.USER_ID = T1.TAG_SUBJECT_ID " +
                "AND T.TAG_PHOTO_ID = T1.TAG_PHOTO_ID " +
                "GROUP BY U.USER_ID, U1.USER_ID " 
                );

            // ResultSet rst = stmt.executeQuery("SELECT " + "1,2,3,4,5,6,7,8,9,10,11,12");
            ResultSet rst = stmt.executeQuery(
                "SELECT A,B,C,D,E,F,G,H,I,J,K,L FROM (" +
                "SELECT U1.USER_ID AS A, U1.FIRST_NAME AS B, U1.LAST_NAME AS C, U1.YEAR_OF_BIRTH AS D, " +
                "U2.USER_ID AS E, U2.FIRST_NAME AS F, U2.LAST_NAME AS G, U2.YEAR_OF_BIRTH AS H, " +
                "T1.TAG_PHOTO_ID AS I, A.ALBUM_ID AS J, P.PHOTO_LINK AS K, A.ALBUM_NAME AS L"
                + " FROM " + UsersTable + " U1, " + UsersTable + " U2, " +
                TagsTable + " T1, " + TagsTable + " T2, " + AlbumsTable + " A, " +
                PhotosTable + " P, USER_YEAR_TABLE Y " +
                "WHERE U1.USER_ID = T1.TAG_SUBJECT_ID AND U2.USER_ID = T2.TAG_SUBJECT_ID " +
                "AND T1.TAG_PHOTO_ID = T2.TAG_PHOTO_ID " +
                "AND A.ALBUM_ID = P.ALBUM_ID AND T1.TAG_PHOTO_ID = P.PHOTO_ID " +
                "AND U1.USER_ID = Y.USER1 AND U2.USER_ID = Y.USER2" +
                ") WHERE ROWNUM <= " + num + " " + 
                "ORDER BY B");

            while (rst.next()) {
                UserInfo u1 = new UserInfo(rst.getLong(1), rst.getString(2), rst.getString(3));
                UserInfo u2 = new UserInfo(rst.getLong(5), rst.getString(6), rst.getString(7));
                MatchPair mp = new MatchPair(u1, rst.getLong(4), u2, rst.getLong(8));
                PhotoInfo p = new PhotoInfo(rst.getLong(9), rst.getLong(10), rst.getString(11), rst.getString(12));
                mp.addSharedPhoto(p);
                results.add(mp);
            }

            stmt.executeUpdate("DROP VIEW USER_YEAR_TABLE");
		    stmt.close();
            rst.close();

            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                UserInfo u1 = new UserInfo(93103, "Romeo", "Montague");
                UserInfo u2 = new UserInfo(93113, "Juliet", "Capulet");
                MatchPair mp = new MatchPair(u1, 1597, u2, 1597);
                PhotoInfo p = new PhotoInfo(167, 309, "www.photolink.net", "Tragedy");
                mp.addSharedPhoto(p);
                results.add(mp);
            */
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }

        return results;
    }

    @Override
    // Query 6
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the IDs, first names, and last names of each of the two users in
    //            the top <num> pairs of users who are not friends but have a lot of
    //            common friends
    //        (B) For each pair identified in (A), find the IDs, first names, and last names
    //            of all the two users' common friends
    public FakebookArrayList<UsersPair> suggestFriends(int num) throws SQLException {
        FakebookArrayList<UsersPair> results = new FakebookArrayList<UsersPair>("\n");

        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                UserInfo u1 = new UserInfo(16, "The", "Hacker");
                UserInfo u2 = new UserInfo(80, "Dr.", "Marbles");
                UserInfo u3 = new UserInfo(192, "Digit", "Le Boid");
                UsersPair up = new UsersPair(u1, u2);
                up.addSharedFriend(u3);
                results.add(up);
            */
            ResultSet rst = stmt.executeQuery(
                "WITH friends AS ( " + 
                    "SELECT user1_id AS u1id, user2_id AS u2id FROM "+ FriendsTable + " " + 
                    "UNION " + 
                    "SELECT user2_id AS u1id, user1_id AS u2id FROM "+ FriendsTable + " " + 
                ") " + 
                "SELECT " + 
                    "p.u1id AS u1id, u1.first_name AS u1fn, u1.last_name AS u1ln, " + 
                    "p.u2id AS u2id, u2.first_name AS u2fn, u2.last_name AS u2ln, " + 
                    "u3.user_id AS mfid, u3.first_name AS mffn, u3.last_name AS mfln, " + 
                    "p.num_mfs AS num_mfs " + 
                "FROM " + 
                    UsersTable + " u1, " + UsersTable +  " u2, " + UsersTable + " u3, " + 
                    "( " + 
                        "SELECT * FROM ( " + 
                            "SELECT f1.u1id AS u1id, f2.u1id AS u2id, COUNT(*) AS num_mfs " + 
                            "FROM friends f1, friends f2 " + 
                            "WHERE f1.u2id = f2.u2id AND f1.u1id < f2.u1id " + 
                            "AND (f1.u1id, f2.u1id) NOT IN (SELECT * FROM "+ FriendsTable + ") " + 
                            "GROUP BY f1.u1id, f2.u1id " + 
                            "ORDER BY num_mfs DESC " + 
                        ") " + 
                        "WHERE ROWNUM <= " + String.valueOf(num) + 
                    ") p " + 
                "WHERE p.u1id = u1.user_id AND p.u2id = u2.user_id " + 
                "AND (p.u1id, u3.user_id) IN (SELECT * FROM friends) " + 
                "AND (p.u2id, u3.user_id) IN (SELECT * FROM friends) " + 
                "ORDER BY p.num_mfs DESC, p.u1id, p.u2id, u3.user_id "
            );

            while (rst.next()) {
                UserInfo u1 = new UserInfo(rst.getLong(1), rst.getString(2), rst.getString(3));
                UserInfo u2 = new UserInfo(rst.getLong(4), rst.getString(5), rst.getString(6));
                UsersPair up = new UsersPair(u1, u2);
                int num_mfs = rst.getInt(10);
                for (int i = 0; i < num_mfs; ++i) {
                    UserInfo mf = new UserInfo(rst.getLong(7), rst.getString(8), rst.getString(9));
                    up.addSharedFriend(mf);
                    if (i < num_mfs - 1) rst.next();
                }
                results.add(up);
            }
            rst.close();
            stmt.close();
            
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }

        return results;
    }

    @Override
    // Query 7
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the name of the state or states in which the most events are held
    //        (B) Find the number of events held in the states identified in (A)
    public EventStateInfo findEventStates() throws SQLException {
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                EventStateInfo info = new EventStateInfo(50);
                info.addState("Kentucky");
                info.addState("Hawaii");
                info.addState("New Hampshire");
                return info;
            */
            ResultSet rst = stmt.executeQuery(
                "WITH tmp AS ( " + 
                    "SELECT " + 
                        "c.state_name, COUNT(e.event_id) AS num_events " + 
                    "FROM " + 
                        EventsTable + " e, " + CitiesTable +  " c " + 
                    "WHERE e.event_city_id = c.city_id " + 
                    "GROUP BY c.state_name " + 
                ") " + 
                "SELECT state_name, num_events " + 
                "FROM tmp " + 
               " WHERE num_events = (SELECT MAX(num_events) FROM tmp) " + 
                "ORDER BY state_name"
            );
            if (rst.next()) {
                EventStateInfo info = new EventStateInfo(rst.getInt(2));
                info.addState(rst.getString(1));
                while (rst.next()) {
                    info.addState(rst.getString(1));
                }
                rst.close();
                stmt.close();
                return info;
            }
            rst.close();
            stmt.close();
            return new EventStateInfo(-1); // placeholder for compilation
        } catch (SQLException e) {
            System.err.println(e.getMessage());
            return new EventStateInfo(-1);
        }
    }

    @Override
    // Query 8
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the ID, first name, and last name of the oldest friend of the user
    //            with User ID <userID>
    //        (B) Find the ID, first name, and last name of the youngest friend of the user
    //            with User ID <userID>
    public AgeInfo findAgeInfo(long userID) throws SQLException {
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                UserInfo old = new UserInfo(12000000, "Galileo", "Galilei");
                UserInfo young = new UserInfo(80000000, "Neil", "deGrasse Tyson");
                return new AgeInfo(old, young);
            */
            UserInfo old = new UserInfo(-1, "UNWRITTEN", "UNWRITTEN");
            UserInfo young = new UserInfo(-1, "UNWRITTEN", "UNWRITTEN");
            ResultSet rst = stmt.executeQuery(
                "WITH tmp AS ( " + 
                    "SELECT " + 
                        "u.user_id, u.first_name, u.last_name, " + 
                        "u.year_of_birth, u.month_of_birth, u.day_of_birth " + 
                    "FROM " + UsersTable + " u " + 
                    "WHERE u.user_id IN ( " + 
                        "SELECT f.user1_id " + 
                        "FROM " + FriendsTable +" f " + 
                        "WHERE f.user2_id = " + String.valueOf(userID) + 
                        "UNION " + 
                        "SELECT f.user2_id " + 
                        "FROM " + FriendsTable + " f " + 
                       " WHERE f.user1_id = " + String.valueOf(userID) + 
                    ") " + 
                ") " + 
                "SELECT user_id, first_name, last_name " + 
                "FROM ( " + 
                    "SELECT user_id, first_name, last_name " + 
                    "FROM tmp " + 
                    "ORDER BY year_of_birth, month_of_birth, day_of_birth, user_id DESC " + 
                ") " + 
                "WHERE ROWNUM = 1 " 
            );
            if (rst.next()) old = new UserInfo(rst.getLong(1), rst.getString(2), rst.getString(3));
            rst = stmt.executeQuery(
                "WITH tmp AS ( " + 
                    "SELECT " + 
                        "u.user_id, u.first_name, u.last_name, " + 
                        "u.year_of_birth, u.month_of_birth, u.day_of_birth " + 
                    "FROM " + UsersTable + " u " + 
                    "WHERE u.user_id IN ( " + 
                        "SELECT f.user1_id " + 
                        "FROM " + FriendsTable +" f " + 
                        "WHERE f.user2_id = " + String.valueOf(userID) + 
                        "UNION " + 
                        "SELECT f.user2_id " + 
                        "FROM " + FriendsTable + " f " + 
                       " WHERE f.user1_id = " + String.valueOf(userID) + 
                    ") " + 
                ") " + 
                "SELECT user_id, first_name, last_name " + 
                "FROM ( " + 
                    "SELECT user_id, first_name, last_name " + 
                    "FROM tmp " + 
                    "ORDER BY year_of_birth DESC, month_of_birth DESC, day_of_birth DESC, user_id DESC " + 
                ") " + 
                "WHERE ROWNUM = 1 " 
            );
            if (rst.next()) young = new UserInfo(rst.getLong(1), rst.getString(2), rst.getString(3));
            rst.close();
            stmt.close();
            return new AgeInfo(old, young); // placeholder for compilation
        } catch (SQLException e) {
            System.err.println(e.getMessage());
            return new AgeInfo(new UserInfo(-1, "ERROR", "ERROR"), new UserInfo(-1, "ERROR", "ERROR"));
        }
    }

    @Override
    // Query 9
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find all pairs of users that meet each of the following criteria
    //              (i) same last name
    //              (ii) same hometown
    //              (iii) are friends
    //              (iv) less than 10 birth years apart
    public FakebookArrayList<SiblingInfo> findPotentialSiblings() throws SQLException {
        FakebookArrayList<SiblingInfo> results = new FakebookArrayList<SiblingInfo>("\n");

        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                UserInfo u1 = new UserInfo(81023, "Kim", "Kardashian");
                UserInfo u2 = new UserInfo(17231, "Kourtney", "Kardashian");
                SiblingInfo si = new SiblingInfo(u1, u2);
                results.add(si);
            */
            ResultSet rst = stmt.executeQuery(
                "SELECT " + 
                    "u1.user_id, u1.first_name, u1.last_name, u2.user_id, u2.first_name, u2.last_name " + 
                "FROM " + 
                    UsersTable + " u1, " + UsersTable +  " u2, " + 
                    HometownCitiesTable + " hc1, " + HometownCitiesTable + " hc2 " + 
                "WHERE u1.user_id = hc1.user_id AND u2.user_id = hc2.user_id " + 
                "AND u1.last_name = u2.last_name " + 
                "AND hc1.hometown_city_id = hc2.hometown_city_id " + 
                "AND ABS(u1.year_of_birth - u2.year_of_birth) < 10 " + 
                "AND u1.user_id < u2.user_id " + 
                "AND (u1.user_id, u2.user_id) IN (SELECT user1_id, user2_id FROM " + FriendsTable + ") " + 
                "ORDER BY u1.user_id, u2.user_id"
            );
            while (rst.next()) {
                UserInfo u1 = new UserInfo(rst.getLong(1), rst.getString(2), rst.getString(3));
                UserInfo u2 = new UserInfo(rst.getLong(4), rst.getString(5), rst.getString(6));
                SiblingInfo si = new SiblingInfo(u1, u2);
                results.add(si);
            }
            rst.close();
            stmt.close();
            return results;
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }

        return results;
    }

    // Member Variables
    private Connection oracle;
    private final String UsersTable = FakebookOracleConstants.UsersTable;
    private final String CitiesTable = FakebookOracleConstants.CitiesTable;
    private final String FriendsTable = FakebookOracleConstants.FriendsTable;
    private final String CurrentCitiesTable = FakebookOracleConstants.CurrentCitiesTable;
    private final String HometownCitiesTable = FakebookOracleConstants.HometownCitiesTable;
    private final String ProgramsTable = FakebookOracleConstants.ProgramsTable;
    private final String EducationTable = FakebookOracleConstants.EducationTable;
    private final String EventsTable = FakebookOracleConstants.EventsTable;
    private final String AlbumsTable = FakebookOracleConstants.AlbumsTable;
    private final String PhotosTable = FakebookOracleConstants.PhotosTable;
    private final String TagsTable = FakebookOracleConstants.TagsTable;
}
