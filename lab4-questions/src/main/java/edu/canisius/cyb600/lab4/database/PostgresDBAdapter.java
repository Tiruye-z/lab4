package edu.canisius.cyb600.lab4.database;

import edu.canisius.cyb600.lab4.dataobjects.Actor;
import edu.canisius.cyb600.lab4.dataobjects.Category;
import edu.canisius.cyb600.lab4.dataobjects.Film;


import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Posgres Implementation of the db adapter.
 */
public class PostgresDBAdapter extends AbstractDBAdapter {

    public PostgresDBAdapter(Connection conn) {
        super(conn);
    }

    @Override
    public List<Category> getAllDistinctCategoryNames() {
        try (Statement statement = conn.createStatement()) {
            //This statement is easy
            //Select * from actor is saying "Return all Fields for all rows in films". Because there
            //is no "where clause", all rows are returned
            ResultSet results = statement.executeQuery("Select * from category");
            //Initialize an empty List to hold the return set of films.
            List<Category> categories = new ArrayList<>();
            //Loop through all the results and create a new Film object to hold all its information
            while (results.next()) {
                Category cate = new Category();
                cate.setCategoryId(results.getInt("CategoryId"));
                cate.setName(results.getString("Name"));
                cate.setLastUpdate(results.getDate("LastUpdate"));
                //Add film to the array
                categories.add(cate);
            }
            //Return all the films.
            return categories;
        } catch (SQLException exception) {
            exception.printStackTrace();
        }
        return new ArrayList<>();
    }

    @Override
    public List<Film> getAllFilmsWithALengthLongerThanX() {
        //Create a string with the sql statement
        String sql =  "SELECT * FROM Film WHERE length > 6";
        //Prepare the SQL statement with the code
        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            //Substitute a string for last name for the ? in the sql
            ResultSet results = statement.executeQuery();
            //Initialize an empty List to hold the return set of films.
            List<Film> films = new ArrayList<>();
            //Loop through all the results and create a new Film object to hold all its information
            while (results.next()) {
                Film film = new Film();

                //Add film to the array
                films.add(film);
            }
            //Return all the films.
            return films;
        } catch (SQLException exception) {
            exception.printStackTrace();
        }
        return new ArrayList<>();
    }


    @Override
    public List<Actor> getActorsFirstNameStartingWithX() {
        List<Actor> actors = new ArrayList<>();

        // Create an SQL statement to retrieve actors
        String sql = "SELECT * FROM Actor WHERE last_name LIKE ?";
        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setString(1, "X%"); // Use wildcard to match any name starting with 'X'
            ResultSet results = statement.executeQuery();

            while (results.next()) {
                Actor actor = new Actor();
                actor.setActorId(results.getInt("actor_id"));
                actor.setFirstName(results.getString("first_name"));
                actor.setLastName(results.getString("last_name"));
                // Set other actor properties as needed
                actors.add(actor);
            }
        } catch (SQLException exception) {
            exception.printStackTrace();
        }

        return actors;
    }
    }

