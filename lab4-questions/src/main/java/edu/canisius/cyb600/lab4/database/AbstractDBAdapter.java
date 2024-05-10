package edu.canisius.cyb600.lab4.database;

import edu.canisius.cyb600.lab4.dataobjects.Actor;
import edu.canisius.cyb600.lab4.dataobjects.Category;
import edu.canisius.cyb600.lab4.dataobjects.Film;

import java.sql.Connection;
import java.util.List;

/**
 * Abstract DB Adapter
 */
public abstract class AbstractDBAdapter {
    Connection conn;

    public AbstractDBAdapter(Connection conn) {
        this.conn = conn;
    }

    public abstract List<Category> getAllDistinctCategoryNames();

    public abstract List<Film> getAllFilmsWithALengthLongerThanX();

    public abstract List<Actor> getActorsFirstNameStartingWithX();

    public abstract List<Film> getFilmsInCategory(Category category);

    //SELECTS

    //INSERTS

    //JOIN


}
