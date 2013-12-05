package se.kth.id2212.bankjdbc;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

@SuppressWarnings("serial")
public class BankImpl extends UnicastRemoteObject implements Bank {

    public static final String TABLE_NAME = "ACCOUNT";
    private PreparedStatement createAccountStatement;
    private PreparedStatement findAccountStatement;
    private PreparedStatement deleteAccountStatement;
    //Although we now have all the data about accounts in a database now, we
    //still have this HashMap as a in-memory cache
    private Map<String, Account> accounts = new HashMap<>();
    private String datasource;
    private String dbms;

    public BankImpl(String datasource, String dbms)
            throws RemoteException, ClassNotFoundException, SQLException {
        //UnicastRemoteObject constructor call
        super();
        this.datasource = datasource;
        this.dbms = dbms;
        Connection connection = createDatasource();
        prepareStatements(connection);
    }

    private Connection createDatasource() throws ClassNotFoundException, SQLException {
        Connection connection = getConnection();
        boolean exist = false;
        //getTables is in SDK library, that the table names are held in column 3
        //is specified in the documentation
        int tableNameColumn = 3;
        DatabaseMetaData dbm = connection.getMetaData();
        //Trying to find the table ACCOUNT
        for (ResultSet rs = dbm.getTables(null, null, null, null); rs.next();) {
            if (rs.getString(tableNameColumn).equals(TABLE_NAME)) {
                exist = true;
                rs.close();
                break;
            }
        }
        if (!exist) {
            Statement statement = connection.createStatement();
            statement.executeUpdate("CREATE TABLE " + TABLE_NAME
                    + " (name VARCHAR(32) PRIMARY KEY, balance FLOAT)");
        }
        return connection;
    }

    private Connection getConnection()
            throws ClassNotFoundException, SQLException {
        if (dbms.equalsIgnoreCase("access")) {
            Class.forName("sun.jdbc.odbc.JdbcOdbcDriver");
            return DriverManager.getConnection("jdbc:odbc:" + datasource);
        } else if (dbms.equalsIgnoreCase("cloudscape")) {
            Class.forName("COM.cloudscape.core.RmiJdbcDriver");
            return DriverManager.getConnection(
                    "jdbc:cloudscape:rmi://localhost:1099/" + datasource
                    + ";create=true;");
        } else if (dbms.equalsIgnoreCase("pointbase")) {
            Class.forName("com.pointbase.jdbc.jdbcUniversalDriver");
            return DriverManager.getConnection(
                    "jdbc:pointbase:server://localhost:9092/" + datasource
                    + ",new", "PBPUBLIC", "PBPUBLIC");
        } else if (dbms.equalsIgnoreCase("derby")) {
            Class.forName("org.apache.derby.jdbc.ClientXADataSource");
            return DriverManager.getConnection(
                    "jdbc:derby://localhost:1527/" + datasource + ";create=true");
        } else if (dbms.equalsIgnoreCase("mysql")) {
            Class.forName("com.mysql.jdbc.Driver");
            return DriverManager.getConnection(
                    "jdbc:mysql://localhost:3306/" + datasource, "root", "javajava");
        } else {
            return null;
        }
    }

    private void prepareStatements(Connection connection) throws SQLException {
        createAccountStatement = connection.prepareStatement("INSERT INTO "
                + TABLE_NAME + " VALUES (?, 0)");
        findAccountStatement = connection.prepareStatement("SELECT * from "
                + TABLE_NAME + " WHERE NAME = ?");
        deleteAccountStatement = connection.prepareStatement("DELETE FROM "
                + TABLE_NAME + " WHERE name = ?");
    }

    //synchronized because this program uses RMI, and more than one thread could
    //enter some of these methods. But this also results in the server not being
    //really multi-threaded anymore. We could use another way of access control
    //for database operations than synchronized, and that is transactions!
    @Override
    public synchronized String[] listAccounts() {
        return accounts.keySet().toArray(new String[1]);
    }

    @Override
    public synchronized Account newAccount(String name) throws RemoteException,
            RejectedException {
        //accounts is the HashTable (in-memory caching)
        AccountImpl account = (AccountImpl) accounts.get(name);
        if (account != null) {
            //If account not null then the account already exists in our cache
            //throw exception and abort execution of this method
            System.out.println("Account [" + name + "] exists!!!");
            throw new RejectedException("Rejected: Bank:  Account for: "
                    + name + " already exists: " + account);
        }
        ResultSet result = null;
        try {
            findAccountStatement.setString(1, name);
            result = findAccountStatement.executeQuery();

            if (result.next()) {
                // account exists, instantiate, put in cache and throw exception.
                account = new AccountImpl(name, result.getFloat("balance"),
                        getConnection());
                accounts.put(name, account);
                throw new RejectedException("Rejected: Account for: " + name
                        + " already exists");
            }
            result.close();

            //If we get all the way here without throwing exception (and aborting),
            //we can create account.
            createAccountStatement.setString(1, name);
            int rows = createAccountStatement.executeUpdate();
            if (rows == 1) {
                account = new AccountImpl(name, getConnection());
                accounts.put(name, account);
                System.out.println("Bank: Account: " + account
                        + " has been created for " + name);
                return account;
            } else {
                //we didn't update one row
                throw new RejectedException("Cannot create an account for " + name);
            }
        } catch (SQLException | ClassNotFoundException e) {
            e.printStackTrace();
            throw new RejectedException("Cannot create an account for " + name, e);
        }
    }

    @Override
    public synchronized Account getAccount(String name) throws RemoteException,
            RejectedException {
        if (name == null) {
            return null;
        }

        Account acct = accounts.get(name);
        if (acct == null) {
            try {
                findAccountStatement.setString(1, name);
                ResultSet result = findAccountStatement.executeQuery();
                if (result.next()) {
                    acct = new AccountImpl(result.getString("name"),
                            result.getFloat("balance"), getConnection());
                    result.close();
                    accounts.put(name, acct);
                } else {
                    return null;
                }
            } catch (SQLException | ClassNotFoundException e) {
                throw new RejectedException("Unable to find account for " + name, e);
            }
        }
        return acct;
    }

    @Override
    public synchronized boolean deleteAccount(String name) throws RejectedException {
        if (!hasAccount(name)) {
            return false;
        }
        accounts.remove(name);
        try {
            deleteAccountStatement.setString(1, name);
            int rows = deleteAccountStatement.executeUpdate();
            if (rows != 1) {
                throw new RejectedException("Unable to delete account..." + name);
            }
        } catch (SQLException e) {
            System.out.println("Unable to delete account for " + name + ": "
                    + e.getMessage());
            throw new RejectedException("Unable to delete account..." + name, e);
        }
        System.out.println("Bank: Account for " + name + " has been deleted");
        return true;
    }

    private boolean hasAccount(String name) {
        if (accounts.get(name) == null) {
            return false;
        } else {
            return true;
        }
    }
}
