package marketplace.server;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import marketplace.shared.BadCredentialsException;
import marketplace.shared.DuplicateItemException;
import marketplace.shared.Marketplace;
import marketplace.shared.MarketplaceAccount;
import marketplace.shared.Item;
import marketplace.shared.MarketplaceClient;
import marketplace.shared.RegisterCustomerException;
import se.kth.id2212.bankjdbc.RejectedException;

public class MarketplaceImpl extends UnicastRemoteObject implements Marketplace {

    public static final String USER_TABLE_NAME = "USERDATA";
    public static final String ITEM_TABLE_NAME = "ITEM";
    public static final String WISH_TABLE_NAME = "WISH";
    private PreparedStatement createAccountStatement;
    private PreparedStatement getCredentialsStatement;
    private PreparedStatement deleteAccountStatement;
    private PreparedStatement makeWishStatement;
    private PreparedStatement addItemStatement;
    private PreparedStatement getItemStatement;
    private PreparedStatement removeItemStatement;
    private PreparedStatement removeSoldItemStatement;
    private PreparedStatement getWishStatement;
    private PreparedStatement removeUsersWishesStatement;
    private PreparedStatement removeWishStatement;
    private PreparedStatement getAccountNameStatement;
    private PreparedStatement getBankAccountNameStatement;
    private PreparedStatement incrementSoldStatement;
    private PreparedStatement incrementBoughtStatement;
    private PreparedStatement getAllItemsStatement;
    private PreparedStatement getAllWishesStatement;
    Map<String, MarketplaceAccount> accounts;
    List<Wish> wishes;
    List<Item> items;

    public MarketplaceImpl() throws RemoteException {
        Connection connection;
        try {
            connection = createDatasource();
            prepareStatements(connection);
        } catch (ClassNotFoundException ex) {
            System.out.println("Problem with datasource");
            ex.printStackTrace();
        } catch (SQLException ex) {
            System.out.println("Problem connecting to database");
            ex.printStackTrace();
        }
        
        accounts = new HashMap();
        wishes = new LinkedList();
        items = new LinkedList();
        
        try {
            String itemName;
            float price;
            String user;
            //We need to get all data from database and add it to our in-memory cache
            ResultSet itemsResult = getAllItemsStatement.executeQuery();
            ResultSet wishResult = getAllWishesStatement.executeQuery();
            while (itemsResult.next()) {
                itemName = itemsResult.getString("itemname");
                price = itemsResult.getFloat("price");
                user = itemsResult.getString("seller");
                items.add((Item) new ItemImpl(itemName, price, user));
            } 
            Collections.sort(items);
            while (wishResult.next()) {
                itemName = wishResult.getString("itemname");
                price = wishResult.getFloat("price");
                user = wishResult.getString("wisher");
                wishes.add(new Wish(itemName, price, user));
            } 
        } catch (SQLException ex) {
            System.out.println("Problem retrieving items and/or wishes from "
                    + "database.");
            ex.printStackTrace();
        }
    }

    private Connection createDatasource() throws ClassNotFoundException, SQLException {
        Connection connection = getConnection();
        boolean exist = false;
        //getTables is in SDK library, that the table names are held in column 3
        //is specified in the documentation
        int tableNameColumn = 3;
        DatabaseMetaData dbm = connection.getMetaData();
        //Trying to find the table named as USER_TABLE_NAME
        for (ResultSet rs = dbm.getTables(null, null, null, null); rs.next();) {
            if (rs.getString(tableNameColumn).equals(USER_TABLE_NAME)) {
                System.out.println("Table already exists in database, no need to"
                        + " create new.");
                exist = true;
                rs.close();
                break;
            }
        }
        if (!exist) {
            System.out.println("Didn't find tables in database, creating them...");
            //If the user-table doesn't exist, create all tables (user, item and
            //wish).
            Statement statement1 = connection.createStatement();
            statement1.executeUpdate("CREATE TABLE " + USER_TABLE_NAME
                    + " (username VARCHAR(32) PRIMARY KEY, password VARCHAR(32),"
                    + " bankaccount VARCHAR(32), bought INTEGER, sold INTEGER)");
            Statement statement2 = connection.createStatement();
            statement2.executeUpdate("CREATE TABLE " + ITEM_TABLE_NAME
                    + "(itemid INTEGER NOT NULL GENERATED ALWAYS AS IDENTITY "
                    + "(START WITH 1, INCREMENT BY 1) PRIMARY KEY, "
                    + "itemname VARCHAR(32), price FLOAT, "
                    + "seller VARCHAR(32), "
                    + "FOREIGN KEY (seller) REFERENCES " + USER_TABLE_NAME
                    + " (username))");
            Statement statement3 = connection.createStatement();
            statement3.executeUpdate("CREATE TABLE " + WISH_TABLE_NAME
                    + "(wishid INTEGER NOT NULL GENERATED ALWAYS AS IDENTITY "
                    + "(START WITH 1, INCREMENT BY 1) PRIMARY KEY, "
                    + "itemname VARCHAR(32), price FLOAT, "
                    + "wisher VARCHAR(32), "
                    + "FOREIGN KEY (wisher) REFERENCES " + USER_TABLE_NAME
                    + " (username))");
        }
        return connection;
    }

    private Connection getConnection()
            throws ClassNotFoundException, SQLException {
        if (MarketplaceServer.DBMS.equalsIgnoreCase("access")) {
            Class.forName("sun.jdbc.odbc.JdbcOdbcDriver");
            return DriverManager.getConnection("jdbc:odbc:"
                    + MarketplaceServer.DATASOURCE);
        } else if (MarketplaceServer.DBMS.equalsIgnoreCase("cloudscape")) {
            Class.forName("COM.cloudscape.core.RmiJdbcDriver");
            return DriverManager.getConnection(
                    "jdbc:cloudscape:rmi://localhost:1099/"
                    + MarketplaceServer.DATASOURCE
                    + ";create=true;");
        } else if (MarketplaceServer.DBMS.equalsIgnoreCase("pointbase")) {
            Class.forName("com.pointbase.jdbc.jdbcUniversalDriver");
            return DriverManager.getConnection(
                    "jdbc:pointbase:server://localhost:9092/"
                    + MarketplaceServer.DATASOURCE
                    + ",new", "PBPUBLIC", "PBPUBLIC");
        } else if (MarketplaceServer.DBMS.equalsIgnoreCase("derby")) {
            Class.forName("org.apache.derby.jdbc.ClientXADataSource");
            return DriverManager.getConnection(
                    "jdbc:derby://localhost:1527/"
                    + MarketplaceServer.DATASOURCE + ";create=true");
        } else if (MarketplaceServer.DBMS.equalsIgnoreCase("mysql")) {
            Class.forName("com.mysql.jdbc.Driver");
            return DriverManager.getConnection(
                    "jdbc:mysql://localhost:3306/"
                    + MarketplaceServer.DATASOURCE, "root", "javajava");
        } else {
            return null;
        }
    }

    private void prepareStatements(Connection connection) throws SQLException {
        createAccountStatement = connection.prepareStatement("INSERT INTO "
                + USER_TABLE_NAME + " VALUES (?, ?, ?, 0, 0)");
        getAccountNameStatement = connection.prepareStatement("SELECT username "
                + "FROM " + USER_TABLE_NAME + " WHERE username = ?");
        getBankAccountNameStatement = connection.prepareStatement("SELECT bankaccount "
                + "FROM " + USER_TABLE_NAME + " WHERE username = ?");
        getCredentialsStatement = connection.prepareStatement("SELECT password "
                + "from "
                + USER_TABLE_NAME + " WHERE username = ?");
        deleteAccountStatement = connection.prepareStatement("DELETE FROM "
                + USER_TABLE_NAME + " WHERE username = ?");
        removeItemStatement = connection.prepareStatement("DELETE FROM "
                + ITEM_TABLE_NAME + " WHERE seller = ?");
        removeSoldItemStatement = connection.prepareStatement("DELETE FROM "
                + ITEM_TABLE_NAME + " WHERE itemname = ? AND price = ?");
        removeUsersWishesStatement = connection.prepareStatement("DELETE FROM "
                + WISH_TABLE_NAME + " WHERE wisher = ?");
        removeWishStatement = connection.prepareStatement("DELETE FROM "
                + WISH_TABLE_NAME + " WHERE itemname = ? AND price = ? "
                + "AND wisher = ?");
        addItemStatement = connection.prepareStatement("INSERT INTO "
                + ITEM_TABLE_NAME + " (itemname, price, seller) VALUES (?, ?, ?)");
        getItemStatement = connection.prepareStatement("SELECT COUNT(1) FROM "
                + ITEM_TABLE_NAME + " WHERE itemname = ? AND price = ?");
        makeWishStatement = connection.prepareStatement("INSERT INTO "
                + WISH_TABLE_NAME + " (itemname, price, wisher) VALUES (?, ?, ?)");
        incrementSoldStatement = connection.prepareStatement("UPDATE " 
                + USER_TABLE_NAME + " SET sold = sold + 1 WHERE username = ?");
        incrementBoughtStatement = connection.prepareStatement("UPDATE " 
                + USER_TABLE_NAME + " SET bought = bought + 1 WHERE username = ?");
        getAllItemsStatement = connection.prepareStatement("SELECT * FROM " +
                ITEM_TABLE_NAME);
        getAllWishesStatement = connection.prepareStatement("SELECT * FROM " +
                WISH_TABLE_NAME);       
    }

//    private boolean accountsContains(String userName) {
//        boolean contains = false;
//        for (MarketplaceAccount account : accounts) {
//            if (((MarketplaceAccountImpl)account).getCustomerName().equals(userName)) {
//                return true;
//            }
//        }
//        return contains;
//    }
//    
//    private MarketplaceAccount getAccount(String userName) {
//        for(MarketplaceAccount account : accounts){
//            if(((MarketplaceAccountImpl)account).getCustomerName().equals(userName)){
//                return account;
//            }   
//        }
//        return null;
//    }
    
    @Override
    public MarketplaceAccount registerCustomer(MarketplaceClient client,
            String password, String bankAccountName)
            throws RemoteException, RegisterCustomerException {
        try {
            String customerName = client.getName();
            if (!accounts.containsKey(customerName)) {
                MarketplaceAccount account = (MarketplaceAccount) new MarketplaceAccountImpl(client, customerName, bankAccountName, this);
                ResultSet result = null;
                getAccountNameStatement.setString(1, customerName);
                result = getAccountNameStatement.executeQuery();
                accounts.put(customerName, account);
                if (result.next()) {
                    throw new RegisterCustomerException("Account already exists");
                } else {
                    createAccountStatement.setString(1, customerName);
                    createAccountStatement.setString(2, password);
                    createAccountStatement.setString(3, bankAccountName);
                    createAccountStatement.executeUpdate();
                    System.out.println("New account registered: " + customerName);
                }

                return account;
            } else {
                System.out.println("Client tried to create account with already"
                        + " existing name.");
                throw new RegisterCustomerException("Not a unique customer name!");
            }
        } catch (Exception e) {
            System.out.println(e);
            throw new RegisterCustomerException("Something went wrong with"
                    + " registration.");
        }
    }

    @Override
    public boolean unregisterCustomer(String customerName) throws RemoteException {
        MarketplaceAccountImpl account = (MarketplaceAccountImpl) accounts.get(customerName);
        List<Item> customerSales = account.getAvailableSales();
        try {
            deleteAccountStatement.setString(1, customerName);
            deleteAccountStatement.executeUpdate();
            removeItemStatement.setString(1, customerName);
            removeItemStatement.executeUpdate();
            removeUsersWishesStatement.setString(1, customerName);
            removeUsersWishesStatement.executeUpdate();
            for (Item item : customerSales) {
                if (item.getSellerName().equals(customerName)) {
                    items.remove(item);
                }
            }
            for (Wish wish : wishes) {
                if (wish.getWisherName().equals(customerName)) {
                    wishes.remove(wish);
                }
            }
        } catch (SQLException ex) {
            System.out.println("Problem deleting from database");
            ex.printStackTrace();
        }
        accounts.remove(customerName);
        System.out.println("Account removed: " + customerName);
        return true;
    }

    @Override
    public synchronized List<Item> listItems() throws RemoteException {
        return items;
    }

    public synchronized void addProduct(Item product) throws DuplicateItemException {
        try {
            if (!items.contains(product)) {
                addItemStatement.setString(1, product.getName());
                addItemStatement.setFloat(2, product.getPrice());
                addItemStatement.setString(3, product.getSellerName());
                addItemStatement.executeUpdate();
                items.add(product);
                MarketplaceAccountImpl account = (MarketplaceAccountImpl) accounts.get(product.getSellerName());
                //TEST!!!!!!!!!!!!!!!!!!!!!!!1
                System.out.println("Account object: " + account);
                //TEST!!!!!!!!!!!!!!!!!!!!!!!
                account.getAvailableSales().add(product);
                Collections.sort(items);
                for (Wish wish : wishes) {
                    if (wish.getItemName().equals(product.getName())
                            && wish.getPrice() == product.getPrice()) {
                        notifyWish(wish);
                    }
                }
                System.out.println("Product added: " + product.getName());
            } else {
                System.out.println("Can't add duplicate items to sales!");
                throw new DuplicateItemException("Tried to add item that already"
                        + " exists");
            }
        } catch (SQLException ex) {
            System.out.println("Problem adding product to database.");
            ex.printStackTrace();
        }
    }

    public synchronized void buyProduct(Item product, String buyer) {
        if (items.contains(product)) {
            try {
                //Check if the item even exists in database
                
                removeSoldItemStatement.setString(1, product.getName());
                removeSoldItemStatement.setFloat(2, product.getPrice());
                removeSoldItemStatement.executeUpdate();
                //If this product is wished by the buyer we remove it from the
                //database
                removeWishStatement.setString(1, product.getName());
                removeWishStatement.setFloat(2, product.getPrice());
                removeWishStatement.setString(3, buyer);
                removeWishStatement.executeUpdate();
                //Increment the bought and sold counters
                incrementSoldStatement.setString(1, product.getSellerName());
                incrementSoldStatement.executeUpdate();
                incrementBoughtStatement.setString(1, buyer);
                incrementBoughtStatement.executeUpdate();
                
                MarketplaceAccountImpl seller = (MarketplaceAccountImpl) accounts.get(product.getSellerName());
                //TEST!!!!!!!!!!!!!!!!!!!!
                System.out.println("product.getSellerName: " + product.getSellerName());
                System.out.println("Account object: " + seller);
                //TEST!!!!!!!!!!!!!!!!!!!!
                seller.deposit(product.getPrice());
                items.remove(product);
                seller.getAvailableSales().remove(product);
                seller.notifySale(product.getName(), product.getPrice());
            } catch (RemoteException ex) {
                System.out.println("Problem contacting bank when depositing");
            } catch (RejectedException ex) {
                System.out.println("Problem depositing to bank account of seller");
            } catch (SQLException ex) {
                System.out.println("Problem removing product from database");
                ex.printStackTrace();
            }
        }
    }

    public synchronized void addWish(String itemName, float price, String wisherName) {
        try {
            makeWishStatement.setString(1, itemName);
            makeWishStatement.setFloat(2, price);
            makeWishStatement.setString(3, wisherName);
            makeWishStatement.executeUpdate();
            Wish wish = new Wish(itemName, price, wisherName);
            wishes.add(wish);
            //Check if wished item already exists, in that case notify
            if (items.contains(new ItemImpl(itemName, price, null))) {
                notifyWish(wish);
            }
        } catch (SQLException ex) {
            System.out.println("Problem adding wish to database");
            ex.printStackTrace();
        }

    }

    public synchronized void notifyWish(Wish wish) {
        String wisherName = wish.getWisherName();
        MarketplaceAccountImpl wisherAccount = (MarketplaceAccountImpl) accounts.get(wisherName);
        wisherAccount.notifyWishAvailable(wish.getItemName(), wish.getPrice());
    }

    public List<Wish> getWishes() {
        return wishes;
    }

    @Override
    public MarketplaceAccount login(MarketplaceClient client, String name, String password) throws RemoteException,
            BadCredentialsException {
        MarketplaceAccount account = null;
        ResultSet passwordResult = null;
        ResultSet bankNameResult = null;
        try {
            getCredentialsStatement.setString(1, name);
            passwordResult = getCredentialsStatement.executeQuery();
            String correctPassword = "";
            if (passwordResult.next()) {
                correctPassword = passwordResult.getString("password");
            }
            getBankAccountNameStatement.setString(1, name);
            bankNameResult = getBankAccountNameStatement.executeQuery();
            String bankAccountName = "";
            if (bankNameResult.next()) {
                bankAccountName = bankNameResult.getString("bankaccount");
            }
            System.out.println("Password: " + correctPassword);
            System.out.println("Bank account name: " + bankAccountName);
            if (correctPassword.equals(password)) {
                try {
                    account = (MarketplaceAccount) new MarketplaceAccountImpl(client,
                            name, bankAccountName, this);
                    accounts.put(name, account);
                } catch (RegisterCustomerException ex) {
                    throw new BadCredentialsException("Problem creating account"
                            + " object at marketplace");
                }
            } else {
                throw new BadCredentialsException("Wrong user name and/or "
                        + "password.");
            }
            passwordResult.close();
            bankNameResult.close();
        } catch (SQLException e) {
            System.out.println("Problem getting user credentials from database");
            e.printStackTrace();
        }
        System.out.println(name + " logged in.");
        System.out.println("Account object: " + account);
        return account;
    }

    @Override
    public void logout(String name) throws RemoteException {
        accounts.remove(name);
        System.out.println(name + " logged out.");
    }
}
