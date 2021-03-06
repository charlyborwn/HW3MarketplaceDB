package se.kth.id2212.bankjdbc;

import java.net.MalformedURLException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.sql.SQLException;

public class Server {

    private static final String USAGE = "java bankjdbc.Server [rmi-URL of a bank] "
            + "[database] [dbms: access, derby, pointbase, cloudscape, mysql]";
    private static final String BANK = "Nordea";
    private static final String DATASOURCE = "Banks";
    private static final String DBMS = "derby";

    public Server(String bankName, String datasource, String dbms) 
            throws RemoteException, MalformedURLException{
        try {
            LocateRegistry.getRegistry(1099).list();
        }
        catch (RemoteException e) {
            System.out.println("will create registry");
            LocateRegistry.createRegistry(1099); //IS THIS RIGHT?
        }        
        try {
            Bank bankobj = new BankImpl(datasource, dbms);
            System.out.println("created new BankImpl");
            // Register the newly created object at rmiregistry.
            java.rmi.Naming.rebind(bankName, bankobj);
            System.out.println(bankobj + " is ready.");
        } catch (RemoteException | MalformedURLException |
                ClassNotFoundException | SQLException e) {
            System.out.println("Failed to start bank server.");
            e.printStackTrace();
            System.exit(1);
        }
    }

    public static void main(String[] args) {
        if (args.length > 3 || (args.length > 0 && args[0].equalsIgnoreCase("-h"))) {
            System.out.println(USAGE);
            System.exit(1);
        }

        String bankName = null;
        if (args.length > 0) {
            bankName = args[0];
        } else {
            bankName = BANK;
        }

        String datasource = null;
        if (args.length > 1) {
            datasource = args[1];
        } else {
            datasource = DATASOURCE;
        }

        String dbms = null;
        if (args.length > 2) {
            dbms = args[2];
        } else {
            dbms = DBMS;
        }
        try {
            new Server(bankName, datasource, dbms);
        } catch (RemoteException ex) {
            ex.printStackTrace();
        } catch (MalformedURLException ex) {
            ex.printStackTrace();
        }
    }
}
