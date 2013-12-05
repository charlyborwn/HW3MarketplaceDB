package marketplace.server;

import se.kth.id2212.bankjdbc.Account;
import se.kth.id2212.bankjdbc.Bank;
import se.kth.id2212.bankjdbc.RejectedException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.List;
import marketplace.shared.DuplicateItemException;
import marketplace.shared.Item;
import marketplace.shared.MarketplaceAccount;
import marketplace.shared.MarketplaceClient;
import marketplace.shared.RegisterCustomerException;

public class MarketplaceAccountImpl extends UnicastRemoteObject implements MarketplaceAccount{
    static final String DEFAULT_BANK = "Nordea";
    static String HOST = "localhost";
    private String customerName;
    private String password;
    private int sold;
    private int bought;
    private Account bankAccount;
    private MarketplaceClient client;   //to make callbacks
    private MarketplaceImpl marketplace;
    private List<Item> availableSales;
    
    public MarketplaceAccountImpl(MarketplaceClient client, String customerName, String bankAccountName, 
            MarketplaceImpl marketplace) throws RemoteException, RegisterCustomerException {
        this.customerName = customerName;
        this.client = client;
        setBankAccount(bankAccountName);
        this.marketplace = marketplace;
        System.out.println("MarketplaceAccount succesfully created");
//        try {
//            deposit(1);
//        } catch (RejectedException ex) {
//            System.out.println("Deposit rejected");
//        }
//        System.out.println("Welcome to Marketplace, here's a complimentary dollar");
        availableSales = new ArrayList();
    }
    
    private void setBankAccount(String bankAccountName) throws RegisterCustomerException {
    //Make RMI to bank account possible
        try {
            Bank bank = (Bank) Naming.lookup("rmi://" + HOST + "/" + DEFAULT_BANK);
            bankAccount = bank.getAccount(bankAccountName);
            System.out.println("Bank: " + bank);
            System.out.println("Bank Account: " + bankAccount);
        } catch (Exception e) {
            System.out.println("Failed creating a serverside representation"
                    + "of bank account in marketplace account: " 
                    + e.getMessage());
            throw new RegisterCustomerException("Failed creating a serverside representation"
                    + " of bank account in constructor of marketplace account: " + e.getMessage()); 
        }  
    }

    @Override
    public synchronized void deposit(float value) throws RemoteException, RejectedException {
        bankAccount.deposit(value);
    }

    @Override
    public synchronized void withdraw(float value) throws RemoteException, RejectedException {
        bankAccount.withdraw(value);
    }

    @Override
    public synchronized void addProduct(String productName, float price) 
            throws RemoteException, DuplicateItemException {
        Item product = (Item) new ItemImpl(productName, price, customerName);
        if (marketplace.listItems().contains(product)) {
            throw new RemoteException("Item already exists! Change price or name");
        }
        marketplace.addProduct(product);
        System.out.println(productName + " added.");
    }

    @Override
    public synchronized void buyProduct(Item product) throws RemoteException {
        try {
            withdraw(product.getPrice());
            marketplace.buyProduct(product, customerName);
            String productName = product.getName();
            float productPrice = product.getPrice();
            //see if a wish has to be removed
            List<Wish> wishes = marketplace.getWishes();
            for (Wish wish : wishes) {
                if (wish.getItemName().equals(productName)
                        && wish.getPrice() == productPrice
                        && wish.getWisherName().equals(customerName)) {
                    wishes.remove(wish);
                    System.out.println("Wish for " + productName +
                            " removed for " + customerName + ".");
                }
            }
            System.out.println("Product " + productName + " sold to " 
                    + customerName);
        } catch (RejectedException ex) {
            System.out.println("Bank rejected withdrawal for purchase");
        }
        
    }

    @Override
    public void addWish(String itemName, float maxPrice) throws RemoteException {
        marketplace.addWish(itemName, maxPrice, customerName);
    }


    public void notifyWishAvailable(String itemName, float price) {
        try {
            client.notifyWishAvailable(itemName, price);
        } catch (RemoteException ex) {
            System.out.println("Problem notifying client of available wish");
        }
    }
    
    public void notifySale(String productName, float price) {
        try {
            client.notifySale(productName, price);
            System.out.println(customerName + " notified of sale of " 
                    + productName);
        } catch (RemoteException ex) {
            System.out.println("Problem notifying client of sale");
        }
    }
    
    public List<Item> getAvailableSales() {
        return availableSales;
    }
    
    public String getCustomerName() {
        return customerName;
    }

    public void setClient(MarketplaceClient client) {
        this.client = client;
    }
    
    public void setAvailableSales(List<Item> availableSales) {
        this.availableSales = availableSales;
    }
}
