package marketplace.shared;

import se.kth.id2212.bankjdbc.RejectedException;
import java.rmi.Remote;
import java.rmi.RemoteException;

public interface MarketplaceAccount extends Remote {
    public void deposit(float value) throws RemoteException, RejectedException;
    public void withdraw(float value) throws RemoteException, RejectedException; 
    public void addProduct(String productName, float price) 
            throws RemoteException, DuplicateItemException;
    public void buyProduct(Item product) throws RemoteException;
    public void addWish(String itemName, float maxPrice) throws RemoteException;
}
