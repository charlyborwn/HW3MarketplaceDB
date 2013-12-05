package marketplace.shared;

import java.rmi.Remote;
import java.rmi.RemoteException;


public interface MarketplaceClient extends Remote {
    public void notifySale(String productName, float price) throws RemoteException;
    public void notifyWishAvailable(String itemName, float price) 
            throws RemoteException;
    public String getName() throws RemoteException;
}
