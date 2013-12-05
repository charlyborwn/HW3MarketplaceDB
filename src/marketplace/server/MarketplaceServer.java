
package marketplace.server;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import marketplace.shared.Marketplace;
import java.sql.SQLException;


public class MarketplaceServer {
    
    public static final String MARKETPLACENAME = "Marketplace";
    public static final String DATASOURCE = "MarketplaceDB";
    public static final String DBMS = "derby";    
    private Marketplace marketplace;
            
    /**
    * Here we do some naming
    */
    public MarketplaceServer() throws RemoteException {
            try {
                LocateRegistry.getRegistry(1099).list();
            }
            catch (RemoteException e) {
                System.out.println("will create registry");
                LocateRegistry.createRegistry(1099);
            }
        
           try {
                   marketplace = new MarketplaceImpl();
                   // Register the newly created object at rmiregistry.
                   java.rmi.Naming.rebind("rmi://localhost/" + MARKETPLACENAME, marketplace);
                   System.out.println(marketplace + " is ready.");
           } catch (Exception e) {
                   e.printStackTrace();
           }
    } 
    
    public static void main(String[] args) {
        try {
            MarketplaceServer server = new MarketplaceServer();
        } catch (RemoteException ex) {
            System.out.println("Problem initializing server");
        }
    }    
}
