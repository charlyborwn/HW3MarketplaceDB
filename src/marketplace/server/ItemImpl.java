package marketplace.server;

import java.io.Serializable;
import marketplace.shared.Item;


public class ItemImpl implements Item, Serializable {
    private String name;
    private float price;
    private String sellerName;
    
    public ItemImpl (String name, float price, String sellerName) {
        this.name = name;
        this.price = price;
        this.sellerName = sellerName;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public float getPrice() {
        return price;
    }

    public void setPrice(float price) {
        this.price = price;
    }

    public String getSellerName() {
        return sellerName;
    }

    public void setSellerName(String seller) {
        this.sellerName = seller;
    }
    
    @Override
    public boolean equals(Object o) {
        try {
            Item otherItem = (Item) o;
            if (name.equals(otherItem.getName()) && 
                    price == otherItem.getPrice()) {
                return true;
            }
            else {
                return false;
            }
        } catch(ClassCastException ex) {
            return false;
        }
    }

    @Override
    public int compareTo(Object o) {
        int result = 0;
        try {
            Item otherItem = (Item) o;
            result += 1000000000 * this.getName().
                    compareToIgnoreCase(otherItem.getName());
            result += this.getPrice() - otherItem.getPrice();
        } catch(ClassCastException ex) {
        }
        

        return result;
        
    }
}
