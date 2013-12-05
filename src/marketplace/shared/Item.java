package marketplace.shared;

public interface Item extends Comparable{
        
    public String getName();
    public void setName(String name);
    public float getPrice();
    public void setPrice(float price);
    public String getSellerName();
    public void setSellerName(String seller);
}
