package marketplace.server;

public class Wish {
    private String itemName;
    private float price;
    private String wisherName;
    
    public Wish(String itemName, float price, String wisherName) {
        this.itemName = itemName;
        this.price = price;
        this.wisherName = wisherName;
    }

    public String getItemName() {
        return itemName;
    }

    public float getPrice() {
        return price;
    }

    public String getWisherName() {
        return wisherName;
    }
}
