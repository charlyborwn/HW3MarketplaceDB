package marketplace.shared;

public class BadCredentialsException extends Exception {
    public BadCredentialsException (String message) {
        super(message);
    }
}
