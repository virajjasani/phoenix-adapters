package org.apache.phoenix.ddb.rest.auth;

/**
 * Class for the User credentials data.
 * <p>
 * This is a simple immutable data class that holds the essential
 * information needed for authentication:
 * - userName: Human-readable username or identifier
 * - accessKeyId: access key
 * - secretKey: Secret key
 * </p>
 */
public class UserCredentials {

    private final String userName;
    private final String accessKeyId;
    private final String secretKey;

    /**
     * Creates new user credentials.
     * 
     * @param userName Human-readable username or identifier.
     * @param accessKeyId Access key ID.
     * @param secretKey Secret key.
     */
    public UserCredentials(String userName, String accessKeyId, String secretKey) {
        this.userName = userName;
        this.accessKeyId = accessKeyId;
        this.secretKey = secretKey;
    }
    
    /**
     * @return The human-readable username or identifier.
     */
    public String getUserName() { 
        return userName; 
    }
    
    /**
     * @return The AWS-style access key ID.
     */
    public String getAccessKeyId() { 
        return accessKeyId; 
    }
    
    /**
     * @return The secret key.
     */
    public String getSecretKey() { 
        return secretKey; 
    }
}