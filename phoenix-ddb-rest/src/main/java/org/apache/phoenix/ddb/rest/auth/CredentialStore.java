package org.apache.phoenix.ddb.rest.auth;

/**
 * Interface for credential storage and retrieval.
 * <p>
 * Implementers can use any storage mechanism they prefer:
 * - Database (Phoenix, MySQL, PostgreSQL etc.)
 * - File-based storage (properties files, JSON etc.)
 * - LDAP/Active Directory
 * - In-memory storage (for testing)
 * - External services (Vault, Secret Service etc.)
 * </p>
 */
public interface CredentialStore {
    
    /**
     * Retrieves user credentials by access key ID.
     * 
     * @param accessKeyId The AWS-style access key ID
     * @return UserCredentials if found and valid, null otherwise
     */
    UserCredentials getCredentials(String accessKeyId);
} 