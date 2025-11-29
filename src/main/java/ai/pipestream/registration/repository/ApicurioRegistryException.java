package ai.pipestream.registration.repository;

/**
 * Exception thrown when operations with Apicurio Registry fail.
 * This allows callers to specifically catch and handle Apicurio-related failures
 * separately from other runtime exceptions.
 */
public class ApicurioRegistryException extends RuntimeException {
    
    private final String serviceName;
    private final String artifactId;
    
    /**
     * Creates a new ApicurioRegistryException with a message.
     *
     * @param message explanation of the failure
     */
    public ApicurioRegistryException(String message) {
        super(message);
        this.serviceName = null;
        this.artifactId = null;
    }
    
    /**
     * Creates a new ApicurioRegistryException with a message and cause.
     *
     * @param message explanation of the failure
     * @param cause the underlying exception that caused this failure
     */
    public ApicurioRegistryException(String message, Throwable cause) {
        super(message, cause);
        this.serviceName = null;
        this.artifactId = null;
    }
    
    /**
     * Creates a new ApicurioRegistryException with context information.
     *
     * @param message explanation of the failure
     * @param serviceName the service name that was being operated on
     * @param artifactId the artifact ID that was being operated on
     * @param cause the underlying exception that caused this failure
     */
    public ApicurioRegistryException(String message, String serviceName, String artifactId, Throwable cause) {
        super(message, cause);
        this.serviceName = serviceName;
        this.artifactId = artifactId;
    }
    
    public String getServiceName() {
        return serviceName;
    }
    
    public String getArtifactId() {
        return artifactId;
    }
}

