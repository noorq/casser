package net.helenus.core;

public interface AuditProvider {

    /**
     * What to record in the database row as the name of the agent causing the mutation.
     * @return a string name that indicates the identity of the operator mutating the data at this time.
     */
    public String operatorName();
}
