package com.poc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Minimal auth service used only to give SonarQube something real to scan.
 * The hardcoded credential below is intentional — it is the vulnerability
 * this PoC's AI Firewall is meant to catch before it reaches the agent.
 */
public class AuthService {

    // Intentional vulnerability (Sonar rule java:S2068 - hardcoded credentials)
    private static final String DB_PASSWORD = "password=Sup3rS3cr3tDbP4ss!";

    public Connection connect() throws SQLException {
        return DriverManager.getConnection(
                "jdbc:postgresql://localhost:5432/authdb",
                "auth_service_user",
                DB_PASSWORD);
    }

    public boolean authenticate(String username, String suppliedPassword) {
        if (username == null || suppliedPassword == null) {
            return false;
        }
        return username.equals("demo") && suppliedPassword.equals("demo");
    }
}
