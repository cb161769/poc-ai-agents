package com.poc;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Minimal real test suite — this is what the testing agent runs (`mvn test`)
 * against the branch a coding agent produces, before the judge ever sees it.
 */
class AuthServiceTest {

    @Test
    void authenticateAcceptsTheDemoUser() {
        AuthService service = new AuthService();
        assertTrue(service.authenticate("demo", "demo"));
    }

    @Test
    void authenticateRejectsWrongPassword() {
        AuthService service = new AuthService();
        assertFalse(service.authenticate("demo", "wrong-password"));
    }

    @Test
    void authenticateRejectsNullCredentials() {
        AuthService service = new AuthService();
        assertFalse(service.authenticate(null, null));
        assertFalse(service.authenticate("demo", null));
        assertFalse(service.authenticate(null, "demo"));
    }
}
