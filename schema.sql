-- The portal creates this table automatically on startup.
-- This file is provided for inspection/manual setup if desired.
CREATE TABLE IF NOT EXISTS raza_pair_sessions (
  token_hash CHAR(64) NOT NULL PRIMARY KEY,
  phone_number VARCHAR(20) NOT NULL,
  version INT NOT NULL DEFAULT 1,
  encrypted_payload MEDIUMTEXT NOT NULL,
  created_at DATETIME(3) NOT NULL,
  expires_at DATETIME(3) NOT NULL,
  INDEX idx_raza_pair_sessions_expiry (expires_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
