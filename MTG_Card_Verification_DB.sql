-- ============================
--  MTG Image Downloader Schema
-- ============================

CREATE TABLE sets (
    id INT AUTO_INCREMENT PRIMARY KEY,
    set_code VARCHAR(16) NOT NULL UNIQUE,
    set_name VARCHAR(255),
    total_cards INT,
    last_checked DATETIME,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE cards (
    id INT AUTO_INCREMENT PRIMARY KEY,
    card_id VARCHAR(64) NOT NULL UNIQUE,     -- Scryfall UUID
    set_code VARCHAR(16) NOT NULL,
    name VARCHAR(255),
    rarity VARCHAR(32),
    collector_number VARCHAR(32),
    json_data JSON,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (set_code) REFERENCES sets(set_code)
        ON UPDATE CASCADE ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE card_images (
    id INT AUTO_INCREMENT PRIMARY KEY,
    card_id VARCHAR(64) NOT NULL,
    set_code VARCHAR(16) NOT NULL,
    image_path TEXT NOT NULL,
    downloaded_at DATETIME NOT NULL,
    UNIQUE KEY unique_card_set (card_id, set_code),
    FOREIGN KEY (card_id) REFERENCES cards(card_id)
        ON UPDATE CASCADE ON DELETE CASCADE,
    FOREIGN KEY (set_code) REFERENCES sets(set_code)
        ON UPDATE CASCADE ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ============================
--  Config Table
-- ============================
CREATE TABLE config (
    config_key VARCHAR(128) PRIMARY KEY,
    config_value TEXT NOT NULL,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
        ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
