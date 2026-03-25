CREATE DATABASE IF NOT EXISTS inventory;
USE inventory;

CREATE TABLE IF NOT EXISTS histalarms (
    id INT AUTO_INCREMENT PRIMARY KEY,
    node_id VARCHAR(50) NOT NULL,
    trap_id INT NOT NULL,
    event_time BIGINT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB;

-- Ensure Debezium can read the table
GRANT ALL PRIVILEGES ON inventory.* TO 'mysqluser'@'%';
FLUSH PRIVILEGES;