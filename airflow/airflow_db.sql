CREATE DATABASE airflow_db CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
GRANT ALL PRIVILEGES ON airflow_db.* TO 'hw_airflow'@'localhost';
FLUSH PRIVILEGES;
