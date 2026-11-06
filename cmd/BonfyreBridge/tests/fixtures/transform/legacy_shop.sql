-- A small legacy shop database in SQLite, written for the transformation
-- evidence tests. Nothing here is a customer's data.
PRAGMA foreign_keys=ON;
CREATE TABLE customer (
  id integer PRIMARY KEY,
  name varchar(80) NOT NULL,
  email varchar(120),
  vip tinyint NOT NULL DEFAULT 0,
  created_at datetime NOT NULL
);
CREATE TABLE product (
  id integer PRIMARY KEY,
  sku char(12) NOT NULL UNIQUE,
  title nvarchar(200) NOT NULL,
  unit_price decimal(10,2) NOT NULL CHECK (unit_price >= 0),
  weight_kg real,
  photo blob
);
CREATE TABLE sales_order (
  id integer PRIMARY KEY,
  customer_id integer NOT NULL REFERENCES customer(id),
  ordered_at datetime NOT NULL,
  note clob
);
CREATE TABLE sales_order_line (
  id integer PRIMARY KEY,
  order_id integer NOT NULL REFERENCES sales_order(id),
  product_id integer NOT NULL REFERENCES product(id),
  qty integer NOT NULL,
  unit_price decimal(10,2) NOT NULL
);
CREATE INDEX idx_line_order ON sales_order_line(order_id);
CREATE VIEW order_total AS
  SELECT order_id, sum(qty * unit_price) AS total FROM sales_order_line GROUP BY order_id;
CREATE TRIGGER customer_name_trim AFTER INSERT ON customer
  BEGIN UPDATE customer SET name = trim(name) WHERE id = new.id; END;

INSERT INTO customer VALUES
  (1, 'alice wong', 'alice@example.test', 1, '2019-03-04 09:15:00'),
  (2, 'Alan Ruiz', NULL, 0, '2020-11-30 17:45:10'),
  (3, 'Zoë Brontë', 'zoe@example.test', 0, '2021-01-01T00:00:00'),
  (4, 'O''Hara & Sons', 'ohara@example.test', 1, '2022-06-15 12:00:00'),
  (5, 'bob', '', 0, '2023-02-28 23:59:59');
INSERT INTO product VALUES
  (10, 'SKU-0000010', 'Steel bracket', 4.50, 0.25, X'00FF10'),
  (11, 'SKU-0000011', 'Brass hinge, "heavy"', 12.00, 1.5, NULL),
  (12, 'SKU-0000012', 'Rubber foot', 0.35, NULL, X'');
INSERT INTO sales_order VALUES
  (100, 1, '2024-01-05 10:00:00', 'rush'),
  (101, 2, '2024-01-06 11:30:00', NULL),
  (102, 4, '2024-02-10 08:05:00', '');
INSERT INTO sales_order_line VALUES
  (1000, 100, 10, 4, 4.50),
  (1001, 100, 11, 1, 12.00),
  (1002, 101, 12, 20, 0.35),
  (1003, 102, 10, 2, 4.25);
