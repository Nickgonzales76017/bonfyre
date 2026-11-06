-- A small Odoo-shaped partner table in SQLite, written for the ERP evidence
-- test. Nothing here is a customer's data.
CREATE TABLE res_partner (
  id integer PRIMARY KEY,
  name varchar NOT NULL,
  is_company boolean NOT NULL DEFAULT 0,
  vat varchar,
  active boolean NOT NULL DEFAULT 1,
  comment text
);
INSERT INTO res_partner VALUES
  (1, 'Acme Fabrication', 1, 'US-12-3456789', 1, 'key account'),
  (2, 'Dana Ortiz', 0, NULL, 1, NULL),
  (3, 'Old Mill Supply', 1, 'US-98-7654321', 0, 'closed 2021'),
  (10, 'Ünal Kaya', 0, '', 1, '');
