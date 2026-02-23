INSERT INTO raw.raw_categories (category_id, name)
VALUES
  (1, 'Electronics'),
  (2, 'Home'),
  (3, 'Fashion'),
  (4, 'Beauty'),
  (5, 'Sports'),
  (6, 'Toys'),
  (7, 'Books'),
  (8, 'Groceries'),
  (9, 'Automotive'),
  (10, 'Office')
ON CONFLICT (name) DO NOTHING;

INSERT INTO raw.raw_products (product_id, name, price, category_id)
VALUES
    (1, 'Smartphone', 699.99, 1),
    (2, 'Laptop', 1299.99, 1),
    (3, 'Headphones', 199.99, 1),
    (4, 'Coffee Maker', 89.99, 2),
    (5, 'Vacuum Cleaner', 149.99, 2),
    (6, 'Sofa', 899.99, 2),
    (7, 'T-Shirt', 19.99, 3),
    (8, 'Jeans', 49.99, 3),
    (9, 'Sneakers', 79.99, 3),
    (10, 'Lipstick', 29.99, 4),
    (11, 'Foundation', 39.99, 4),
    (12, 'Mascara', 24.99, 4),
    (13, 'Football', 29.99, 5),
    (14, 'Tennis Racket', 89.99, 5),
    (15, 'Yoga Mat', 39.99, 5),
    (16, 'Action Figure', 14.99, 6),
    (17, 'Board Game', 29.99, 6),
    (18, 'Doll', 19.99, 6),
    (19, 'Novel', 14.99, 7),
    (20, 'Cookbook', 24.99, 7),
    (21, 'Biography', 19.99, 7),
    (22, 'Organic Apples', 3.99, 8),
    (23, 'Milk', 2.49, 8),
    (24, 'Bread', 1.99, 8),
    (25, 'Car Oil', 29.99, 9),
    (26, 'Tire Inflator', 49.99, 9),
    (27, 'Car Vacuum', 39.99, 9),
    (28, 'Office Chair', 199.99, 10),
    (29, 'Desk Lamp', 49.99, 10),
    (30, 'Notebook', 5.99, 10)
ON CONFLICT (name) DO NOTHING;

INSERT INTO raw.raw_customers (customer_id, name, email)
VALUES
    (1, 'Alice Johnson', 'alice.johnson@example.com'),
    (2, 'Bob Smith', 'bob.smith@example.com'),
    (3, 'Charlie Brown', 'charlie.brown@example.com'),
    (4, 'Diana Prince', 'diana.prince@example.com'),
    (5, 'Ethan Hunt', 'ethan.hunt@example.com'),
    (6, 'Fiona Gallagher', 'fiona.gallagher@example.com'),
    (7, 'George Martin', 'george.martin@example.com'),
    (8, 'Hannah Baker', 'hannah.baker@example.com'),
    (9, 'Ian Fleming', 'ian.fleming@example.com'),
    (10, 'Jane Doe', 'jane.doe@example.com')
ON CONFLICT (email) DO NOTHING;