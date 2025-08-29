ALTER TABLE orders
ADD COLUMN shipping_address String DEFAULT '',
ADD COLUMN payment_method Enum8('credit_card' = 1, 'debit_card' = 2, 'paypal' = 3, 'bank_transfer' = 4) DEFAULT 'credit_card',
ADD COLUMN discount_amount Decimal(8, 2) DEFAULT 0.00;
