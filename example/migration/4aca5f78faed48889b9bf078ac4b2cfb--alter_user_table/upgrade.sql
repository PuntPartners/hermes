ALTER TABLE user
ADD COLUMN email String DEFAULT '',
ADD COLUMN age UInt8 DEFAULT 0,
ADD COLUMN created_at DateTime DEFAULT now();
