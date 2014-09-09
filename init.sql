-- Create a table for incoming mail

CREATE TABLE mail_raw (
  id  SERIAL PRIMARY KEY,
  raw TEXT
);

-- Insert a row into the table;

INSERT INTO mail_raw (raw) VALUES ('my mail');
NOTIFY mail_raw;

--
-- Alternate technique with implicit notifies through triggers
--

CREATE TABLE mail_raw_t (
  id  SERIAL PRIMARY KEY,
  raw TEXT
);

-- This trigger function does the notification
CREATE FUNCTION notify_trigger()
  RETURNS TRIGGER AS $$
BEGIN
  PERFORM pg_notify(TG_TABLE_NAME, NEW.id :: TEXT);
  RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- Create a trigger on the mail_raw_t table and make it run on each new row
CREATE TRIGGER mail_raw_t_notify
AFTER INSERT ON mail_raw_t
FOR EACH ROW
EXECUTE PROCEDURE notify_trigger();

INSERT INTO mail_raw_t (raw) VALUES ('my mail');
