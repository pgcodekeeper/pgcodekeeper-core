CREATE DOMAIN public.dom2 AS integer NOT NULL
	CONSTRAINT dom2_check CHECK ((VALUE < 1000));