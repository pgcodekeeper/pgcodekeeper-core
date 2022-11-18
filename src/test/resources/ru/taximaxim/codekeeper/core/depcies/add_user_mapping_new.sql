CREATE SERVER myserver VERSION '9.4' FOREIGN DATA WRAPPER mywrapper;

ALTER SERVER myserver OWNER TO user11;

CREATE USER MAPPING FOR testuser SERVER myserver OPTIONS (username 'user1', password 'fff');