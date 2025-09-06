COPY raw.transacao
FROM STDIN
WITH (FORMAT csv, DELIMITER E'\t', HEADER true, NULL '');