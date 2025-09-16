INSTALL vortex FROM community;
LOAD vortex;
FROM read_vortex('out/messages.vortex');
CREATE TEMP TABLE duckdb AS SELECT unnest(generate_series(1, 10)) AS a, unnest(generate_series(1, 10)) AS b;
FROM duckdb;
COPY duckdb TO 'out/duckdb.vortex' (FORMAT VORTEX);
