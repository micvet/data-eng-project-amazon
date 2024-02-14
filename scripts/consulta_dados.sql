SELECT * FROM fires;
SELECT * FROM deforestation;
SELECT * FROM el_nino;

--Estados com maiores focos de incência, em ordem descrescente
SELECT state, SUM(firespots) AS total_firespots
FROM fires
GROUP BY state
ORDER BY total_firespots DESC;

--Número total de foco de incêncio por estado
SELECT state, SUM(firespots) FROM fires
WHERE state = 'PARA'
GROUP BY state;

-- Datas com maiores focos de incêndio
SELECT date, SUM(firespots) AS total_firespots
FROM fires
GROUP BY date
ORDER BY total_firespots DESC;

