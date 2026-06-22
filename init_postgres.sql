CREATE TABLE IF NOT EXISTS faturamento_categoria (
    categoria VARCHAR(255) PRIMARY KEY,
    faturamento_total NUMERIC(15, 2),
    ultima_atualizacao TIMESTAMP
);
