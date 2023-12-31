CREATE TABLE vinkosdb.visitante (
	idVisitas INT AUTO_INCREMENT PRIMARY KEY,
    createdAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    email VARCHAR(100) NOT NULL UNIQUE,
    fechaPrimeraVisita DATETIME,
    fechaUltimaVisita DATETIME,
    visitasTotales INT NOT NULL,
    visitasAnioActual INT NOT NULL,
    visitasMesActual INT NOT NULL
);