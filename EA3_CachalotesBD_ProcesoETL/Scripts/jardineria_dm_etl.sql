/********************************************************************************************************************
  Jardinería — Data Mart (Star Schema) + ETL desde Staging
    - Función dm.udf_CleanTrim para limpieza/normalización de textos.
    - Procedimiento dm.usp_Ensure_UnknownRows para filas 'Unknown' en dimensiones.
    - Dimensiones: normalización de textos (UPPER + CleanTrim) en cargas Tipo 1 y SCD2.
    - FactVentas: agrega CostoUnitario, QualityFlag y MargenLinea calculada; INSERT con 'Unknown' fallback.
    - usp_Load_FactVentas (aliases y DateKey aritmético, sin FORMAT).
*********************************************************************************************************************/

/* =========================================
   0) PREPARACIÓN DE BASE Y ESQUEMAS
========================================= */
IF DB_ID('jardineria_dm') IS NULL
    CREATE DATABASE jardineria_dm;
GO
USE jardineria_dm;
GO

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'dm') EXEC('CREATE SCHEMA dm');
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'ctl_dm') EXEC('CREATE SCHEMA ctl_dm');
GO

/* =========================================
   1) TABLAS DE CONTROL DEL DM
========================================= */
IF OBJECT_ID('ctl_dm.DM_Load') IS NULL
BEGIN
CREATE TABLE ctl_dm.DM_Load (
                                load_id      BIGINT IDENTITY(1,1) PRIMARY KEY,
                                started_at   DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME(),
                                finished_at  DATETIME2(3) NULL,
                                status       VARCHAR(20)  NOT NULL DEFAULT('RUNNING'), -- RUNNING/OK/ERROR
                                note         NVARCHAR(2000) NULL
);
END;
GO

/* =========================================
   2) FUNCIONES Y UTILIDADES
========================================= */
IF OBJECT_ID('dm.udf_CleanTrim','FN') IS NOT NULL DROP FUNCTION dm.udf_CleanTrim;
GO
CREATE FUNCTION dm.udf_CleanTrim(@s NVARCHAR(4000))
    RETURNS NVARCHAR(4000)
                    AS
BEGIN
    IF @s IS NULL RETURN NULL;
    DECLARE @r NVARCHAR(4000) = REPLACE(REPLACE(REPLACE(@s, CHAR(13), ' '), CHAR(10), ' '), CHAR(9), ' ');
    SET @r = LTRIM(RTRIM(@r));
    WHILE CHARINDEX('  ', @r) > 0 SET @r = REPLACE(@r, '  ', ' ');
RETURN @r;
END;
GO

IF OBJECT_ID('dm.usp_Ensure_UnknownRows','P') IS NOT NULL DROP PROCEDURE dm.usp_Ensure_UnknownRows;
GO
CREATE PROCEDURE dm.usp_Ensure_UnknownRows
    AS
BEGIN
    SET NOCOUNT ON;
    -- DimTiempo (DateKey 0)
    IF NOT EXISTS (SELECT 1 FROM dm.DimTiempo WHERE DateKey = 0)
        INSERT INTO dm.DimTiempo(DateKey, Fecha, Anio, Semestre, Trimestre, Mes, NombreMes, Dia, NombreDia, EsFinDeSemana, SemanaISO)
        VALUES (0, '19000101', 1900, 1, 1, 1, N'DESCONOCIDO', 1, N'DESCONOCIDO', 0, 1);

    -- DimCliente (BK -1)
    IF NOT EXISTS (SELECT 1 FROM dm.DimCliente WHERE BK_ID_cliente = -1 AND IsCurrent = 1)
        INSERT INTO dm.DimCliente(BK_ID_cliente, NombreCliente, Ciudad, Pais, IsCurrent, ValidFrom)
        VALUES (-1, N'DESCONOCIDO', N'DESCONOCIDO', N'DESCONOCIDO', 1, SYSUTCDATETIME());

    -- DimEmpleado (BK -1)
    IF NOT EXISTS (SELECT 1 FROM dm.DimEmpleado WHERE BK_ID_empleado = -1)
        INSERT INTO dm.DimEmpleado(BK_ID_empleado, Nombre, Apellido1, Puesto)
        VALUES (-1, N'DESCONOCIDO', N'DESCONOCIDO', N'DESCONOCIDO');

    -- DimOficina (BK -1)
    IF NOT EXISTS (SELECT 1 FROM dm.DimOficina WHERE BK_ID_oficina = -1)
        INSERT INTO dm.DimOficina(BK_ID_oficina, Descripcion, Ciudad, Pais)
        VALUES (-1, N'DESCONOCIDO', N'DESCONOCIDO', N'DESCONOCIDO');

    -- DimProducto (BK -1)
    IF NOT EXISTS (SELECT 1 FROM dm.DimProducto WHERE BK_ID_producto = -1)
        INSERT INTO dm.DimProducto(BK_ID_producto, CodigoProducto, NombreProducto)
        VALUES (-1, N'UNKNOWN', N'DESCONOCIDO');
END;
GO

/* =========================================
   3) TABLAS DIMENSIÓN
========================================= */

-- 3.1 DimTiempo
IF OBJECT_ID('dm.DimTiempo') IS NULL
BEGIN
CREATE TABLE dm.DimTiempo (
                              DateKey            INT NOT NULL PRIMARY KEY,  -- YYYYMMDD
                              Fecha              DATE NOT NULL,
                              Anio               INT  NOT NULL,
                              Semestre           TINYINT NOT NULL,
                              Trimestre          TINYINT NOT NULL,
                              Mes                TINYINT NOT NULL,
                              NombreMes          NVARCHAR(20) NOT NULL,
                              Dia                TINYINT NOT NULL,
                              NombreDia          NVARCHAR(20) NOT NULL,
                              EsFinDeSemana      BIT NOT NULL,
                              SemanaISO          TINYINT NOT NULL
);
END;
GO

-- 3.2 DimCliente (SCD Tipo 2)
IF OBJECT_ID('dm.DimCliente') IS NULL
BEGIN
CREATE TABLE dm.DimCliente (
                               ClienteKey             INT IDENTITY(1,1) PRIMARY KEY,
                               BK_ID_cliente          INT NOT NULL,
                               NombreCliente          NVARCHAR(50) NULL,
                               NombreContacto         NVARCHAR(30) NULL,
                               ApellidoContacto       NVARCHAR(30) NULL,
                               Telefono               NVARCHAR(15) NULL,
                               Fax                    NVARCHAR(15) NULL,
                               Direccion1             NVARCHAR(50) NULL,
                               Direccion2             NVARCHAR(50) NULL,
                               Ciudad                 NVARCHAR(50) NULL,
                               Region                 NVARCHAR(50) NULL,
                               Pais                   NVARCHAR(50) NULL,
                               CodigoPostal           NVARCHAR(10) NULL,
                               RepVentas_BK           INT NULL,
                               LimiteCredito          DECIMAL(15,2) NULL,
                               SrcHash                BINARY(16) NULL,
                               IsCurrent              BIT NOT NULL DEFAULT (1),
                               ValidFrom              DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME(),
                               ValidTo                DATETIME2(3) NULL
);
CREATE INDEX IX_DimCliente_BK ON dm.DimCliente(BK_ID_cliente, IsCurrent);
END;
GO

-- 3.3 DimEmpleado
IF OBJECT_ID('dm.DimEmpleado') IS NULL
BEGIN
CREATE TABLE dm.DimEmpleado (
                                EmpleadoKey        INT IDENTITY(1,1) PRIMARY KEY,
                                BK_ID_empleado     INT NOT NULL,
                                Nombre             NVARCHAR(50) NULL,
                                Apellido1          NVARCHAR(50) NULL,
                                Apellido2          NVARCHAR(50) NULL,
                                Puesto             NVARCHAR(50) NULL,
                                Extension          NVARCHAR(10) NULL,
                                Email              NVARCHAR(100) NULL,
                                Oficina_BK         INT NULL,
                                SrcHash            BINARY(16) NULL
);
CREATE UNIQUE INDEX UX_DimEmpleado_BK ON dm.DimEmpleado(BK_ID_empleado);
END;
GO

-- 3.4 DimOficina
IF OBJECT_ID('dm.DimOficina') IS NULL
BEGIN
CREATE TABLE dm.DimOficina (
                               OficinaKey         INT IDENTITY(1,1) PRIMARY KEY,
                               BK_ID_oficina      INT NOT NULL,
                               Descripcion        NVARCHAR(10) NULL,
                               Ciudad             NVARCHAR(30) NULL,
                               Pais               NVARCHAR(50) NULL,
                               Region             NVARCHAR(50) NULL,
                               CodigoPostal       NVARCHAR(10) NULL,
                               Telefono           NVARCHAR(20) NULL,
                               Direccion1         NVARCHAR(50) NULL,
                               Direccion2         NVARCHAR(50) NULL,
                               SrcHash            BINARY(16) NULL
);
CREATE UNIQUE INDEX UX_DimOficina_BK ON dm.DimOficina(BK_ID_oficina);
END;
GO

-- 3.5 DimCategoria
IF OBJECT_ID('dm.DimCategoria') IS NULL
BEGIN
CREATE TABLE dm.DimCategoria (
                                 CategoriaKey       INT IDENTITY(1,1) PRIMARY KEY,
                                 BK_Id_Categoria    INT NOT NULL,
                                 DescCategoria      NVARCHAR(50) NULL,
                                 SrcHash            BINARY(16) NULL
);
CREATE UNIQUE INDEX UX_DimCategoria_BK ON dm.DimCategoria(BK_Id_Categoria);
END;
GO

-- 3.6 DimProducto
IF OBJECT_ID('dm.DimProducto') IS NULL
BEGIN
CREATE TABLE dm.DimProducto (
                                ProductoKey        INT IDENTITY(1,1) PRIMARY KEY,
                                BK_ID_producto     INT NOT NULL,
                                CodigoProducto     NVARCHAR(15) NULL,
                                NombreProducto     NVARCHAR(70) NULL,
                                CategoriaKey       INT NULL,
                                Categoria_BK       INT NULL,
                                Dimensiones        NVARCHAR(25) NULL,
                                Proveedor          NVARCHAR(50) NULL,
                                PrecioVenta        DECIMAL(15,2) NULL,
                                PrecioProveedor    DECIMAL(15,2) NULL,
                                SrcHash            BINARY(16) NULL,
                                CONSTRAINT FK_DimProducto_DimCategoria FOREIGN KEY (CategoriaKey) REFERENCES dm.DimCategoria(CategoriaKey)
);
CREATE UNIQUE INDEX UX_DimProducto_BK ON dm.DimProducto(BK_ID_producto);
END;
GO

/* =========================================
   4) TABLAS DE HECHOS
========================================= */
IF OBJECT_ID('dm.FactVentas') IS NULL
BEGIN
CREATE TABLE dm.FactVentas (
                               FactVentasID       BIGINT IDENTITY(1,1) PRIMARY KEY,
                               DateKeyPedido      INT NOT NULL,
                               ClienteKey         INT NOT NULL,
                               ProductoKey        INT NOT NULL,
                               EmpleadoKey        INT NULL,
                               OficinaKey         INT NULL,
                               BK_ID_Pedido       INT NOT NULL,
                               NumeroLinea        SMALLINT NOT NULL,
                               Cantidad           INT NOT NULL,
                               PrecioUnidad       DECIMAL(15,2) NOT NULL,
                               CostoUnitario      DECIMAL(15,2) NULL,
                               QualityFlag        TINYINT NOT NULL CONSTRAINT DF_FV_QF DEFAULT(0),
                               MargenLinea        AS (Cantidad * (PrecioUnidad - ISNULL(CostoUnitario,0))) PERSISTED,
                               EstadoPedido       NVARCHAR(15) NULL,
                               CONSTRAINT FK_FV_Tiempo   FOREIGN KEY (DateKeyPedido) REFERENCES dm.DimTiempo(DateKey),
                               CONSTRAINT FK_FV_Cliente  FOREIGN KEY (ClienteKey)    REFERENCES dm.DimCliente(ClienteKey),
                               CONSTRAINT FK_FV_Producto FOREIGN KEY (ProductoKey)   REFERENCES dm.DimProducto(ProductoKey),
                               CONSTRAINT FK_FV_Empleado FOREIGN KEY (EmpleadoKey)   REFERENCES dm.DimEmpleado(EmpleadoKey),
                               CONSTRAINT FK_FV_Oficina  FOREIGN KEY (OficinaKey)    REFERENCES dm.DimOficina(OficinaKey)
);
CREATE INDEX IX_FV_BKPedido ON dm.FactVentas(BK_ID_Pedido);
END;
ELSE
BEGIN
    IF COL_LENGTH('dm.FactVentas','CostoUnitario') IS NULL
ALTER TABLE dm.FactVentas ADD CostoUnitario DECIMAL(15,2) NULL;
IF COL_LENGTH('dm.FactVentas','QualityFlag') IS NULL
ALTER TABLE dm.FactVentas ADD QualityFlag TINYINT NOT NULL CONSTRAINT DF_FV_QF DEFAULT(0);
IF COL_LENGTH('dm.FactVentas','MargenLinea') IS NULL
ALTER TABLE dm.FactVentas ADD MargenLinea AS (Cantidad * (PrecioUnidad - ISNULL(CostoUnitario,0))) PERSISTED;
END;
GO

IF OBJECT_ID('dm.FactPagos') IS NULL
BEGIN
CREATE TABLE dm.FactPagos (
                              FactPagosID        BIGINT IDENTITY(1,1) PRIMARY KEY,
                              DateKeyPago        INT NOT NULL,
                              ClienteKey         INT NOT NULL,
                              BK_ID_Pago         INT NOT NULL,
                              FormaPago          NVARCHAR(40) NULL,
                              IdTransaccion      NVARCHAR(50) NULL,
                              TotalPago          DECIMAL(15,2) NOT NULL,
                              CONSTRAINT FK_FP_Tiempo  FOREIGN KEY (DateKeyPago) REFERENCES dm.DimTiempo(DateKey),
                              CONSTRAINT FK_FP_Cliente FOREIGN KEY (ClienteKey)  REFERENCES dm.DimCliente(ClienteKey)
);
CREATE INDEX IX_FP_BKPago ON dm.FactPagos(BK_ID_Pago);
END;
GO

/* =========================================
   5) ETL: PROCEDIMIENTOS DE CARGA
========================================= */

-- 5.1 DimTiempo
IF OBJECT_ID('dm.usp_Load_DimTiempo','P') IS NOT NULL DROP PROCEDURE dm.usp_Load_DimTiempo;
GO
CREATE PROCEDURE dm.usp_Load_DimTiempo
    AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @minDate DATE, @maxDate DATE;

SELECT
    @minDate = (SELECT MIN(fecha_pedido) FROM jardineria_stg.stg.pedido),
    @maxDate = (SELECT MAX(fecha_pedido) FROM jardineria_stg.stg.pedido);

IF @minDate IS NULL OR @maxDate IS NULL RETURN;

    SET @minDate = DATEADD(DAY, -7, @minDate);
    SET @maxDate = DATEADD(DAY,  7, @maxDate);

    ;WITH d AS (
    SELECT @minDate AS dt
    UNION ALL
    SELECT DATEADD(DAY,1,dt) FROM d WHERE dt < @maxDate
)
     INSERT INTO dm.DimTiempo (DateKey, Fecha, Anio, Semestre, Trimestre, Mes, NombreMes, Dia, NombreDia, EsFinDeSemana, SemanaISO)
SELECT
    CONVERT(INT, CONVERT(CHAR(8), dt, 112)) AS DateKey,
    dt AS Fecha,
    YEAR(dt) AS Anio,
    CASE WHEN MONTH(dt)<=6 THEN 1 ELSE 2 END AS Semestre,
        DATEPART(QUARTER, dt) AS Trimestre,
        MONTH(dt) AS Mes,
        DATENAME(MONTH, dt) AS NombreMes,
        DAY(dt) AS Dia,
        DATENAME(WEEKDAY, dt) AS NombreDia,
        CASE WHEN DATENAME(WEEKDAY, dt) IN (N'Saturday', N'Sunday', N'Sábado', N'Domingo') THEN 1 ELSE 0 END AS EsFinDeSemana,
        DATEPART(ISO_WEEK, dt) AS SemanaISO
    FROM d
    WHERE NOT EXISTS (SELECT 1 FROM dm.DimTiempo t WHERE t.DateKey = CONVERT(INT, CONVERT(CHAR(8), d.dt, 112)))
    OPTION (MAXRECURSION 32767);
END;
GO

-- 5.2 DimOficina
IF OBJECT_ID('dm.usp_Load_DimOficina','P') IS NOT NULL DROP PROCEDURE dm.usp_Load_DimOficina;
GO
CREATE PROCEDURE dm.usp_Load_DimOficina
    AS
BEGIN
    SET NOCOUNT ON;
MERGE dm.DimOficina AS tgt
    USING (
    SELECT
    BK_ID_oficina = o.BK_ID_oficina,
    Descripcion   = dm.udf_CleanTrim(o.Descripcion),
    Ciudad        = UPPER(dm.udf_CleanTrim(o.ciudad)),
    Pais          = UPPER(dm.udf_CleanTrim(o.pais)),
    Region        = UPPER(dm.udf_CleanTrim(o.region)),
    CodigoPostal  = dm.udf_CleanTrim(o.codigo_postal),
    Telefono      = dm.udf_CleanTrim(o.telefono),
    Direccion1    = dm.udf_CleanTrim(o.linea_direccion1),
    Direccion2    = dm.udf_CleanTrim(o.linea_direccion2),
    SrcHash       = o.src_hash
    FROM jardineria_stg.stg.oficina o
    WHERE o.batch_id = (SELECT MAX(batch_id) FROM jardineria_stg.stg.oficina)
    ) AS src
    ON tgt.BK_ID_oficina = src.BK_ID_oficina
    WHEN MATCHED AND (ISNULL(tgt.SrcHash,0x0) <> ISNULL(src.SrcHash,0x0))
    THEN UPDATE SET Descripcion=src.Descripcion, Ciudad=src.Ciudad, Pais=src.Pais, Region=src.Region,
             CodigoPostal=src.CodigoPostal, Telefono=src.Telefono, Direccion1=src.Direccion1, Direccion2=src.Direccion2,
             SrcHash=src.SrcHash
             WHEN NOT MATCHED BY TARGET
             THEN INSERT (BK_ID_oficina, Descripcion, Ciudad, Pais, Region, CodigoPostal, Telefono, Direccion1, Direccion2, SrcHash)
         VALUES (src.BK_ID_oficina, src.Descripcion, src.Ciudad, src.Pais, src.Region, src.CodigoPostal, src.Telefono, src.Direccion1, src.Direccion2, src.SrcHash);
END;
GO

-- 5.3 DimEmpleado
IF OBJECT_ID('dm.usp_Load_DimEmpleado','P') IS NOT NULL DROP PROCEDURE dm.usp_Load_DimEmpleado;
GO
CREATE PROCEDURE dm.usp_Load_DimEmpleado
    AS
BEGIN
    SET NOCOUNT ON;
MERGE dm.DimEmpleado AS tgt
    USING (
    SELECT
    BK_ID_empleado = e.BK_ID_empleado,
    Nombre   = dm.udf_CleanTrim(e.nombre),
    Apellido1= dm.udf_CleanTrim(e.apellido1),
    Apellido2= dm.udf_CleanTrim(e.apellido2),
    Puesto   = dm.udf_CleanTrim(e.puesto),
    Extension= dm.udf_CleanTrim(e.extension),
    Email    = dm.udf_CleanTrim(e.email),
    Oficina_BK=e.ID_oficina,
    SrcHash  = e.src_hash
    FROM jardineria_stg.stg.empleado e
    WHERE e.batch_id = (SELECT MAX(batch_id) FROM jardineria_stg.stg.empleado)
    ) AS src
    ON tgt.BK_ID_empleado = src.BK_ID_empleado
    WHEN MATCHED AND (ISNULL(tgt.SrcHash,0x0) <> ISNULL(src.SrcHash,0x0))
    THEN UPDATE SET Nombre=src.Nombre, Apellido1=src.Apellido1, Apellido2=src.Apellido2,
             Puesto=src.Puesto, Extension=src.Extension, Email=src.Email,
             Oficina_BK=src.Oficina_BK, SrcHash=src.SrcHash
             WHEN NOT MATCHED BY TARGET
             THEN INSERT (BK_ID_empleado, Nombre, Apellido1, Apellido2, Puesto, Extension, Email, Oficina_BK, SrcHash)
         VALUES (src.BK_ID_empleado, src.Nombre, src.Apellido1, src.Apellido2, src.Puesto, src.Extension, src.Email, src.Oficina_BK, src.SrcHash);
END;
GO

-- 5.4 DimCategoria
IF OBJECT_ID('dm.usp_Load_DimCategoria','P') IS NOT NULL DROP PROCEDURE dm.usp_Load_DimCategoria;
GO
CREATE PROCEDURE dm.usp_Load_DimCategoria
    AS
BEGIN
    SET NOCOUNT ON;
    IF OBJECT_ID('jardineria_stg.stg.categoria_producto') IS NULL RETURN;

MERGE dm.DimCategoria AS tgt
    USING (
    SELECT
    BK_Id_Categoria = c.BK_Id_Categoria,
    DescCategoria   = dm.udf_CleanTrim(c.Desc_Categoria),
    SrcHash         = c.src_hash
    FROM jardineria_stg.stg.categoria_producto c
    WHERE c.batch_id=(SELECT MAX(batch_id) FROM jardineria_stg.stg.categoria_producto)
    ) AS src
    ON tgt.BK_Id_Categoria = src.BK_Id_Categoria
    WHEN MATCHED AND (ISNULL(tgt.SrcHash,0x0) <> ISNULL(src.SrcHash,0x0))
    THEN UPDATE SET DescCategoria=src.DescCategoria, SrcHash=src.SrcHash
             WHEN NOT MATCHED BY TARGET
             THEN INSERT (BK_Id_Categoria, DescCategoria, SrcHash)
         VALUES (src.BK_Id_Categoria, src.DescCategoria, src.SrcHash);
END;
GO

-- 5.5 DimProducto
IF OBJECT_ID('dm.usp_Load_DimProducto','P') IS NOT NULL DROP PROCEDURE dm.usp_Load_DimProducto;
GO
CREATE PROCEDURE dm.usp_Load_DimProducto
    AS
BEGIN
    SET NOCOUNT ON;
MERGE dm.DimProducto AS tgt
    USING (
    SELECT
    BK_ID_producto = p.BK_ID_producto,
    CodigoProducto = dm.udf_CleanTrim(p.CodigoProducto),
    NombreProducto = dm.udf_CleanTrim(p.nombre),
    Categoria_BK   = p.Categoria,
    CategoriaKey   = (SELECT TOP 1 CategoriaKey FROM dm.DimCategoria dc WHERE dc.BK_Id_Categoria=p.Categoria),
    Dimensiones    = dm.udf_CleanTrim(p.dimensiones),
    Proveedor      = dm.udf_CleanTrim(p.proveedor),
    PrecioVenta    = p.precio_venta,
    PrecioProveedor= p.precio_proveedor,
    SrcHash        = p.src_hash
    FROM jardineria_stg.stg.producto p
    WHERE p.batch_id = (SELECT MAX(batch_id) FROM jardineria_stg.stg.producto)
    ) AS src
    ON tgt.BK_ID_producto = src.BK_ID_producto
    WHEN MATCHED AND (ISNULL(tgt.SrcHash,0x0) <> ISNULL(src.SrcHash,0x0))
    THEN UPDATE SET CodigoProducto=src.CodigoProducto, NombreProducto=src.NombreProducto,
             CategoriaKey=src.CategoriaKey, Categoria_BK=src.Categoria_BK,
             Dimensiones=src.Dimensiones, Proveedor=src.Proveedor,
             PrecioVenta=src.PrecioVenta, PrecioProveedor=src.PrecioProveedor,
             SrcHash=src.SrcHash
             WHEN NOT MATCHED BY TARGET
             THEN INSERT (BK_ID_producto, CodigoProducto, NombreProducto, CategoriaKey, Categoria_BK,
         Dimensiones, Proveedor, PrecioVenta, PrecioProveedor, SrcHash)
         VALUES (src.BK_ID_producto, src.CodigoProducto, src.NombreProducto, src.CategoriaKey, src.Categoria_BK,
             src.Dimensiones, src.Proveedor, src.PrecioVenta, src.PrecioProveedor, src.SrcHash);
END;
GO

-- 5.6 DimCliente (SCD2 con tabla variable + normalización)
IF OBJECT_ID('dm.usp_Load_DimCliente','P') IS NOT NULL DROP PROCEDURE dm.usp_Load_DimCliente;
GO
CREATE PROCEDURE dm.usp_Load_DimCliente
    AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @now DATETIME2(3) = SYSUTCDATETIME();

    DECLARE @src TABLE (
        BK_ID_cliente     INT           NOT NULL PRIMARY KEY,
        NombreCliente     NVARCHAR(50)  NULL,
        NombreContacto    NVARCHAR(30)  NULL,
        ApellidoContacto  NVARCHAR(30)  NULL,
        Telefono          NVARCHAR(15)  NULL,
        Fax               NVARCHAR(15)  NULL,
        Direccion1        NVARCHAR(50)  NULL,
        Direccion2        NVARCHAR(50)  NULL,
        Ciudad            NVARCHAR(50)  NULL,
        Region            NVARCHAR(50)  NULL,
        Pais              NVARCHAR(50)  NULL,
        CodigoPostal      NVARCHAR(10)  NULL,
        RepVentas_BK      INT           NULL,
        LimiteCredito     DECIMAL(15,2) NULL,
        SrcHash           BINARY(16)    NULL
    );

INSERT INTO @src
SELECT
    c.BK_ID_cliente,
    dm.udf_CleanTrim(c.nombre_cliente),
    dm.udf_CleanTrim(c.nombre_contacto),
    dm.udf_CleanTrim(c.apellido_contacto),
    dm.udf_CleanTrim(c.telefono),
    dm.udf_CleanTrim(c.fax),
    dm.udf_CleanTrim(c.linea_direccion1),
    dm.udf_CleanTrim(c.linea_direccion2),
    UPPER(dm.udf_CleanTrim(c.ciudad)),
    UPPER(dm.udf_CleanTrim(c.region)),
    UPPER(dm.udf_CleanTrim(c.pais)),
    dm.udf_CleanTrim(c.codigo_postal),
    c.ID_empleado_rep_ventas,
    c.limite_credito,
    c.src_hash
FROM jardineria_stg.stg.cliente AS c
WHERE c.batch_id = (SELECT MAX(batch_id) FROM jardineria_stg.stg.cliente);

-- Cerrar versiones vigentes que cambiaron
UPDATE d
SET d.IsCurrent = 0,
    d.ValidTo   = @now
    FROM dm.DimCliente AS d
    JOIN @src          AS s
ON s.BK_ID_cliente = d.BK_ID_cliente
WHERE d.IsCurrent = 1
  AND ISNULL(d.SrcHash, 0x0) <> ISNULL(s.SrcHash, 0x0);

-- Insertar nuevas versiones para BK cerrados
INSERT INTO dm.DimCliente (
    BK_ID_cliente, NombreCliente, NombreContacto, ApellidoContacto, Telefono, Fax,
    Direccion1, Direccion2, Ciudad, Region, Pais, CodigoPostal,
    RepVentas_BK, LimiteCredito, SrcHash,
    IsCurrent, ValidFrom, ValidTo
)
SELECT
    s.BK_ID_cliente, s.NombreCliente, s.NombreContacto, s.ApellidoContacto, s.Telefono, s.Fax,
    s.Direccion1, s.Direccion2, s.Ciudad, s.Region, s.Pais, s.CodigoPostal,
    s.RepVentas_BK, s.LimiteCredito, s.SrcHash,
    1, @now, NULL
FROM @src AS s
WHERE EXISTS (
    SELECT 1
    FROM dm.DimCliente AS d
    WHERE d.BK_ID_cliente = s.BK_ID_cliente AND d.ValidTo = @now
);

-- Insertar BK nuevos (sin versión vigente)
INSERT INTO dm.DimCliente (
    BK_ID_cliente, NombreCliente, NombreContacto, ApellidoContacto, Telefono, Fax,
    Direccion1, Direccion2, Ciudad, Region, Pais, CodigoPostal,
    RepVentas_BK, LimiteCredito, SrcHash,
    IsCurrent, ValidFrom, ValidTo
)
SELECT
    s.BK_ID_cliente, s.NombreCliente, s.NombreContacto, s.ApellidoContacto, s.Telefono, s.Fax,
    s.Direccion1, s.Direccion2, s.Ciudad, s.Region, s.Pais, s.CodigoPostal,
    s.RepVentas_BK, s.LimiteCredito, s.SrcHash,
    1, @now, NULL
FROM @src AS s
WHERE NOT EXISTS (
    SELECT 1
    FROM dm.DimCliente AS d
    WHERE d.BK_ID_cliente = s.BK_ID_cliente
      AND d.IsCurrent = 1
);
END;
GO

-- 5.7 FactVentas
IF OBJECT_ID('dm.usp_Load_FactVentas','P') IS NOT NULL DROP PROCEDURE dm.usp_Load_FactVentas;
GO
CREATE PROCEDURE dm.usp_Load_FactVentas
    AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @batch_det INT = (SELECT MAX(batch_id) FROM jardineria_stg.stg.detalle_pedido);
    DECLARE @batch_ped INT = (SELECT MAX(batch_id) FROM jardineria_stg.stg.pedido);

    ;WITH ped AS (
    SELECT
        BK_ID_pedido = pe.BK_ID_pedido,
        fecha_pedido = pe.fecha_pedido,
        estado       = pe.estado,
        ID_cliente   = pe.ID_cliente
    FROM jardineria_stg.stg.pedido AS pe
    WHERE pe.batch_id = @batch_ped
),
          det AS (
              SELECT
                  BK_ID_detalle = d.BK_ID_detalle,
                  ID_pedido     = d.ID_pedido,
                  ID_producto   = d.ID_producto,
                  cantidad      = d.cantidad,
                  precio_unidad = d.precio_unidad,
                  numero_linea  = d.numero_linea
              FROM jardineria_stg.stg.detalle_pedido AS d
              WHERE d.batch_id = @batch_det
          ),
          bridge AS (
              SELECT
                  DateKeyPedido = (YEAR(p.fecha_pedido)*10000 + MONTH(p.fecha_pedido)*100 + DAY(p.fecha_pedido)),
                  ID_cliente    = p.ID_cliente,
                  ID_producto   = d.ID_producto,
                  ID_pedido     = d.ID_pedido,
                  numero_linea  = d.numero_linea,
                  cantidad      = d.cantidad,
                  precio_unidad = d.precio_unidad,
                  estado        = p.estado
              FROM det AS d
                       INNER JOIN ped AS p
                                  ON p.BK_ID_pedido = d.ID_pedido
          )
     INSERT INTO dm.FactVentas (
        DateKeyPedido, ClienteKey, ProductoKey, EmpleadoKey, OficinaKey,
        BK_ID_Pedido, NumeroLinea, Cantidad, PrecioUnidad, EstadoPedido,
        CostoUnitario, QualityFlag
    )
SELECT
    b.DateKeyPedido,
    ISNULL(c.ClienteKey,  (SELECT TOP 1 ClienteKey  FROM dm.DimCliente  WHERE BK_ID_cliente = -1 AND IsCurrent=1)),
    ISNULL(pr.ProductoKey,(SELECT TOP 1 ProductoKey FROM dm.DimProducto WHERE BK_ID_producto = -1)),
    ISNULL(e.EmpleadoKey, (SELECT TOP 1 EmpleadoKey FROM dm.DimEmpleado WHERE BK_ID_empleado = -1)),
    ISNULL(o.OficinaKey,  (SELECT TOP 1 OficinaKey  FROM dm.DimOficina  WHERE BK_ID_oficina = -1)),
    b.ID_pedido,
    b.numero_linea,
    b.cantidad,
    b.precio_unidad,
    b.estado,
    ISNULL(pr.PrecioProveedor, 0) AS CostoUnitario,
    (CASE WHEN b.cantidad <= 0 THEN 1 ELSE 0 END)
        + (CASE WHEN b.precio_unidad < 0 THEN 2 ELSE 0 END)
        + (CASE WHEN c.ClienteKey IS NULL OR pr.ProductoKey IS NULL OR e.EmpleadoKey IS NULL OR o.OficinaKey IS NULL THEN 4 ELSE 0 END) AS QualityFlag
FROM bridge AS b
         LEFT JOIN dm.DimCliente  AS c  ON c.BK_ID_cliente = b.ID_cliente AND c.IsCurrent = 1
         LEFT JOIN dm.DimProducto AS pr ON pr.BK_ID_producto = b.ID_producto
         LEFT JOIN dm.DimEmpleado AS e  ON e.BK_ID_empleado = (
    SELECT TOP 1 c2.RepVentas_BK
    FROM dm.DimCliente AS c2
    WHERE c2.BK_ID_cliente = b.ID_cliente AND c2.IsCurrent = 1
    ORDER BY c2.ClienteKey DESC
)
         LEFT JOIN dm.DimOficina  AS o  ON o.BK_ID_oficina = e.Oficina_BK;
END;
GO

-- 5.8 FactPagos
IF OBJECT_ID('dm.usp_Load_FactPagos','P') IS NOT NULL DROP PROCEDURE dm.usp_Load_FactPagos;
GO
CREATE PROCEDURE dm.usp_Load_FactPagos
    AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @batch INT = (SELECT MAX(batch_id) FROM jardineria_stg.stg.pago);
INSERT INTO dm.FactPagos (DateKeyPago, ClienteKey, BK_ID_Pago, FormaPago, IdTransaccion, TotalPago)
SELECT
    CONVERT(INT, CONVERT(CHAR(8), p.fecha_pago, 112)) AS DateKeyPago,
    ISNULL((SELECT TOP 1 c.ClienteKey FROM dm.DimCliente c WHERE c.BK_ID_cliente=p.ID_cliente AND c.IsCurrent=1 ORDER BY c.ClienteKey DESC),
           (SELECT TOP 1 c2.ClienteKey FROM dm.DimCliente c2 WHERE c2.BK_ID_cliente=-1 AND c2.IsCurrent=1)),
    p.BK_ID_pago,
    dm.udf_CleanTrim(p.forma_pago),
    dm.udf_CleanTrim(p.id_transaccion),
    p.total
FROM jardineria_stg.stg.pago p
WHERE p.batch_id = @batch;
END;
GO

/* =========================================
   6) VISTAS DE VALIDACIÓN
========================================= */
IF OBJECT_ID('dm.v_DM_Counts') IS NOT NULL DROP VIEW dm.v_DM_Counts;
GO
CREATE VIEW dm.v_DM_Counts AS
SELECT 'DimTiempo'   AS Tabla, COUNT(*) AS Registros FROM dm.DimTiempo
UNION ALL SELECT 'DimCliente', COUNT(*) FROM dm.DimCliente WHERE IsCurrent=1
UNION ALL SELECT 'DimEmpleado', COUNT(*) FROM dm.DimEmpleado
UNION ALL SELECT 'DimOficina', COUNT(*) FROM dm.DimOficina
UNION ALL SELECT 'DimCategoria', COUNT(*) FROM dm.DimCategoria
UNION ALL SELECT 'DimProducto', COUNT(*) FROM dm.DimProducto
UNION ALL SELECT 'FactVentas', COUNT(*)  FROM dm.FactVentas
UNION ALL SELECT 'FactPagos', COUNT(*)   FROM dm.FactPagos;
GO

IF OBJECT_ID('dm.v_FactVentas_FKChecks') IS NOT NULL DROP VIEW dm.v_FactVentas_FKChecks;
GO
CREATE VIEW dm.v_FactVentas_FKChecks AS
SELECT TOP 100
    fv.FactVentasID, fv.BK_ID_Pedido, fv.NumeroLinea,
       fv.ClienteKey, fv.ProductoKey, fv.EmpleadoKey, fv.OficinaKey,
       fv.QualityFlag
FROM dm.FactVentas fv
         LEFT JOIN dm.DimCliente  dc ON dc.ClienteKey  = fv.ClienteKey
         LEFT JOIN dm.DimProducto dp ON dp.ProductoKey = fv.ProductoKey
WHERE (dc.ClienteKey IS NULL OR dp.ProductoKey IS NULL);
GO

/* =========================================
   7) ORQUESTADOR
========================================= */
IF OBJECT_ID('dm.usp_Load_DataMart','P') IS NOT NULL DROP PROCEDURE dm.usp_Load_DataMart;
GO
CREATE PROCEDURE dm.usp_Load_DataMart
    AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @load_id BIGINT;
INSERT INTO ctl_dm.DM_Load(status, note) VALUES ('RUNNING','Carga completa del DM desde staging (v2)');
SET @load_id = SCOPE_IDENTITY();

BEGIN TRY
EXEC dm.usp_Load_DimTiempo;
EXEC dm.usp_Ensure_UnknownRows;
EXEC dm.usp_Load_DimOficina;
EXEC dm.usp_Load_DimEmpleado;
EXEC dm.usp_Load_DimCategoria;
EXEC dm.usp_Load_DimProducto;
EXEC dm.usp_Load_DimCliente;
EXEC dm.usp_Load_FactVentas;
EXEC dm.usp_Load_FactPagos;

UPDATE ctl_dm.DM_Load SET status='OK', finished_at=SYSUTCDATETIME() WHERE load_id=@load_id;
END TRY
BEGIN CATCH
UPDATE ctl_dm.DM_Load SET status='ERROR', finished_at=SYSUTCDATETIME(), note=ERROR_MESSAGE() WHERE load_id=@load_id;
THROW;
END CATCH
END;
GO

/* =========================================
   8) EJECUCIÓN DE EJEMPLO
========================================= */

EXEC dm.usp_Load_DataMart;
SELECT * FROM dm.v_DM_Counts;
SELECT * FROM dm.v_FactVentas_FKChecks;

