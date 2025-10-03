/********************************************************************************************************************
  Jardinería — Carga de Registros al Data Mart
*********************************************************************************************************************/

SET NOCOUNT ON;
SET XACT_ABORT ON;

USE jardineria_dm;
GO

/* =========================
   0) PRE-FLIGHT CHECKS
========================= */
DECLARE @bPed INT = (SELECT MAX(batch_id) FROM jardineria_stg.stg.pedido);
DECLARE @bDet INT = (SELECT MAX(batch_id) FROM jardineria_stg.stg.detalle_pedido);
DECLARE @bPag INT = (SELECT MAX(batch_id) FROM jardineria_stg.stg.pago);

IF @bPed IS NULL OR @bDet IS NULL
    THROW 50000, 'Staging sin datos de pedido/detalle. Verifique cargas previas.', 1;

/* =========================
   1) CARGA DIMENSIONES
========================= */
BEGIN TRY
BEGIN TRAN;

EXEC dm.usp_Load_DimTiempo;
EXEC dm.usp_Load_DimOficina;
EXEC dm.usp_Load_DimEmpleado;
EXEC dm.usp_Load_DimCategoria;
EXEC dm.usp_Load_DimProducto;
EXEC dm.usp_Load_DimCliente;

COMMIT;
END TRY
BEGIN CATCH
IF @@TRANCOUNT > 0 ROLLBACK;
    THROW;
END CATCH;

/* =========================
   2) MAPAS DE LLAVES
========================= */
-- Unknown keys
DECLARE @unkCliente  INT = (SELECT TOP 1 ClienteKey  FROM dm.DimCliente  WHERE BK_ID_cliente  = -1 AND IsCurrent=1);
DECLARE @unkProd     INT = (SELECT TOP 1 ProductoKey FROM dm.DimProducto WHERE BK_ID_producto = -1);
DECLARE @unkEmp      INT = (SELECT TOP 1 EmpleadoKey FROM dm.DimEmpleado WHERE BK_ID_empleado = -1);
DECLARE @unkOfi      INT = (SELECT TOP 1 OficinaKey  FROM dm.DimOficina  WHERE BK_ID_oficina  = -1);

-- Mapas en memoria (temp) con índices para joins rápidos
IF OBJECT_ID('tempdb..#map_cliente') IS NOT NULL DROP TABLE #map_cliente;
SELECT BK_ID_cliente, ClienteKey = MAX(ClienteKey)
INTO #map_cliente
FROM dm.DimCliente
WHERE IsCurrent = 1
GROUP BY BK_ID_cliente;
CREATE UNIQUE CLUSTERED INDEX IX_map_cliente ON #map_cliente(BK_ID_cliente);

IF OBJECT_ID('tempdb..#map_producto') IS NOT NULL DROP TABLE #map_producto;
SELECT BK_ID_producto, ProductoKey
INTO #map_producto
FROM dm.DimProducto;
CREATE UNIQUE CLUSTERED INDEX IX_map_producto ON #map_producto(BK_ID_producto);

IF OBJECT_ID('tempdb..#map_empleado') IS NOT NULL DROP TABLE #map_empleado;
SELECT BK_ID_empleado, EmpleadoKey, Oficina_BK
INTO #map_empleado
FROM dm.DimEmpleado;
CREATE UNIQUE CLUSTERED INDEX IX_map_empleado ON #map_empleado(BK_ID_empleado);

IF OBJECT_ID('tempdb..#map_oficina') IS NOT NULL DROP TABLE #map_oficina;
SELECT BK_ID_oficina, OficinaKey
INTO #map_oficina
FROM dm.DimOficina;
CREATE UNIQUE CLUSTERED INDEX IX_map_oficina ON #map_oficina(BK_ID_oficina);

/* =========================
   3) BRIDGE (staging) DEL LOTE
========================= */
DECLARE @bPed INT = (SELECT MAX(batch_id) FROM jardineria_stg.stg.pedido);
DECLARE @bDet INT = (SELECT MAX(batch_id) FROM jardineria_stg.stg.detalle_pedido);

IF OBJECT_ID('tempdb..#bridge') IS NOT NULL DROP TABLE #bridge;
SELECT
    DateKeyPedido = CONVERT(INT, CONVERT(CHAR(8), p.fecha_pedido, 112)),
    ID_cliente    = p.ID_cliente,
    ID_producto   = d.ID_producto,
    ID_pedido     = d.ID_pedido,
    numero_linea  = d.numero_linea,
    cantidad      = d.cantidad,
    precio_unidad = d.precio_unidad,
    estado        = p.estado
INTO #bridge
FROM jardineria_stg.stg.detalle_pedido AS d
         JOIN jardineria_stg.stg.pedido AS p
              ON p.BK_ID_pedido = d.ID_pedido
WHERE d.batch_id = @bDet
  AND p.batch_id = @bPed;

CREATE NONCLUSTERED INDEX IX_bridge_pedido ON #bridge(ID_pedido, numero_linea);
CREATE NONCLUSTERED INDEX IX_bridge_date   ON #bridge(DateKeyPedido);

/* =========================
   4) FACTVENTAS
========================= */
DELETE fv
FROM dm.FactVentas AS fv
WHERE EXISTS (
    SELECT 1 FROM #bridge b
    WHERE b.ID_pedido = fv.BK_ID_Pedido
      AND b.numero_linea = fv.NumeroLinea
);

-- Inserción con resolución de llaves en un solo paso
IF COL_LENGTH('dm.FactVentas','CostoUnitario') IS NULL
ALTER TABLE dm.FactVentas ADD CostoUnitario DECIMAL(15,2) NULL;

IF COL_LENGTH('dm.FactVentas','QualityFlag') IS NULL
ALTER TABLE dm.FactVentas ADD QualityFlag TINYINT NOT NULL CONSTRAINT DF_FV_QF DEFAULT(0);


DECLARE @unkProd     INT = (SELECT TOP 1 ProductoKey FROM dm.DimProducto WHERE BK_ID_producto = -1);
DECLARE @unkEmp      INT = (SELECT TOP 1 EmpleadoKey FROM dm.DimEmpleado WHERE BK_ID_empleado = -1);
DECLARE @unkOfi      INT = (SELECT TOP 1 OficinaKey  FROM dm.DimOficina  WHERE BK_ID_oficina  = -1);
IF OBJECT_ID('tempdb..#ins_fv') IS NOT NULL DROP TABLE #ins_fv;
CREATE TABLE #ins_fv (FactVentasID BIGINT NOT NULL);



INSERT INTO dm.FactVentas WITH (TABLOCK) (
    DateKeyPedido, ClienteKey, ProductoKey, EmpleadoKey, OficinaKey,
    BK_ID_Pedido, NumeroLinea, Cantidad, PrecioUnidad, EstadoPedido,
    CostoUnitario, QualityFlag
)
OUTPUT inserted.FactVentasID INTO #ins_fv(FactVentasID)
SELECT
    b.DateKeyPedido,
    ISNULL(mc.ClienteKey,  @unkCliente),
    ISNULL(mp.ProductoKey, @unkProd),
    ISNULL(me.EmpleadoKey, @unkEmp),
    ISNULL(mo.OficinaKey,  @unkOfi),
    b.ID_pedido,
    b.numero_linea,
    b.cantidad,
    b.precio_unidad,
    b.estado,
    ISNULL(dp.PrecioProveedor, 0) AS CostoUnitario,
    (CASE WHEN b.cantidad <= 0 THEN 1 ELSE 0 END)
        + (CASE WHEN b.precio_unidad < 0 THEN 2 ELSE 0 END)
        + (CASE WHEN mc.ClienteKey IS NULL OR mp.ProductoKey IS NULL OR me.EmpleadoKey IS NULL OR mo.OficinaKey IS NULL THEN 4 ELSE 0 END) AS QualityFlag
FROM #bridge AS b
         LEFT JOIN #map_cliente  AS mc ON mc.BK_ID_cliente  = b.ID_cliente
         LEFT JOIN #map_producto AS mp ON mp.BK_ID_producto = b.ID_producto
         LEFT JOIN dm.DimCliente AS dc ON dc.BK_ID_cliente  = b.ID_cliente AND dc.IsCurrent = 1
         LEFT JOIN #map_empleado AS me ON me.BK_ID_empleado = dc.RepVentas_BK
         LEFT JOIN #map_oficina  AS mo ON mo.BK_ID_oficina  = me.Oficina_BK
         LEFT JOIN dm.DimProducto AS dp ON dp.ProductoKey   = mp.ProductoKey

/* =========================
   5) FACTPAGOS
========================= */
    USE jardineria_dm;
GO
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name='dm')
    EXEC('CREATE SCHEMA dm');
GO

IF OBJECT_ID('dm.udf_CleanTrim','FN') IS NOT NULL
DROP FUNCTION dm.udf_CleanTrim;
GO
CREATE FUNCTION dm.udf_CleanTrim(@s NVARCHAR(4000))
    RETURNS NVARCHAR(4000)
                    AS
BEGIN
    IF @s IS NULL RETURN NULL;
    DECLARE @r NVARCHAR(4000);
    -- reemplaza saltos de línea y tabs por espacio
    SET @r = REPLACE(REPLACE(REPLACE(@s, CHAR(13), ' '), CHAR(10), ' '), CHAR(9), ' ');
    -- quita espacios en extremos
    SET @r = LTRIM(RTRIM(@r));
    -- colapsa espacios dobles
    WHILE CHARINDEX('  ', @r) > 0
        SET @r = REPLACE(@r, '  ', ' ');
RETURN @r;
END;
GO


DECLARE @bPag INT = (SELECT MAX(batch_id) FROM jardineria_stg.stg.pago);
DECLARE @unkCliente  INT = (SELECT TOP 1 ClienteKey  FROM dm.DimCliente  WHERE BK_ID_cliente  = -1 AND IsCurrent=1);
DECLARE @unkProd     INT = (SELECT TOP 1 ProductoKey FROM dm.DimProducto WHERE BK_ID_producto = -1);
DECLARE @unkEmp      INT = (SELECT TOP 1 EmpleadoKey FROM dm.DimEmpleado WHERE BK_ID_empleado = -1);
DECLARE @unkOfi      INT = (SELECT TOP 1 OficinaKey  FROM dm.DimOficina  WHERE BK_ID_oficina  = -1);

IF @bPag IS NOT NULL
BEGIN

    DELETE fp
    FROM dm.FactPagos fp
    WHERE EXISTS (
        SELECT 1 FROM jardineria_stg.stg.pago p
        WHERE p.batch_id = @bPag AND p.BK_ID_pago = fp.BK_ID_Pago
    );

INSERT INTO dm.FactPagos (
    DateKeyPago, ClienteKey, BK_ID_Pago, FormaPago, IdTransaccion, TotalPago
)
SELECT
    CONVERT(INT, CONVERT(CHAR(8), p.fecha_pago, 112)) AS DateKeyPago,
    ISNULL(mc.ClienteKey, @unkCliente),
    p.BK_ID_pago,
    dm.udf_CleanTrim(p.forma_pago),
    dm.udf_CleanTrim(p.id_transaccion),
    p.total
FROM jardineria_stg.stg.pago p
         LEFT JOIN #map_cliente mc ON mc.BK_ID_cliente = p.ID_cliente
WHERE p.batch_id = @bPag;
END

/* =========================
   6) VERIFICACIÓN AUTOMÁTICA
========================= */
DECLARE @rows_src BIGINT = (SELECT COUNT(*) FROM #bridge);
DECLARE @rows_ins BIGINT = (SELECT COUNT(*) FROM #ins_fv);

-- a) Conteo insertado vs. fuente del lote
PRINT CONCAT('FactVentas — filas fuente: ', @rows_src, ' / insertadas: ', @rows_ins);
IF @rows_src <> @rows_ins
BEGIN
    PRINT 'ADVERTENCIA: Conteo insertado != conteo fuente (Revise Unknown rows y FKs).';
END

-- b) Fechas cubiertas en DimTiempo
DECLARE @missingDates INT = (
    SELECT COUNT(*) FROM #bridge b
    LEFT JOIN dm.DimTiempo t ON t.DateKey = b.DateKeyPedido
    WHERE t.DateKey IS NULL
);
IF @missingDates > 0
    PRINT CONCAT('ADVERTENCIA: ', @missingDates, ' DateKey(s) no están en DimTiempo.');

-- c) Distribución QualityFlag
SELECT QualityFlag, COUNT(*) AS Registros
FROM dm.FactVentas
WHERE FactVentasID IN (SELECT FactVentasID FROM #ins_fv)
GROUP BY QualityFlag
ORDER BY QualityFlag;

-- d) Uso de claves Unknown
DECLARE @unkCliente  INT = (SELECT TOP 1 ClienteKey  FROM dm.DimCliente  WHERE BK_ID_cliente  = -1 AND IsCurrent=1);
DECLARE @unkProd     INT = (SELECT TOP 1 ProductoKey FROM dm.DimProducto WHERE BK_ID_producto = -1);
DECLARE @unkEmp      INT = (SELECT TOP 1 EmpleadoKey FROM dm.DimEmpleado WHERE BK_ID_empleado = -1);
DECLARE @unkOfi      INT = (SELECT TOP 1 OficinaKey  FROM dm.DimOficina  WHERE BK_ID_oficina  = -1);
SELECT
    Unknown_Cliente  = SUM(CASE WHEN fv.ClienteKey  = @unkCliente THEN 1 ELSE 0 END),
    Unknown_Producto = SUM(CASE WHEN fv.ProductoKey = @unkProd    THEN 1 ELSE 0 END),
    Unknown_Empleado = SUM(CASE WHEN fv.EmpleadoKey = @unkEmp     THEN 1 ELSE 0 END),
    Unknown_Oficina  = SUM(CASE WHEN fv.OficinaKey  = @unkOfi     THEN 1 ELSE 0 END)
FROM dm.FactVentas fv
WHERE fv.FactVentasID IN (SELECT FactVentasID FROM #ins_fv);

-- e) Sumatoria de Importe vs. fuente
SELECT
    MontoFuente = SUM(CAST(b.cantidad AS DECIMAL(18,2)) * CAST(b.precio_unidad AS DECIMAL(18,2))),
    -- Si NO tienes la columna calculada ImporteLinea, usa la versión de abajo (Opción B)
    MontoDM     = (
        SELECT SUM(fv.ImporteLinea)
        FROM dm.FactVentas fv
        WHERE fv.FactVentasID IN (SELECT FactVentasID FROM #ins_fv)
    )
FROM #bridge AS b;


/* =========================
   7) REPORTES RÁPIDOS POST-CARGA
========================= */
-- Conteos generales DM
SELECT * FROM dm.v_DM_Counts;

-- Top 20 registros con bandera de calidad
SELECT TOP 20 *
FROM dm.FactVentas
WHERE FactVentasID IN (SELECT FactVentasID FROM #ins_fv)
  AND QualityFlag <> 0
ORDER BY QualityFlag DESC, FactVentasID DESC;

-- Muestra de 10 filas insertadas
SELECT TOP 10 fv.*
FROM dm.FactVentas fv
         JOIN #ins_fv i ON i.FactVentasID = fv.FactVentasID
ORDER BY fv.FactVentasID DESC;

PRINT 'Carga de registros finalizada.';
