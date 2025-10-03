/* 02_carga_staging.sql */
USE jardineria_stg;
GO

DECLARE @batch_id UNIQUEIDENTIFIER = NEWID();
DECLARE @src NVARCHAR(50) = N'jardineria';

INSERT INTO stg._etl_batch_log(batch_id, src_system, status, msg)
VALUES (@batch_id, @src, 'RUNNING', N'Inicio de carga full desde jardineria');

INSERT INTO stg.oficina_raw
( ID_oficina, Descripcion, ciudad, pais, region, codigo_postal, telefono,
  linea_direccion1, linea_direccion2, _src_system, _batch_id )
SELECT
  o.ID_oficina,
  LTRIM(RTRIM(o.Descripcion)),
  LTRIM(RTRIM(o.ciudad)),
  UPPER(LTRIM(RTRIM(o.pais))),
  LTRIM(RTRIM(o.region)),
  LTRIM(RTRIM(o.codigo_postal)),
  LTRIM(RTRIM(o.telefono)),
  LTRIM(RTRIM(o.linea_direccion1)),
  NULLIF(LTRIM(RTRIM(o.linea_direccion2)), ''),
  @src, @batch_id
FROM jardineria.dbo.oficina o;

INSERT INTO stg.empleado_raw
( ID_empleado, nombre, apellido1, apellido2, extension, email, ID_oficina, ID_jefe, puesto,
  _src_system, _batch_id )
SELECT
  e.ID_empleado,
  LTRIM(RTRIM(e.nombre)),
  LTRIM(RTRIM(e.apellido1)),
  NULLIF(LTRIM(RTRIM(e.apellido2)), ''),
  LTRIM(RTRIM(e.extension)),
  LOWER(LTRIM(RTRIM(e.email))),
  e.ID_oficina,
  e.ID_jefe,
  NULLIF(LTRIM(RTRIM(e.puesto)), ''),
  @src, @batch_id
FROM jardineria.dbo.empleado e;

INSERT INTO stg.categoria_producto_raw
( Id_Categoria, Desc_Categoria, descripcion_texto, descripcion_html, imagen, _src_system, _batch_id )
SELECT
  c.Id_Categoria,
  LTRIM(RTRIM(c.Desc_Categoria)),
  c.descripcion_texto,
  c.descripcion_html,
  c.imagen,
  @src, @batch_id
FROM jardineria.dbo.Categoria_producto c;

INSERT INTO stg.cliente_raw
( ID_cliente, nombre_cliente, nombre_contacto, apellido_contacto, telefono, fax,
  linea_direccion1, linea_direccion2, ciudad, region, pais, codigo_postal,
  ID_empleado_rep_ventas, limite_credito, pais_iso3, _src_system, _batch_id )
SELECT
  cl.ID_cliente,
  LTRIM(RTRIM(cl.nombre_cliente)),
  NULLIF(LTRIM(RTRIM(cl.nombre_contacto)), ''),
  NULLIF(LTRIM(RTRIM(cl.apellido_contacto)), ''),
  LTRIM(RTRIM(cl.telefono)),
  NULLIF(LTRIM(RTRIM(cl.fax)), ''),
  LTRIM(RTRIM(cl.linea_direccion1)),
  NULLIF(LTRIM(RTRIM(cl.linea_direccion2)), ''),
  LTRIM(RTRIM(cl.ciudad)),
  NULLIF(LTRIM(RTRIM(cl.region)), ''),
  UPPER(LTRIM(RTRIM(cl.pais))),
  LTRIM(RTRIM(cl.codigo_postal)),
  cl.ID_empleado_rep_ventas,
  TRY_CONVERT(DECIMAL(15,2), cl.limite_credito),
  CASE UPPER(LTRIM(RTRIM(cl.pais)))
    WHEN 'SPAIN'    THEN 'ESP'
    WHEN 'ESPAÑA'   THEN 'ESP'
    WHEN 'USA'      THEN 'USA'
    WHEN 'EEUU'     THEN 'USA'
    WHEN 'FRANCE'   THEN 'FRA'
    WHEN 'FRANCIA'  THEN 'FRA'
    WHEN 'UNITED KINGDOM' THEN 'GBR'
    WHEN 'INGLATERRA'     THEN 'GBR'
    WHEN 'AUSTRALIA' THEN 'AUS'
    WHEN 'JAPÓN'     THEN 'JPN'
    ELSE NULL
  END,
  @src, @batch_id
FROM jardineria.dbo.cliente cl;

INSERT INTO stg.pedido_raw
( ID_pedido, fecha_pedido, fecha_esperada, fecha_entrega, estado, comentarios, ID_cliente,
  estado_norm, _src_system, _batch_id )
SELECT
  p.ID_pedido,
  TRY_CONVERT(date, p.fecha_pedido),
  TRY_CONVERT(date, p.fecha_esperada),
  TRY_CONVERT(date, p.fecha_entrega),
  UPPER(LTRIM(RTRIM(p.estado))),
  p.comentarios,
  p.ID_cliente,
  CASE UPPER(LTRIM(RTRIM(p.estado)))
    WHEN 'ENTREGADO' THEN 'ENTREGADO'
    WHEN 'PENDIENTE' THEN 'PENDIENTE'
    WHEN 'RECHAZADO' THEN 'RECHAZADO'
    ELSE 'OTRO'
  END,
  @src, @batch_id
FROM jardineria.dbo.pedido p;

INSERT INTO stg.producto_raw
( ID_producto, nombre, Categoria_raw, Categoria_id_norm, dimensiones_raw,
  proveedor, descripcion, cantidad_en_stock, precio_venta, precio_proveedor,
  dim_valor_decimal, dim_min, dim_max, _src_system, _batch_id )
SELECT
  p.ID_producto,
  LTRIM(RTRIM(p.nombre)),
  LTRIM(RTRIM(CAST(p.Categoria AS NVARCHAR(50)))),
  COALESCE(
    TRY_CONVERT(INT, p.Categoria),
    (SELECT c.Id_Categoria
     FROM jardineria.dbo.Categoria_producto c
     WHERE UPPER(LTRIM(RTRIM(c.Desc_Categoria))) COLLATE Latin1_General_CI_AI
           = UPPER(LTRIM(RTRIM(CAST(p.Categoria AS NVARCHAR(50))))) COLLATE Latin1_General_CI_AI)
  ),
  p.dimensiones,
  p.proveedor,
  p.descripcion,
  TRY_CONVERT(INT, p.cantidad_en_stock),
  TRY_CONVERT(DECIMAL(15,2), p.precio_venta),
  TRY_CONVERT(DECIMAL(15,2), p.precio_proveedor),
  TRY_CONVERT(DECIMAL(10,3), REPLACE(p.dimensiones, ',', '.')),
  TRY_CONVERT(DECIMAL(10,3), PARSENAME(REPLACE(p.dimensiones,'/','.'), 2)),
  TRY_CONVERT(DECIMAL(10,3), PARSENAME(REPLACE(p.dimensiones,'/','.'), 1)),
  @src, @batch_id
FROM jardineria.dbo.producto p;

INSERT INTO stg.detalle_pedido_raw
( ID_pedido, ID_producto, cantidad, precio_unidad, numero_linea, _src_system, _batch_id )
SELECT
  d.ID_pedido,
  d.ID_producto,
  TRY_CONVERT(INT, d.cantidad),
  TRY_CONVERT(DECIMAL(15,2), d.precio_unidad),
  TRY_CONVERT(SMALLINT, d.numero_linea),
  @src, @batch_id
FROM jardineria.dbo.detalle_pedido d;

INSERT INTO stg.pago_raw
( ID_cliente, forma_pago, id_transaccion, fecha_pago, total, _src_system, _batch_id )
SELECT
  pg.ID_cliente,
  UPPER(LTRIM(RTRIM(pg.forma_pago))),
  LTRIM(RTRIM(pg.id_transaccion)),
  TRY_CONVERT(date, pg.fecha_pago),
  TRY_CONVERT(DECIMAL(15,2), pg.total),
  @src, @batch_id
FROM jardineria.dbo.pago pg;

UPDATE stg._etl_batch_log
SET ended_at = SYSUTCDATETIME(),
    status   = 'OK',
    rows_ingested =
      (SELECT
          (SELECT COUNT(*) FROM stg.oficina_raw       WHERE _batch_id = @batch_id) +
          (SELECT COUNT(*) FROM stg.empleado_raw      WHERE _batch_id = @batch_id) +
          (SELECT COUNT(*) FROM stg.categoria_producto_raw WHERE _batch_id = @batch_id) +
          (SELECT COUNT(*) FROM stg.cliente_raw       WHERE _batch_id = @batch_id) +
          (SELECT COUNT(*) FROM stg.pedido_raw        WHERE _batch_id = @batch_id) +
          (SELECT COUNT(*) FROM stg.producto_raw      WHERE _batch_id = @batch_id) +
          (SELECT COUNT(*) FROM stg.detalle_pedido_raw WHERE _batch_id = @batch_id) +
          (SELECT COUNT(*) FROM stg.pago_raw          WHERE _batch_id = @batch_id))
WHERE batch_id = @batch_id;

PRINT CONCAT('Carga OK. batch_id=', CONVERT(NVARCHAR(36), @batch_id));