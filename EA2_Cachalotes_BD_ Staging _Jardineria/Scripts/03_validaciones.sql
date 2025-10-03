/* 03_validaciones.sql */
USE jardineria_stg;
GO

DECLARE @last_batch UNIQUEIDENTIFIER =
( SELECT TOP(1) batch_id FROM stg._etl_batch_log WHERE status='OK' ORDER BY started_at DESC );

SELECT 'oficina' AS tabla,
       (SELECT COUNT(*) FROM jardineria.dbo.oficina) AS src,
       (SELECT COUNT(*) FROM stg.oficina_raw WHERE _batch_id=@last_batch) AS stg;

SELECT 'empleado' AS tabla,
       (SELECT COUNT(*) FROM jardineria.dbo.empleado),
       (SELECT COUNT(*) FROM stg.empleado_raw WHERE _batch_id=@last_batch);

SELECT 'Categoria_producto' AS tabla,
       (SELECT COUNT(*) FROM jardineria.dbo.Categoria_producto),
       (SELECT COUNT(*) FROM stg.categoria_producto_raw WHERE _batch_id=@last_batch);

SELECT 'cliente' AS tabla,
       (SELECT COUNT(*) FROM jardineria.dbo.cliente),
       (SELECT COUNT(*) FROM stg.cliente_raw WHERE _batch_id=@last_batch);

SELECT 'pedido' AS tabla,
       (SELECT COUNT(*) FROM jardineria.dbo.pedido),
       (SELECT COUNT(*) FROM stg.pedido_raw WHERE _batch_id=@last_batch);

SELECT 'producto' AS tabla,
       (SELECT COUNT(*) FROM jardineria.dbo.producto),
       (SELECT COUNT(*) FROM stg.producto_raw WHERE _batch_id=@last_batch);

SELECT 'detalle_pedido' AS tabla,
       (SELECT COUNT(*) FROM jardineria.dbo.detalle_pedido),
       (SELECT COUNT(*) FROM stg.detalle_pedido_raw WHERE _batch_id=@last_batch);

SELECT 'pago' AS tabla,
       (SELECT COUNT(*) FROM jardineria.dbo.pago),
       (SELECT COUNT(*) FROM stg.pago_raw WHERE _batch_id=@last_batch);

SELECT *
FROM stg.pedido_raw
WHERE _batch_id=@last_batch
  AND estado_norm='ENTREGADO'
  AND fecha_entrega IS NULL;

SELECT *
FROM stg.pedido_raw
WHERE _batch_id=@last_batch
  AND fecha_esperada < fecha_pedido;

SELECT nombre_cliente, telefono, linea_direccion1, COUNT(*) AS veces
FROM stg.cliente_raw
WHERE _batch_id=@last_batch
GROUP BY nombre_cliente, telefono, linea_direccion1
HAVING COUNT(*)>1;

SELECT p.*
FROM stg.producto_raw p
WHERE p._batch_id=@last_batch
  AND p.Categoria_id_norm IS NULL;

SELECT *
FROM stg.detalle_pedido_raw
WHERE _batch_id=@last_batch
  AND (cantidad IS NULL OR cantidad<=0 OR precio_unidad<0);