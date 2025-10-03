/* 01_creacion_staging.sql */
IF DB_ID('jardineria_stg') IS NULL
BEGIN
  CREATE DATABASE jardineria_stg;
END
GO

USE jardineria_stg;
GO

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'stg')
  EXEC('CREATE SCHEMA stg');
GO

IF OBJECT_ID('stg._etl_batch_log') IS NULL
BEGIN
  CREATE TABLE stg._etl_batch_log(
    batch_id     UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID(),
    src_system   NVARCHAR(50)     NOT NULL,
    started_at   DATETIME2        NOT NULL DEFAULT SYSUTCDATETIME(),
    ended_at     DATETIME2        NULL,
    status       NVARCHAR(20)     NOT NULL DEFAULT 'RUNNING',
    rows_ingested INT             NULL,
    msg          NVARCHAR(4000)   NULL,
    CONSTRAINT PK__etl_batch_log PRIMARY KEY (batch_id)
  );
END
GO

IF OBJECT_ID('stg.oficina_raw') IS NULL
CREATE TABLE stg.oficina_raw(
  ID_oficina         INT            NOT NULL,
  Descripcion        NVARCHAR(10)   NULL,
  ciudad             NVARCHAR(30)   NULL,
  pais               NVARCHAR(50)   NULL,
  region             NVARCHAR(50)   NULL,
  codigo_postal      NVARCHAR(10)   NULL,
  telefono           NVARCHAR(20)   NULL,
  linea_direccion1   NVARCHAR(50)   NULL,
  linea_direccion2   NVARCHAR(50)   NULL,
  _src_system        NVARCHAR(50)   NOT NULL DEFAULT 'jardineria',
  _batch_id          UNIQUEIDENTIFIER NOT NULL,
  _ingestion_ts      DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),
  CONSTRAINT PK_stg_oficina_raw PRIMARY KEY (ID_oficina, _batch_id)
);

IF OBJECT_ID('stg.empleado_raw') IS NULL
CREATE TABLE stg.empleado_raw(
  ID_empleado   INT            NOT NULL,
  nombre        NVARCHAR(50)   NULL,
  apellido1     NVARCHAR(50)   NULL,
  apellido2     NVARCHAR(50)   NULL,
  extension     NVARCHAR(10)   NULL,
  email         NVARCHAR(100)  NULL,
  ID_oficina    INT            NULL,
  ID_jefe       INT            NULL,
  puesto        NVARCHAR(50)   NULL,
  _src_system   NVARCHAR(50)   NOT NULL DEFAULT 'jardineria',
  _batch_id     UNIQUEIDENTIFIER NOT NULL,
  _ingestion_ts DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),
  CONSTRAINT PK_stg_empleado_raw PRIMARY KEY (ID_empleado, _batch_id)
);

IF OBJECT_ID('stg.categoria_producto_raw') IS NULL
CREATE TABLE stg.categoria_producto_raw(
  Id_Categoria        INT            NOT NULL,
  Desc_Categoria      NVARCHAR(50)   NULL,
  descripcion_texto   NVARCHAR(MAX)  NULL,
  descripcion_html    NVARCHAR(MAX)  NULL,
  imagen              NVARCHAR(256)  NULL,
  _src_system         NVARCHAR(50)   NOT NULL DEFAULT 'jardineria',
  _batch_id           UNIQUEIDENTIFIER NOT NULL,
  _ingestion_ts       DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),
  CONSTRAINT PK_stg_categoria_producto_raw PRIMARY KEY (Id_Categoria, _batch_id)
);

IF OBJECT_ID('stg.cliente_raw') IS NULL
CREATE TABLE stg.cliente_raw(
  ID_cliente                INT            NOT NULL,
  nombre_cliente            NVARCHAR(50)   NULL,
  nombre_contacto           NVARCHAR(30)   NULL,
  apellido_contacto         NVARCHAR(30)   NULL,
  telefono                  NVARCHAR(15)   NULL,
  fax                       NVARCHAR(15)   NULL,
  linea_direccion1          NVARCHAR(50)   NULL,
  linea_direccion2          NVARCHAR(50)   NULL,
  ciudad                    NVARCHAR(50)   NULL,
  region                    NVARCHAR(50)   NULL,
  pais                      NVARCHAR(50)   NULL,
  codigo_postal             NVARCHAR(10)   NULL,
  ID_empleado_rep_ventas    INT            NULL,
  limite_credito            DECIMAL(15,2)  NULL,
  pais_iso3                 CHAR(3)        NULL,
  _src_system               NVARCHAR(50)   NOT NULL DEFAULT 'jardineria',
  _batch_id                 UNIQUEIDENTIFIER NOT NULL,
  _ingestion_ts             DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),
  CONSTRAINT PK_stg_cliente_raw PRIMARY KEY (ID_cliente, _batch_id)
);

IF OBJECT_ID('stg.pedido_raw') IS NULL
CREATE TABLE stg.pedido_raw(
  ID_pedido      INT            NOT NULL,
  fecha_pedido   DATE           NULL,
  fecha_esperada DATE           NULL,
  fecha_entrega  DATE           NULL,
  estado         NVARCHAR(15)   NULL,
  comentarios    NVARCHAR(MAX)  NULL,
  ID_cliente     INT            NULL,
  estado_norm    NVARCHAR(15)   NULL,
  _src_system    NVARCHAR(50)   NOT NULL DEFAULT 'jardineria',
  _batch_id      UNIQUEIDENTIFIER NOT NULL,
  _ingestion_ts  DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),
  CONSTRAINT PK_stg_pedido_raw PRIMARY KEY (ID_pedido, _batch_id)
);

IF OBJECT_ID('stg.producto_raw') IS NULL
CREATE TABLE stg.producto_raw(
  ID_producto        NVARCHAR(15)   NOT NULL,
  nombre             NVARCHAR(70)   NULL,
  Categoria_raw      NVARCHAR(50)   NULL,
  Categoria_id_norm  INT            NULL,
  dimensiones_raw    NVARCHAR(25)   NULL,
  proveedor          NVARCHAR(50)   NULL,
  descripcion        NVARCHAR(MAX)  NULL,
  cantidad_en_stock  INT            NULL,
  precio_venta       DECIMAL(15,2)  NULL,
  precio_proveedor   DECIMAL(15,2)  NULL,
  dim_valor_decimal  DECIMAL(10,3)  NULL,
  dim_min            DECIMAL(10,3)  NULL,
  dim_max            DECIMAL(10,3)  NULL,
  _src_system        NVARCHAR(50)   NOT NULL DEFAULT 'jardineria',
  _batch_id          UNIQUEIDENTIFIER NOT NULL,
  _ingestion_ts      DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),
  CONSTRAINT PK_stg_producto_raw PRIMARY KEY (ID_producto, _batch_id)
);

IF OBJECT_ID('stg.detalle_pedido_raw') IS NULL
CREATE TABLE stg.detalle_pedido_raw(
  ID_pedido      INT            NOT NULL,
  ID_producto    NVARCHAR(15)   NOT NULL,
  cantidad       INT            NULL,
  precio_unidad  DECIMAL(15,2)  NULL,
  numero_linea   SMALLINT       NULL,
  _src_system    NVARCHAR(50)   NOT NULL DEFAULT 'jardineria',
  _batch_id      UNIQUEIDENTIFIER NOT NULL,
  _ingestion_ts  DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),
  CONSTRAINT PK_stg_detalle_pedido_raw PRIMARY KEY (ID_pedido, ID_producto, _batch_id)
);

IF OBJECT_ID('stg.pago_raw') IS NULL
CREATE TABLE stg.pago_raw(
  ID_cliente     INT            NOT NULL,
  forma_pago     NVARCHAR(40)   NULL,
  id_transaccion NVARCHAR(50)   NOT NULL,
  fecha_pago     DATE           NULL,
  total          DECIMAL(15,2)  NULL,
  _src_system    NVARCHAR(50)   NOT NULL DEFAULT 'jardineria',
  _batch_id      UNIQUEIDENTIFIER NOT NULL,
  _ingestion_ts  DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),
  CONSTRAINT PK_stg_pago_raw PRIMARY KEY (ID_cliente, id_transaccion, _batch_id)
);