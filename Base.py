# Importar la librería de Cassandra
from cassandra.cluster import Cluster

# Conexión al cluster Cassandra
cluster = Cluster(['localhost'], port=9042)
session = cluster.connect()

# Creación y uso del espacio de nombre
session.execute("CREATE KEYSPACE IF NOT EXISTS KSTienda WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}")

session.execute("USE KSTienda")

# Creación de una tabla 
session.execute("CREATE TABLE IF NOT EXISTS Producto (id UUID PRIMARY KEY, nombre TEXT, descripcion TEXT, precio FLOAT, unidades INT)")

# Inserción de valores a la tabla
session.execute("INSERT INTO Producto (id, nombre, descripcion, precio, unidades) VALUES (uuid(), 'Camiseta Roja', 'Camiseta de algodón roja', 15.99, 50)")
session.execute("INSERT INTO Producto (id, nombre, descripcion, precio, unidades) VALUES (uuid(), 'Silla de Oficina', 'Silla ergonómica para oficina', 129.99, 20)")
session.execute("INSERT INTO Producto (id, nombre, descripcion, precio, unidades) VALUES (uuid(), 'Botines de Fútbol', 'Botines deportivos para fútbol', 49.99, 30)")
session.execute("INSERT INTO Producto (id, nombre, descripcion, precio, unidades) VALUES (uuid(), 'Gafas de Sol Aviador', 'Gafas de sol estilo aviador', 39.99, 40)")
session.execute("INSERT INTO Producto (id, nombre, descripcion, precio, unidades) VALUES (uuid(), 'Teclado Mecánico RGB', 'Teclado mecánico con retroiluminación RGB', 89.99, 15)")
session.execute("INSERT INTO Producto (id, nombre, descripcion, precio, unidades) VALUES (uuid(), 'Libreta de Notas', 'Libreta de notas con tapa dura', 5.99, 100)")
session.execute("INSERT INTO Producto (id, nombre, descripcion, precio, unidades) VALUES (uuid(), 'Reloj Inteligente', 'Reloj con funciones inteligentes', 129.99, 25)")
session.execute("INSERT INTO Producto (id, nombre, descripcion, precio, unidades) VALUES (uuid(), 'Mochila para Portátil', 'Mochila acolchada para portátil', 49.99, 30)")
session.execute("INSERT INTO Producto (id, nombre, descripcion, precio, unidades) VALUES (uuid(), 'Cámara de Seguridad', 'Cámara de seguridad para interiores', 79.99, 10)")
session.execute("INSERT INTO Producto (id, nombre, descripcion, precio, unidades) VALUES (uuid(), 'Auriculares Inalámbricos', 'Auriculares con conexión Bluetooth', 59.99, 40)")
session.execute("INSERT INTO Producto (id, nombre, descripcion, precio, unidades) VALUES (uuid(), 'Lámpara de Escritorio', 'Lámpara LED ajustable para escritorio', 29.99, 20)")
session.execute("INSERT INTO Producto (id, nombre, descripcion, precio, unidades) VALUES (uuid(), 'Pelota de Baloncesto', 'Pelota oficial de baloncesto', 19.99, 30)")
session.execute("INSERT INTO Producto (id, nombre, descripcion, precio, unidades) VALUES (uuid(), 'Cargador Inalámbrico', 'Base de carga inalámbrica para dispositivos', 24.99, 50)")
session.execute("INSERT INTO Producto (id, nombre, descripcion, precio, unidades) VALUES (uuid(), 'Mesa de Centro', 'Mesa de centro para sala de estar', 99.99, 8)")
session.execute("INSERT INTO Producto (id, nombre, descripcion, precio, unidades) VALUES (uuid(), 'Batería Externa', 'Batería portátil para dispositivos móviles', 34.99, 25)")
session.execute("INSERT INTO Producto (id, nombre, descripcion, precio, unidades) VALUES (uuid(), 'Juego de Destornilladores', 'Set de destornilladores de precisión', 12.99, 60)")
session.execute("INSERT INTO Producto (id, nombre, descripcion, precio, unidades) VALUES (uuid(), 'Kit de Herramientas para Jardín', 'Herramientas básicas para jardinería', 29.99, 18)")
session.execute("INSERT INTO Producto (id, nombre, descripcion, precio, unidades) VALUES (uuid(), 'Termo de Acero Inoxidable', 'Termo de doble capa para líquidos', 19.99, 35)")
session.execute("INSERT INTO Producto (id, nombre, descripcion, precio, unidades) VALUES (uuid(), 'Cuaderno de Dibujo', 'Cuaderno especial para dibujo artístico', 9.99, 50)")
session.execute("INSERT INTO Producto (id, nombre, descripcion, precio, unidades) VALUES (uuid(), 'Lápiz USB', 'Almacenamiento portátil en forma de lápiz', 14.99, 22)")
session.execute("INSERT INTO Producto (id, nombre, descripcion, precio, unidades) VALUES (uuid(), 'Funda para Tablet', 'Funda protectora para tablet de 10 pulgadas', 19.99, 45)")
session.execute("INSERT INTO Producto (id, nombre, descripcion, precio, unidades) VALUES (uuid(), 'Juego de Tazas de Café', 'Set de tazas de porcelana para café', 29.99, 28)")
session.execute("INSERT INTO Producto (id, nombre, descripcion, precio, unidades) VALUES (uuid(), 'Calculadora Científica', 'Calculadora avanzada para cálculos científicos', 19.99, 15)")
session.execute("INSERT INTO Producto (id, nombre, descripcion, precio, unidades) VALUES (uuid(), 'Colchón Ortopédico', 'Colchón ortopédico de espuma viscoelástica', 299.99, 5)")
session.execute("INSERT INTO Producto (id, nombre, descripcion, precio, unidades) VALUES (uuid(), 'Laptop Ultradelgada', 'Laptop ultradelgada con pantalla HD', 799.99, 12)")
session.execute("INSERT INTO Producto (id, nombre, descripcion, precio, unidades) VALUES (uuid(), 'Cepillo de Dientes Eléctrico', 'Cepillo de dientes con tecnología sónica', 49.99, 20)")
session.execute("INSERT INTO Producto (id, nombre, descripcion, precio, unidades) VALUES (uuid(), 'Bolsa de Viaje', 'Bolsa de viaje resistente al agua', 39.99, 30)")
session.execute("INSERT INTO Producto (id, nombre, descripcion, precio, unidades) VALUES (uuid(), 'Set de Cubiertos de Acero', 'Set completo de cubiertos de acero inoxidable', 49.99, 40)")
session.execute("INSERT INTO Producto (id, nombre, descripcion, precio, unidades) VALUES (uuid(), 'Pendrive de 128GB', 'Unidad flash USB de alta capacidad', 39.99, 18)")
session.execute("INSERT INTO Producto (id, nombre, descripcion, precio, unidades) VALUES (uuid(), 'Hervidor de Agua Eléctrico', 'Hervidor con capacidad para 1.7 litros', 29.99, 25)")
session.execute("INSERT INTO Producto (id, nombre, descripcion, precio, unidades) VALUES (uuid(), 'Bicicleta de Montaña', 'Bicicleta todoterreno para montaña', 499.99, 8)")

# Consultas (Queries)
''' 
    1) Mostrar todos los registros.
'''
result = session.execute("SELECT * FROM Producto")

for row in result:
    print(row)

''' 
    2) Borrar el ultimo registro ingresado en la base de datos.
'''
session.execute("DELETE FROM Producto WHERE id = <Inserte el ID del registro que desea eliminar>")

''' 
    3) Buscar y mostrar el producto con el precio más alto.
'''
max_price_row = session.execute("SELECT MAX(precio) AS max_price FROM Producto").one()

max_price = max_price_row.max_price
    
result = session.execute("SELECT * FROM Producto WHERE precio = %s ALLOW FILTERING", (max_price,))

for row in result:
    print(row)

''' 
    4) Buscar y mostrar el producto con el precio más bajo.
'''
min_price_row = session.execute("SELECT MIN(precio) AS min_price FROM Producto").one()

min_price = min_price_row.min_price
    
result = session.execute("SELECT * FROM Producto WHERE precio = %s ALLOW FILTERING", (min_price,))

for row in result:
    print(row)

''' 
    5) Buscar y mostrar el producto con la mayor cantidad de unidades.
'''
max_units_row = session.execute("SELECT MAX(unidades) AS max_units FROM Producto").one()

max_units = max_units_row.max_units
    
result = session.execute("SELECT * FROM Producto WHERE unidades = %s ALLOW FILTERING", (max_units,))

for row in result:
    print(row)

''' 
    6) Buscar y mostrar el producto con la menor cantidad de unidades.
'''
min_units_row = session.execute("SELECT MIN(unidades) AS min_units FROM Producto").one()

min_units = min_units_row.min_units
    
result = session.execute("SELECT * FROM Producto WHERE unidades = %s ALLOW FILTERING", (min_units,))

for row in result:
    print(row)

''' 
    7) Buscar y mostrar los productos cuyos nombres comiencen por la letra "C".
'''
result = session.execute("SELECT * FROM Producto")

for row in result:
    if row.nombre.startswith('C'):
        print(row)

''' 
    8) Eliminar todos los registros de la tabla Producto.
'''

session.execute("TRUNCATE Producto")