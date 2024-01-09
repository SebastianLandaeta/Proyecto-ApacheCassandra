# Importar la librería de Cassandra
from cassandra.cluster import Cluster

# Crear una nueva conexion al cluster
cluster = Cluster(['localhost'], port=9042)
session = cluster.connect()

# Eliminar Keyspaces, usar solo para eliminar el anterior Keyspaces
#session.execute("DROP KEYSPACE IF EXISTS kstienda")

# Creación y uso del espacio de nombre
session.execute("""
                CREATE KEYSPACE IF NOT EXISTS kstienda 
                WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}
                """)

session.set_keyspace('kstienda')

# Eliminar tabla
session.execute("DROP TABLE IF EXISTS productos")

# Creación de una tabla 
session.execute("""
                CREATE TABLE IF NOT EXISTS productos(
                id INT, 
                nombre TEXT, 
                precio FLOAT,
                fecha_compra DATE,
                ciudad TEXT, 
                unidades INT,
                compra_en_promocion BOOLEAN, 
                rango_eterio TEXT,
                genero TEXT,
                credito INT,
                PRIMARY KEY (id, nombre)
                )
                """)

# Lista de productos
productos = [
    (101, 'Laptop', 1200.99, '2022-01-10', 'New York', 50, False, '26-35', 'Male', 800),
    (102, 'Smartphone', 799.99, '2022-02-15', 'Los Angeles', 120, True, '18-25', 'Female', 600),
    (103, 'Smartwatch', 349.99, '2022-03-20', 'Chicago', 80, False, '36-45', 'Male', 400),
    (104, 'Headphones', 149.99, '2022-04-25', 'Houston', 60, True, '26-35', 'Female', 200),
    (105, 'Camera', 899.99, '2022-05-30', 'Phoenix', 90, False, '46-55', 'Male', 1000),
    (106, 'Tablet', 499.99, '2022-06-05', 'Philadelphia', 75, True, '18-25', 'Female', 700),
    (107, 'Gaming Console', 299.99, '2022-07-10', 'San Antonio', 110, False, '26-35', 'Male', 1200),
    (108, 'Fitness Tracker', 129.99, '2022-08-15', 'San Diego', 40, True, '36-45', 'Female', 300),
    (109, 'Wireless Speaker', 79.99, '2022-09-20', 'Dallas', 95, False, '46-55', 'Male', 500),
    (110, 'E-reader', 199.99, '2022-10-25', 'San Jose', 70, True, '18-25', 'Female', 400),
    (111, 'Drones', 599.99, '2022-11-30', 'Austin', 55, False, '26-35', 'Male', 900),
    (112, 'VR Headset', 349.99, '2022-12-05', 'Jacksonville', 65, True, '36-45', 'Female', 800),
    (113, 'Coffee Maker', 69.99, '2023-01-10', 'Indianapolis', 30, False, '46-55', 'Male', 150),
    (114, 'Blender', 39.99, '2023-02-15', 'San Francisco', 85, True, '18-25', 'Female', 250),
    (115, 'Vacuum Cleaner', 199.99, '2023-03-20', 'Columbus', 100, False, '26-35', 'Male', 350),
    (116, 'Smart Thermostat', 129.99, '2023-04-25', 'Fort Worth', 45, True, '36-45', 'Female', 200),
    (117, 'Portable Charger', 24.99, '2023-05-30', 'Charlotte', 110, False, '46-55', 'Male', 100),
    (118, 'Bluetooth Earbuds', 49.99, '2023-06-05', 'Detroit', 75, True, '18-25', 'Female', 80),
    (119, 'Car Dash Cam', 149.99, '2023-07-10', 'El Paso', 90, False, '26-35', 'Male', 300),
    (120, 'Electric Toothbrush', 79.99, '2023-08-15', 'Seattle', 55, True, '36-45', 'Female', 120),
    (121, 'Air Purifier', 299.99, '2023-09-20', 'Denver', 65, False, '46-55', 'Male', 400),
    (122, 'Portable Projector', 199.99, '2023-10-25', 'Washington', 80, True, '18-25', 'Female', 600),
    (123, 'External Hard Drive', 129.99, '2023-11-30', 'Boston', 40, False, '26-35', 'Male', 200),
    (124, 'Home Security Camera', 89.99, '2023-12-05', 'Nashville', 95, True, '36-45', 'Female', 300),
    (125, 'Wireless Mouse', 19.99, '2024-01-10', 'Baltimore', 120, False, '46-55', 'Male', 50),
    (126, 'Sleep Tracker', 59.99, '2024-02-15', 'Oklahoma City', 70, True, '18-25', 'Female', 350),
    (127, 'Wireless Keyboard', 29.99, '2024-03-20', 'Louisville', 55, False, '26-35', 'Male', 100),
    (128, 'Home Espresso Machine', 249.99, '2024-04-25', 'Portland', 90, True, '36-45', 'Female', 700),
    (129, 'Noise-Canceling Headphones', 149.99, '2024-05-30', 'Las Vegas', 65, False, '46-55', 'Male', 800),
    (130, 'Smart Refrigerator', 999.99, '2024-06-05', 'Milwaukee', 40, True, '18-25', 'Female', 1200),
    (131, 'Solar Charger', 39.99, '2024-07-10', 'Albuquerque', 85, False, '26-35', 'Male', 200),
    (132, 'Robot Vacuum', 349.99, '2024-08-15', 'Tucson', 110, True, '36-45', 'Female', 300),
    (133, 'Air Fryer', 89.99, '2024-09-20', 'Fresno', 95, False, '46-55', 'Male', 400),
    (134, 'Smart Doorbell', 149.99, '2024-10-25', 'Sacramento', 75, True, '18-25', 'Female', 500),
    (135, 'Electric Scooter', 299.99, '2024-11-30', 'Mesa', 60, False, '26-35', 'Male', 600),
    (136, 'Bluetooth Speaker', 129.99, '2024-12-05', 'Atlanta', 30, True, '36-45', 'Female', 700),
    (137, 'Waterproof Camera', 179.99, '2025-01-10', 'Kansas City', 50, False, '46-55', 'Male', 800),
    (138, 'Fitness Smartwatch', 249.99, '2025-02-15', 'Cleveland', 120, True, '18-25', 'Female', 900),
    (139, 'Desktop Computer', 799.99, '2025-03-20', 'Virginia Beach', 85, False, '26-35', 'Male', 1000),
    (140, 'Electric Shaver', 69.99, '2025-04-25', 'Omaha', 65, True, '36-45', 'Female', 1100),
]

# Inserción de valores a la tabla
for producto in productos:
    session.execute("""
                    INSERT INTO productos (id, nombre, precio, fecha_compra, ciudad, unidades, compra_en_promocion, rango_eterio, genero, credito) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, producto)

# Cerrar la conexion
cluster.shutdown()