# Importar la librería de Cassandra
from cassandra.cluster import Cluster
from prettytable import PrettyTable

# Conexión al cluster Cassandra
cluster = Cluster(['localhost'], port=9042)
session = cluster.connect()

session.set_keyspace('kstienda')

# Consultas (Queries)
''' 
    1) Mostrar todos los registros.
'''
result = session.execute("SELECT * FROM productos")

# Crear tabla para mostrar los resultados
tabla = PrettyTable()
tabla.field_names = result.column_names

for row in result:
    row_data = list(row)
    row_data[-3] = f"${row.precio:.2f}"
    tabla.add_row(row_data)

# Mostrar la tabla por pantalla
print("Consulta 1: Mostrar todos los registros")
print(tabla)

''' 
    2) Buscar y mostrar el producto con el precio más alto.
'''
max_price_row = session.execute("SELECT MAX(precio) AS max_price FROM productos").one()

max_price = max_price_row.max_price
    
result = session.execute("SELECT * FROM productos WHERE precio = %s ALLOW FILTERING", (max_price,))

print("\nConsulta 2: Producto con el precio más alto")
for row in result:
    row_data = list(row)
    row_data[-3] = f"${row.precio:.2f}"
    print(row_data)

''' 
    3) Buscar y mostrar el producto con el precio más bajo.
'''
min_price_row = session.execute("SELECT MIN(precio) AS min_price FROM productos").one()

min_price = min_price_row.min_price
    
result = session.execute("SELECT * FROM productos WHERE precio = %s ALLOW FILTERING", (min_price,))

print("\nConsulta 3: Producto con el precio más bajo")
for row in result:
    row_data = list(row)
    row_data[-3] = f"${row.precio:.2f}"
    print(row_data)

''' 
    4) Buscar y mostrar el producto más vendido.
'''
max_units_row = session.execute("SELECT MAX(unidades) AS max_units FROM productos").one()

max_units = max_units_row.max_units
    
result = session.execute("SELECT * FROM productos WHERE unidades = %s ALLOW FILTERING", (max_units,))

print("\nConsulta 4: Producto más vendido")
for row in result:
    row_data = list(row)
    row_data[-3] = f"${row.precio:.2f}"
    print(row_data)

''' 
    5) Buscar y mostrar el producto con la menor cantidad de unidades.
'''
min_units_row = session.execute("SELECT MIN(unidades) AS min_units FROM productos").one()

min_units = min_units_row.min_units
    
result = session.execute("SELECT * FROM productos WHERE unidades = %s ALLOW FILTERING", (min_units,))

print("\nConsulta 5: Producto con la menor cantidad de unidades")
for row in result:
    row_data = list(row)
    row_data[-3] = f"${row.precio:.2f}"
    print(row_data)

''' 
    6) Buscar y mostrar los productos cuyos nombres comiencen por la letra "C".
'''
result = session.execute("SELECT * FROM productos")

print("\nConsulta 6: Productos cuyos nombres comienzan por 'C'")
for row in result:
    if row.nombre.startswith('C'):
        row_data = list(row)
        row_data[-3] = f"${row.precio:.2f}"
        print(row_data)

# Cunsultas y análisis OLAP
#  Producto más vendido según género
print("\nProducto más vendido según el género")
genero_input = input("Introduce el género ('Male' o 'Female'): ").strip().capitalize()

if genero_input not in ('Male', 'Female'):
    print("Género no válido. Debes introducir 'Male' o 'Female'.")
else:
    result = session.execute("""
                    SELECT nombre, SUM(unidades) AS total_unidades
                    FROM productos WHERE genero = %s ALLOW FILTERING 
                    """, (genero_input,)).one()
    print("Género:", genero_input, "==> Producto:", result.nombre, "==> Unidades vendidas:", result.total_unidades)

# Calcular promedio de crédito entregado según género
print("\nCalculo de promedio de crédito entregado a personas según su género")
genero_input = input("Introduce el género ('Male' o 'Female'): ").strip().capitalize()

if genero_input not in ('Male', 'Female'):
    print("Género no válido. Debes introducir 'Male' o 'Female'.")
else:
    result = session.execute("""
                            SELECT AVG(credito) AS promedio_credito
                            FROM productos WHERE genero = %s ALLOW FILTERING
                            """, (genero_input,)).one()
    print("Género:", genero_input, "==> Promedio de crédito:", result.promedio_credito)

# Calcular promedio de crédito entregado según rango eterio
print("\nCalculo de promedio de crédito entregado a personas según su rango eterio")
rango_eterio_input = input("Introduce el rango eterio (por ejemplo, '26-35'): ").strip()

if '-' not in rango_eterio_input or not all(char.isdigit() or char == '-' for char in rango_eterio_input):
    print("Formato de rango eterio no válido. Debes introducir un formato como '26-35'.")
else:
    result = session.execute("""
                            SELECT AVG(credito) AS promedio_rango_eterio
                            FROM productos WHERE rango_eterio = %s ALLOW FILTERING
                            """, (rango_eterio_input,)).one()
    # Mostrar resultados
    if result.promedio_rango_eterio != 0:
        print("Rango eterio:", rango_eterio_input, "==> Promedio de crédito:", result.promedio_rango_eterio)
    else:
        print("No hay créditos para el rango etrio ", rango_eterio_input)

# Otras consultas y analisis OLAP
print("\nProducto más vendido en una ciudad según el género")
result = session.execute("""
                         SELECT nombre, SUM(unidades) AS total_unidades
                         FROM productos WHERE ciudad = 'San Francisco' AND genero = 'Female' ALLOW FILTERING
                         """).one()
print("Producto:", result.nombre, "==> Unidades vendidas:", result.total_unidades)

print("\nProducto más vendido en fecha y ciudad según el género")
result = session.execute("""
                         SELECT nombre, SUM(unidades) AS total_unidades
                         FROM productos 
                         WHERE fecha_compra = '2022-09-20' AND ciudad = 'Dallas' AND genero = 'Male' ALLOW FILTERING
                         """).one()
print("Producto:", result.nombre, "==> Unidades vendidas:", result.total_unidades)

# Cerrar la conexion
cluster.shutdown()