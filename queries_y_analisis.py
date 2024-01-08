# Importar la librería de Cassandra
from cassandra.cluster import Cluster
from prettytable import PrettyTable

# Conexión al cluster Cassandra
cluster = Cluster(['localhost'], port=9042)
session = cluster.connect('kstienda')

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
    4) Buscar y mostrar el producto con la mayor cantidad de unidades.
'''
max_units_row = session.execute("SELECT MAX(unidades) AS max_units FROM productos").one()

max_units = max_units_row.max_units
    
result = session.execute("SELECT * FROM productos WHERE unidades = %s ALLOW FILTERING", (max_units,))

print("\nConsulta 4: Producto con la mayor cantidad de unidades")
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

# Cerrar la conexion
cluster.shutdown()