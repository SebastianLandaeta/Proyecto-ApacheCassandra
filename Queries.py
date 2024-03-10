# Importar la librería de Cassandra y una librería para hacer tablas en la terminal
from cassandra.cluster import Cluster
from prettytable import PrettyTable

# Conexión al cluster Cassandra
cluster = Cluster(['localhost'], port=9042)
session = cluster.connect()

session.set_keyspace('KStienda')

# Consultas (Queries)
''' 
    1) Mostrar todos los registros.
'''
result = session.execute("SELECT * FROM Producto")

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
max_price_row = session.execute("SELECT MAX(precio) AS max_price FROM Producto").one()

max_price = max_price_row.max_price
    
result = session.execute("SELECT * FROM Producto WHERE precio = %s ALLOW FILTERING", (max_price,))

print("\nConsulta 2: Producto con el precio más alto")
for row in result:
    row_data = list(row)
    row_data[-3] = f"${row.precio:.2f}"
    print(row_data)

''' 
    3) Buscar y mostrar el producto con el precio más bajo.
'''
min_price_row = session.execute("SELECT MIN(precio) AS min_price FROM Producto").one()

min_price = min_price_row.min_price
    
result = session.execute("SELECT * FROM Producto WHERE precio = %s ALLOW FILTERING", (min_price,))

print("\nConsulta 3: Producto con el precio más bajo")
for row in result:
    row_data = list(row)
    row_data[-3] = f"${row.precio:.2f}"
    print(row_data)

''' 
    4) Buscar y mostrar el producto más vendido.
'''
max_units_row = session.execute("SELECT MAX(unidades) AS max_units FROM Producto").one()

max_units = max_units_row.max_units
    
result = session.execute("SELECT * FROM Producto WHERE unidades = %s ALLOW FILTERING", (max_units,))

print("\nConsulta 4: Producto más vendido")
for row in result:
    row_data = list(row)
    row_data[-3] = f"${row.precio:.2f}"
    print(row_data)

''' 
    5) Buscar y mostrar el producto con la menor cantidad de unidades.
'''
min_units_row = session.execute("SELECT MIN(unidades) AS min_units FROM Producto").one()

min_units = min_units_row.min_units
    
result = session.execute("SELECT * FROM Producto WHERE unidades = %s ALLOW FILTERING", (min_units,))

print("\nConsulta 5: Producto con la menor cantidad de unidades")
for row in result:
    row_data = list(row)
    row_data[-3] = f"${row.precio:.2f}"
    print(row_data)

''' 
    6) Buscar y mostrar los productos cuyos nombres comiencen por la letra "C".
'''
result = session.execute("SELECT * FROM Producto")

print("\nConsulta 6: Productos cuyos nombres comienzan por 'C'")
for row in result:
    if row.nombre.startswith('C'):
        row_data = list(row)
        row_data[-3] = f"${row.precio:.2f}"
        print(row_data)

'''
   7) Productos en promoción
'''
query = "SELECT nombre FROM Producto WHERE compra_en_promocion = %s ALLOW FILTERING"
result = session.execute(query, (True,))
print("\nConsulta 7: Productos en promoción")
for row in result:
    print(row.nombre)

''' 
    8) Actualizar un registro a partir de su ID.
'''
id = input("Ingrese el ID que desea modificar: ")
id = int(id)
query = "SELECT * FROM Producto WHERE id = %s"
result = session.execute(query, (id,))

# Imprimir el resultado
print("\nEl registro que desea actualizar es: \n")
for row in result:
    nombre = row.nombre
    row_data = list(row)
    row_data[-3] = f"${row.precio:.2f}"
    print(row_data)

# Solicitar los nuevos valores por consola
new_precio = float(input("Ingrese el nuevo precio: "))
new_ciudad = input("Ingrese la nueva ciudad: ")
new_fecha_compra = input("Ingrese la nueva fecha de compra (en formato 'YYYY-MM-DD'): ")
new_unidades = int(input("Ingrese las nuevas unidades: "))
new_compra_en_promocion = input("La compra está en promoción (True/False): ").lower() == 'true'
new_rango_eterio = input("Ingrese el nuevo rango eterio: ")
new_genero = input("Ingrese el nuevo género: ")
new_credito = int(input("Ingrese el nuevo crédito: "))

query = """
    UPDATE Producto
    SET precio = %s, ciudad = %s, fecha_compra = %s, unidades = %s,
        compra_en_promocion = %s, rango_eterio = %s, genero = %s, credito = %s
    WHERE id = %s AND nombre = %s
"""

session.execute(query, (
    new_precio, new_ciudad, new_fecha_compra, new_unidades,
    new_compra_en_promocion, new_rango_eterio, new_genero, new_credito,
    id, nombre
))

print("\nActualizado")

''' 
    9) Producto más vendido según el género
'''
print("\nProducto más vendido según el género")
gender_input = input("Introduce el género ('Male' o 'Female'): ").strip().capitalize()

if gender_input not in ('Male', 'Female'):
    print("Género no válido. Debes introducir 'Male' o 'Female'.")
else:
    result = session.execute("""
                    SELECT nombre, SUM(unidades) AS total_unidades
                    FROM Producto WHERE genero = %s ALLOW FILTERING 
                    """, (gender_input,)).one()
    print("Género:", gender_input, "==> Producto:", result.nombre, "==> Unidades vendidas:", result.total_unidades)

''' 
    10) Calcular promedio de crédito entregado según género
'''
print("\nCalculo de promedio de crédito entregado a personas según su género")
gender_input = input("Introduce el género ('Male' o 'Female'): ").strip().capitalize()

if gender_input not in ('Male', 'Female'):
    print("Género no válido. Debes introducir 'Male' o 'Female'.")
else:
    result = session.execute("""
                            SELECT AVG(credito) AS promedio_credito
                            FROM Producto WHERE genero = %s ALLOW FILTERING
                            """, (gender_input,)).one()
    print("Género:", gender_input, "==> Promedio de crédito:", result.promedio_credito)

''' 
    11) Calcular promedio de crédito entregado según rango etario
'''
print("\nCálculo de promedio de crédito entregado a personas según su rango etario")
rango_etario_input = input("Introduce el rango etario (por ejemplo, '26-35'): ").strip()

if '-' not in rango_etario_input or not all(char.isdigit() or char == '-' for char in rango_etario_input):
    print("Formato de rango etario no válido. Debes introducir un formato como '26-35'.")
else:
    result = session.execute("""
                            SELECT AVG(credito) AS promedio_rango_eterio
                            FROM Producto WHERE rango_eterio = %s ALLOW FILTERING
                            """, (rango_etario_input,)).one()
    # Mostrar resultados
    if result.promedio_rango_etario != 0:
        print("Rango etario:", rango_etario_input, "==> Promedio de crédito:", result.promedio_rango_etario)
    else:
        print("No hay créditos para el rango eterio ", rango_etario_input)

''' 
    12) Producto más vendido en una ciudad según el género
'''
print("\nProducto más vendido en una ciudad según el género")
result = session.execute("""
                         SELECT nombre, SUM(unidades) AS total_unidades
                         FROM Producto WHERE ciudad = 'San Francisco' AND genero = 'Female' ALLOW FILTERING
                         """).one()
print("Producto:", result.nombre, "==> Unidades vendidas:", result.total_unidades)

''' 
    13) Producto más vendido en fecha y ciudad según el género
'''
print("\nProducto más vendido en fecha y ciudad según el género")
result = session.execute("""
                         SELECT nombre, SUM(unidades) AS total_unidades
                         FROM Producto 
                         WHERE fecha_compra = '2022-09-20' AND ciudad = 'Dallas' AND genero = 'Male' ALLOW FILTERING
                         """).one()
print("Producto:", result.nombre, "==> Unidades vendidas:", result.total_unidades)

# Consulta opcional
''' 
    12) Borrar un registro de la base de datos.

print("\nBorrar un registro de la base de datos")

# Funcion para validar existencia del registro que desea borrar
def validar_existencia(id):
   query = "SELECT COUNT(*) FROM Producto WHERE id = %s"
   query = "DELETE FROM Producto WHERE id = %s"
   result = session.execute(query, (int(id),))
   count = result.one()[0]
   return count > 0

# Solicitar el ID que desea borrar
id = input("Ingrese el ID del registro que desea eliminar: ")

if validar_existencia(int(id)): 
    # El ID existe, proceder con la eliminación
    query = "DELETE FROM Producto WHERE id = %s"
    result = session.execute(query, (int(id),))
    print(f"\nRegistro con ID {id} eliminado exitosamente.")

    # Volver a mostrar la tabla
    result = session.execute("SELECT * FROM Producto")

    # Crear tabla para mostrar los resultados
    tabla = PrettyTable()
    tabla.field_names = result.column_names

    for row in result:
        row_data = list(row)
        row_data[-3] = f"${row.precio:.2f}"
        tabla.add_row(row_data)

    # Mostrar la tabla por pantalla
    print("Mostrar actualización de todos los registros")
    print(tabla)
else:
    print(f"El registro con ID {id} no existe en la base de datos.")
'''

# Cerrar la conexión
cluster.shutdown()