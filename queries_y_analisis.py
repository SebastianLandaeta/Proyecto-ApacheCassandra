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

'''
   7) Producto en promoción
'''
query = "SELECT nombre FROM productos WHERE compra_en_promocion = %s ALLOW FILTERING"
result = session.execute(query, (True,))
print("\nConsulta 7: Productos en promoción")
for row in result:
    print(row.nombre)

''' 
    8) Actualizar un registro a partir de su ID.
'''
id = input("ingrese el ID que desea modificar: ")
id = int(id)
query = "SELECT * FROM productos WHERE id = %s"
result = session.execute(query, (id,))

# Imprimir el resultado
print("\nEl registro que desea actualizar es: \n")
for row in result:
    nombre = row.nombre
    row_data = list(row)
    row_data[-3] = f"${row.precio:.2f}"
    print(row_data)

# Solicitar los nuevos valores por consola
nuevo_precio = float(input("Ingrese el nuevo precio: "))
nueva_ciudad = input("Ingrese la nueva ciudad: ")
nueva_fecha_compra = input("Ingrese la nueva fecha de compra (en formato 'YYYY-MM-DD'): ")
nuevas_unidades = int(input("Ingrese las nuevas unidades: "))
nueva_compra_en_promocion = input("La compra está en promoción (True/False): ").lower() == 'true'
nuevo_rango_eterio = input("Ingrese el nuevo rango eterio: ")
nuevo_genero = input("Ingrese el nuevo género: ")
nuevo_credito = int(input("Ingrese el nuevo crédito: "))

query = """
    UPDATE productos
    SET precio = %s, ciudad = %s, fecha_compra = %s, unidades = %s,
        compra_en_promocion = %s, rango_eterio = %s, genero = %s, credito = %s
    WHERE id = %s AND nombre = %s
"""

session.execute(query, (
    nuevo_precio, nueva_ciudad, nueva_fecha_compra, nuevas_unidades,
    nueva_compra_en_promocion, nuevo_rango_eterio, nuevo_genero, nuevo_credito,
    id, nombre
))

print("\nActualizado")

# Consultas y análisis OLAP
# Producto más vendido según género
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
print("\nCálculo de promedio de crédito entregado a personas según su rango eterio")
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
        print("No hay créditos para el rango eterio ", rango_eterio_input)

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

# Consulta opcional
''' 
    8) Borrar un registro de la base de datos.

print("\nBorrar un registro de la base de datos")

# Funcion para validar existencia del registro que desea borrar
def validar_existencia(id):
   query = "SELECT COUNT(*) FROM productos WHERE id = %s"
   #query = "DELETE FROM productos WHERE id = %s"
   result = session.execute(query, (int(id),))
   count = result.one()[0]
   return count > 0

# Solicitar el ID que desea borrar
id = input("Ingrese el ID del registro que desea eliminar: ")

if validar_existencia(int(id)): 
    # El ID existe, proceder con la eliminación
    query = "DELETE FROM productos WHERE id = %s"
    result = session.execute(query, (int(id),))
    print(f"\nRegistro con ID {id} eliminado exitosamente.")

    # Volver a mostrar la tabla
    result = session.execute("SELECT * FROM productos")

    # Crear tabla para mostrar los resultados
    tabla = PrettyTable()
    tabla.field_names = result.column_names

    for row in result:
        row_data = list(row)
        row_data[-3] = f"${row.precio:.2f}"
        tabla.add_row(row_data)

    # Mostrar la tabla por pantalla
    print("Mostrar actualizacion de todos los registros")
    print(tabla)
else:
    print(f"El registro con ID {id} no existe en la base de datos.")
'''

# Cerrar la conexion
cluster.shutdown()