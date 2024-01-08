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

# Agregar filas a la tabla
for row in result:
    tabla.add_row(row)

# Mostrar la tabla por pantalla
print(tabla)


# Cerrar la conexion
cluster.shutdown()