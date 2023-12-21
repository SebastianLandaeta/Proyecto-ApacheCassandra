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