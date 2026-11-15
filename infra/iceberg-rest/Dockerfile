# The upstream REST fixture bundles only the SQLite JDBC driver. SQLite allows a
# single writer, so concurrent table commits (two streaming queries) fail with
# SQLITE_BUSY. Add the Postgres driver and back the catalog with Postgres.
FROM apache/iceberg-rest-fixture:1.10.1
ADD --chmod=644 https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.7/postgresql-42.7.7.jar /usr/lib/iceberg-rest/postgresql.jar
ENTRYPOINT ["java", "-cp", "/usr/lib/iceberg-rest/iceberg-rest-adapter.jar:/usr/lib/iceberg-rest/postgresql.jar", "org.apache.iceberg.rest.RESTCatalogServer"]
