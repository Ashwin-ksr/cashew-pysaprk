# libs/

JAR files needed by the local stack. They are not committed to git (`libs/*.jar` is ignored), so download them after cloning.

## postgresql-42.7.13.jar

**Why:** JDBC driver the Hive Metastore container uses to connect to Postgres. The Hive image doesn't include one.

**Download** (from the project root):

```bash
curl -fL -o libs/postgresql-42.7.13.jar \
  https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.13/postgresql-42.7.13.jar
```

**Using a different version:** download the new JAR, delete the old one, then update the filename in the `hive-metastore` volume mount in `docker-compose.yaml`:

```yaml
- ./libs/postgresql-42.7.13.jar:/opt/hive/lib/postgres.jar
```

Then recreate the container:

```bash
docker compose up -d --force-recreate hive-metastore