---
sidebar_position: 7
---

# Database

Coroot requires a database to store its configuration, such as projects and Prometheus connection details.

## SQLite (default)

By default, Coroot uses an embedded sqlite database. For production installations, we recommend users to use a 
robust database, such as Postgres. This allows you to run several Coroot replicas for high availability and backup the database.

## Postgres

Create role and database:

```sql
CREATE ROLE coroot WITH LOGIN PASSWORD 'password';
CREATE DATABASE coroot WITH OWNER = coroot;
```

You can configure Coroot to use Postgres by setting the `--pg-connection-string` command line argument or the `PG_CONNECTION_STRING` environment variable:

```bash
docker run -d --name coroot \
  -p 8080:8080 \
  -e PG_CONNECTION_STRING="postgres://coroot:password@127.0.0.1:5432/coroot?sslmode=disable" \
  ghcr.io/coroot/coroot
``` 

Here is an example of how to format the `PG_CONNECTION_STRING` variable using a Kubernetes secret:

```yaml
...
env:
- name: PGPASSWORD
  valueFrom: { secretKeyRef: { name: coroot.pg.credentials, key: password } }
- name: PG_CONNECTION_STRING
  value: "host=coroot-db user=coroot password=$(PGPASSWORD) dbname=coroot sslmode=require connect_timeout=1"
  ...
```
  
To learn more about the connection string format follow the [Postgres documentation](https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING).

### AWS RDS/Aurora IAM Authentication

If you are running Coroot in AWS and want to connect to an RDS or Aurora Postgres database using IAM authentication, you can enable it by setting the `--pg-iam-auth` command-line argument or the `PG_IAM_AUTH=true` environment variable.

When enabled, Coroot will automatically generate an IAM authentication token to use as the password every time a new connection is established, and will safely recycle connections before the 15-minute token expires.

To use IAM Authentication, you must configure both your database and your AWS IAM policies:

**1. Database Role Configuration**
The Postgres user must be granted the `rds_iam` role. Connect to your database as an administrator and run:

```sql
CREATE USER coroot WITH LOGIN;
GRANT rds_iam TO coroot;
CREATE DATABASE coroot WITH OWNER = coroot;
```

**2. IAM Permissions**
The environment running Coroot (e.g., your EKS pod via IRSA, or EC2 instance profile) must have an IAM policy granting the `rds-db:connect` permission:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["rds-db:connect"],
      "Resource": [
        "arn:aws:rds-db:<region>:<account-id>:dbuser:<db-cluster-resource-id>/coroot"
      ]
    }
  ]
}
```

**3. Coroot Configuration**
Your connection string should point to the RDS endpoint and specify `sslmode=require`. You do not need to provide a password in the connection string.

```bash
docker run -d --name coroot \
  -p 8080:8080 \
  -e PG_CONNECTION_STRING="host=db.cluster-xxx.us-east-1.rds.amazonaws.com user=coroot dbname=coroot sslmode=require" \
  -e PG_IAM_AUTH="true" \
  ghcr.io/coroot/coroot
```

### Connection Poolers

Coroot is incompatible with PostgreSQL connection poolers like pgbouncer when they're configured to run in transactional mode. This is because Coroot relies on connection-level prepared statements, which don't persist across transactions in pooled connections. If you need to use a connection pooler, configure it to operate in session mode to ensure prepared statements remain available throughout the connection lifecycle.

