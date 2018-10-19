# datahem.registry
A registry for avro schemas, running on google datastore

# Version:
## 0.7.5 (2018-10-19): Cloud Build and schema path
Added cloudbuild.yaml. Added path as field to datastore.

## 0.7.4 (2018-10-18): Error logging patch
Added logging of errors in registryloader

## 0.7.3 (2018-10-18): Logging
Added logging

## 0.7.2 (2018-10-18): Avro schema name and namespace as fields in registry
Adding name and namespace fields to Schema entity in datastore