CONFIG ?= test

# Starts dynamodb local
dynamodb-local:
	scripts/dynamodb_local.sh
.PHONY:dynamodb-local

s3-local:
	scripts/s3_local.sh
.PHONY:s3-local

# Starts UI for inspecting dynamodb
dynamodb-admin:
	scripts/dynamodb_admin.sh
.PHONY:dynamodb-local

ingest-data: 
	python3 src/main.py insert-vulnerability-data
.PHONY:ingest-data

collect-data:
	python3 src/main.py ${ACTION} ${CONFIG}
.PHONY:collect-data

graphql:
	scripts/server.sh
.PHONY:graphql