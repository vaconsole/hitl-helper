rm -f tmp/test.db
sqlite3 tmp/test.db < test/input/init.sql
node bin/hitl-helper create tmp/test.db 'select * from a' id test
node bin/hitl-helper  update tmp/test.db 1
node bin/hitl-helper assign tmp/test.db 1
node bin/hitl-helper get tmp/test.db 1 | jq
node bin/hitl-helper submit tmp/test.json  tmp/test.db 1 
