SOLR='172.31.17.188'
COLLECTION='boss'

curl -i --negotiate -u : "http://$SOLR:8983/solr/$COLLECTION/select?q=*%3A*&wt=json&indent=true"
