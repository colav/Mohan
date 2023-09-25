<center><img src="https://raw.githubusercontent.com/colav/colav.github.io/master/img/Logo.png"/></center>

# Mohan
Colav Similarity using Elastic Search / Pijao - Mohán spirit of water.

# Description
This package allows to perform similarity using Colav similarity algorithms and Elastic Search

# Installation

## Dependencies
Docker and docker-compose is required.
* https://docs.docker.com/engine/install/ubuntu/ (or https://docs.docker.com/engine/install/debian/, etc)
* Install `docker-compose`:  
```bash
apt install docker-compose
```
or
```bash
pip install docker-compose
```

* https://docs.docker.com/engine/install/linux-postinstall/

* Deploy Elastic Search from Chia https://github.com/colav/Chia/tree/main/elasticsaerch


## Package
`pip install mohan`

# Usage
This package was designed to be used as library,
you need import the class Similarity, to create an index,
insert documents(works) and perform searches.

The next example is with openalex but it can be used with any dataset.

```py

from mohan.Similarity import Similarity

#creating the instance
s = Similarity("openalex_index",es_uri= "http://localhost:9200",
                 es_auth = ('elastic', 'colav'))

#taking openalex as example.
openalex = list(MongoClient()["openalexco"]["works"].find())

#example inserting documents to the Elastic Search index.
bulk_size = 100

es_entries = []
counter = 0
for i in openalex:
    work = {}
    work["title"] = i["title"]
    work["source"] = i["host_venue"]["display_name"]
    work["year"] = i["publication_year"]
    work["volume"] = i["biblio"]["volume"]
    work["issue"] = i["biblio"]["issue"]
    work["first_page"] = i["biblio"]["first_page"]
    work["last_page"] = i["biblio"]["last_page"]

    entry = {"_index": es_index,
                "_id": str(i["_id"]),
                "_source": work}
    es_entries.append(entry)
    if len(es_entries) == bulk_size:
        s.bulk(es_entries)
        es_entries = []
```
### example inserting one document from openalex
```py
work = {"title": i["title"],
        "source": i["host_venue"]["display_name"],
        "year": i["publication_year"],
        "volume": i["biblio"]["volume"],
        "issue": i["biblio"]["issue"],
        "page_start": i["biblio"]["first_page"],
        "page_end": i["biblio"]["last_page"]}
res = s.insert_work(_id=str(i["_id"]), work=work)
```
### example performing a search

```py
res = s.search_work(self, title=i["title"], source = i["host_venue"]["display_name"], year = i["publication_year"],
                    volume = i["biblio"]["volume"], issue = i["biblio"]["issue"], page_start = i["biblio"]["first_page"],
                    page_end = i["biblio"]["last_page"])

```

# License
BSD-3-Clause License

# Links
http://colav.udea.edu.co/



