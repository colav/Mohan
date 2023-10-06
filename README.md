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
from pymongo import MongoClient

es_index = "openalex_index"

#creating the instance
s = Similarity(es_index,es_uri= "http://localhost:9200",
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
    authors = []
    for author in i['authorships']:
        if "display_name" in author["author"].keys():
            authors.append(author["author"]["display_name"])
    work["authors"] = authors
    
    entry = {"_index": es_index,
                "_id": str(i["_id"]),
                "_source": work}
    es_entries.append(entry)
    if len(es_entries) == bulk_size:
        s.insert_bulk(es_entries)
        es_entries = []
```
### example inserting one document from openalex
```py
work = {"title": i["title"],
        "source": i["host_venue"]["display_name"],
        "year": i["publication_year"],
        "authors": authors,
        "volume": i["biblio"]["volume"],
        "issue": i["biblio"]["issue"],
        "page_start": i["biblio"]["first_page"],
        "page_end": i["biblio"]["last_page"]}
res = s.insert_work(_id=str(i["_id"]), work=work)
```
### example performing a search

```py
res = s.search_work(title=i["title"], source = i["host_venue"]["display_name"], year = i["publication_year"],
                    authors = authors, volume = i["biblio"]["volume"], issue = i["biblio"]["issue"], 
                    page_start = i["biblio"]["first_page"], page_end = i["biblio"]["last_page"])

```

NOTES:
* The search is performed using the same fields as the insert_work method.
* The title field can be an array when inserting documents, but it will be used as a string when searching documents.
* Authors field have to be an array when inserting/searching documents.
* Extra fields can be added to the insert methods, but they will not be used for the search. Only the fields (title, source, year, authors, volume, issue, page_start, page_end) will be used for the search.

# License
BSD-3-Clause License

# Links
http://colav.udea.edu.co/



