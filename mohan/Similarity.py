
from mohan.ColavSimilarity import ColavSimilarity, parse_doi, parse_string
from mohan.Schemas import kahi_works
from elasticsearch import Elasticsearch, __version__ as es_version
from elasticsearch.helpers import bulk


class Similarity:
    def __init__(self, es_index, es_uri: str = "http://localhost:9200",
                 es_auth: tuple = ('elastic', 'colav'),
                 es_req_timeout: int = 120, schema=kahi_works):
        """
        Initialize the Similarity class.
        Parameters:
        -----------
        es_index: str 
                name of the index
        es_uri: str 
                uri of the elastic search server
        es_auth: tuple 
                authentication for the elastic search server
        es_req_timeout: int 
                elastic search request timeout
        """
        auth = es_auth
        if es_version[0] < 8:
            self.es = Elasticsearch(
                es_uri, http_auth=auth, timeout=es_req_timeout)
        else:
            self.es = Elasticsearch(
                es_uri, basic_auth=auth, timeout=es_req_timeout)
        self.es_index = es_index
        self.schema = schema

    def create_index(self, mapping: dict = None, recreate: bool = False):
        """
        Create an index.
        Parameters:
        -----------
        index_name: str name of the index
        mapping: dict mapping of the index
        recreate: bool whether to recreate the index or not

        """
        if recreate:
            self.delete_index(self.es_index)
        if mapping:
            self.es.indices.create(index=self.es_index, body=mapping)
        else:
            self.es.indices.create(index=self.es_index)

    def delete_index(self, index_name: str):
        """
        Delete an index.
        Parameters:
        -----------
        index_name: str name of the index
        """
        self.es.indices.delete(index=index_name)

    def search(self, title: str, authors: str, source: str, year: str,
               volume: str, issue: str, page_start: str, page_end: str,
               ratio_thold: int = 90, partial_thold: int = 95, low_thold: int = 80):
        """
        Compare two papers to know if they are the same or not.
        Parameters:
        -----------
        title: str 
                title of the paper
        authors: str 
                authors of the paper
        source: str 
                name of the journal in which the paper was published
        year: int 
                year in which the paper was published
        volume: int 
                volume of the journal in which the paper was published
        issue: int 
                issue of the journal in which the paper was published
        page_start: int 
                first page of the paper
        page_end: int 
                last page of the paper
        ratio_thold: int 
                threshold to compare through ratio function in thefuzz library
        partial_ratio_thold: int 
                threshold to compare through partial_ratio function in thefuzz library
        low_thold: int 
            threshold to discard some results with lower score values
        es_request_timeout: int elastic search request timeout

        Returns:
        --------
        record: dict when the papers are (potentially) the same, None otherwise.
        """
        if not isinstance(title, str):
            title = ""

        if not isinstance(source, str):
            source = ""

        if isinstance(volume, int):
            volume = str(volume)

        if isinstance(issue, int):
            issue = str(issue)

        if isinstance(page_start, int):
            page_start = str(page_start)

        if isinstance(page_end, int):
            page_end = str(page_end)

        if not isinstance(volume, str):
            volume = ""

        if not isinstance(issue, str):
            issue = ""

        if not isinstance(page_start, str):
            page_start = ""

        if not isinstance(page_end, str):
            page_end = ""

        body = {
            "query": {
                "bool": {
                    "should": [
                        {"match": {self.schema["title"]: title}},
                        # se tienen que truncar los autores
                        {"match": {self.schema["authors"]: authors[0:100]}},
                        {"match": {self.schema["source"]: source}},
                        {"term":  {self.schema["year"]: year}},
                        {"term":  {self.schema["volume"]: volume}},
                        {"term":  {self.schema["issue"]: issue}},
                        {"term":  {self.schema["page_start"]: page_start}},
                        {"term":  {self.schema["page_end"]: page_end}},
                    ],
                }
            },
            "size": 10,
        }

        res = self.es.search(index=self.es_index, **body)
        if res["hits"]["total"]["value"] != 0:
            for i in res["hits"]["hits"]:
                title2 = i["_source"]
                for j in self.schema["title"].split("."):
                    title2 = title2[j]
                source2 = i["_source"]
                for j in self.schema["source"].split("."):
                    source2 = source2[j]
                year2 = i["_source"]
                for j in self.schema["year"].split("."):
                    year2 = year2[j]

                value = ColavSimilarity(title, title2,
                                        source, source2,
                                        year, year2,
                                        ratio_thold=ratio_thold, partial_thold=partial_thold, low_thold=low_thold)
                if value:
                    return i
            return None
        else:
            return None
