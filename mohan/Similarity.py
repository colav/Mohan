from hunahpu.Similarity import ColavSimilarity, parse_string
from elasticsearch import Elasticsearch, __version__ as es_version
from elasticsearch.helpers import bulk
import sys
from unidecode import unidecode


class Similarity:
    def __init__(self, es_index, es_uri: str = "http://localhost:9200",
                 es_auth: tuple = ('elastic', 'colav'),
                 es_req_timeout: int = 120,
                 es_retry_on_timeout: bool = True,  # enable timeout
                 es_max_retries: int = 5,
                 ):
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
                es_uri, http_auth=auth, timeout=es_req_timeout, retry_on_timeout=es_retry_on_timeout, max_retries=es_max_retries)
        else:
            self.es = Elasticsearch(
                es_uri, basic_auth=auth, timeout=es_req_timeout, retry_on_timeout=es_retry_on_timeout, max_retries=es_max_retries)
        self.es_index = es_index
        self.es_req_timeout = es_req_timeout
        self.ensure_index()

    def str_normilize(self, word):
        """
        Normalize a string to lowercase and remove accents.

        Parameters
        ----------
        word : str
            string to be normalized.

        Returns
        -------
        str
            normalized string.
        """
        return unidecode(word).lower().strip().replace(".", "")

    def refresh_index(self):
        """
        Refresh the index.
        """
        self.es.indices.refresh(index=self.es_index)

    def ensure_index(self, mapping: dict = None, recreate: bool = False):
        """
        Create an index.
        Parameters:
        -----------
        index_name: str
            name of the index
        mapping: dict
            mapping of the index
        recreate: bool
            whether to recreate the index or not
        """
        if recreate:
            if self.es.indices.exists(index=self.es_index):
                self.delete_index(self.es_index)
        if self.es.indices.exists(index=self.es_index):
            return
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

    def insert_work(self, _id: str, work: dict):
        """
        Insert a work into the index.
        work should have a dict structure like the next one.
        work = {"title": "title of the work",
                "source": "source of the work",
                "year": "year of the work",
                "authors": "list of authors, separated by commas and maximum 5 authors",
                "volume": "volume of the work",
                "issue": "issue of the work",
                "page_start": "page start of the work",
                "page_end": "page end of the work"}
        every value is a string, including the year, volume, issue, page_start and page_end.

        Additional fields such as doi, pmid, pmcid, etc. can be added to the work dict if needed,
        but the search is over the previous fields defined in work.

        Parameters:
        -----------
        _id: str id of the work (ex: mongodb id as string)
        work: dict work to be inserted
        """
        for i in work.keys():
            if i != "authors":
                work[i] = str(work[i])
        response = self.es.index(index=self.es_index,  id=_id, document=work)
        self.refresh_index()
        return response

    def search_work(self, title: str, source: str, year: str, authors: str,
                    volume: str, issue: str, page_start: str, page_end: str,
                    use_es_thold: bool = False, es_thold: int = 130,
                    ratio_thold: int = 90, partial_thold: int = 92, low_thold: int = 81, parse_title: bool = True, hits=1):
        """
        Compare two papers to know if they are the same or not.
        By default the function uses the elastic search score threshold to return the best hit.

        Parameters:
        -----------
        title: str 
                title of the paper
        source: str 
                name of the journal in which the paper was published
        year: int 
                year in which the paper was published
        authors: list
                authors of the paper, list of maximum 5 authors
        volume: int 
                volume of the journal in which the paper was published
        issue: int 
                issue of the journal in which the paper was published
        page_start: int 
                first page of the paper
        page_end: int 
                last page of the paper
        use_es_thold: bool
                whether to use the elastic search score threshold or not
        es_thold: int
                elastic search score threshold to return the best hit
        ratio_thold: int 
                threshold to compare through ratio function in thefuzz library
        partial_ratio_thold: int 
                threshold to compare through partial_ratio function in thefuzz library
        low_thold: int
                threshold to discard some results with lower score values
        es_request_timeout: int
                elastic search request timeout
        parse_title: bool
                whether to parse the title or not (parse title helps to improve the results)
        hits: int
                number of hits to return, only if use_es_thold is True
        Returns:
        --------
        record: dict when the papers are (potentially) the same, None otherwise.
        """
        if not isinstance(title, str):
            title = ""

        title = self.str_normilize(title)

        if not isinstance(source, str):
            source = ""

        if isinstance(year, int):
            year = str(year)

        authors_list = []
        if isinstance(authors, list):
            for author in authors:
                authors_list.append(
                    {"match": {"authors":  {
                        "query": self.str_normilize(author),
                        "operator": "AND"
                    }}}
                )
        else:
            print(
                "Error, Authors should be list, if you dont have authors, please use []")
            sys.exit(1)

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
        if parse_title:
            title = parse_string(title)
        body = {
            "query": {
                "bool": {
                    "should": [
                        {"match": {"title":  {
                            "query": title,
                            "operator": "OR"
                        }}},
                        {"match": {"source":  {
                            "query": source,
                            "operator": "AND"
                        }}},
                        {"term":  {"year": year}},
                        {"term":  {"volume": volume}},
                        {"term":  {"issue": issue}},
                        {"term":  {"page_start": page_start}},
                        {"term":  {"page_end": page_end}},
                    ],
                }
            },
            "size": 20,
        }
        body["query"]["bool"]["should"].extend(authors_list)

        res = self.es.search(index=self.es_index, **body)
        if res["hits"]["total"]["value"] != 0:
            if use_es_thold:
                hits_found = []
                for hit in res["hits"]["hits"][0:hits]:
                    if hit["_score"] >= es_thold:
                        hits_found.append(hit)
                return hits_found
            for i in res["hits"]["hits"]:
                paper1 = {}
                paper1["title"] = title
                paper1["journal"] = source
                paper1["year"] = year

                paper2 = {}
                paper2["title"] = i["_source"]["title"]
                paper2["journal"] = i["_source"]["source"] if "source" in i["_source"].keys(
                ) else ""
                paper2["year"] = i["_source"]["year"] if "year" in i["_source"].keys(
                ) else ""
                if "year" not in i["_source"].keys():
                    print(i)
                if "source" not in i["_source"].keys():
                    print(i)

                value = ColavSimilarity(
                    paper1, paper2, ratio_thold=ratio_thold, partial_thold=partial_thold, low_thold=low_thold)
                if value:
                    return i
            return None
        else:
            return None

    def insert_bulk(self, entries: list, refresh=True):
        """
        Insert a bulk of works into the index.
        Parameters:
        -----------
        entries: list 
                list of works to be inserted
        """
        response = bulk(self.es, entries, index=self.es_index,
                        refresh=refresh, request_timeout=self.es_req_timeout)
        self.refresh_index()
        return response

    def close(self):
        """
        Close the connection to the elastic search server.
        """
        self.es.close()
