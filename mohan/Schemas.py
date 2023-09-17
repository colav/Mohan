# Description: This file contains the schemas for the database collections.

class SchemaDict(dict):
    def __getattr__(self, attr):
        if attr in self:
            return self[attr]
        else:
            raise AttributeError(f"'DotDict' object has no attribute '{attr}'")


kahi_works = {"title": "titles.title",
              "authors": "authors.full_name",
              "source": "source.names.name",
              "year": "year_published",
              "volume": "bibliographic_info.volume",
              "issue": "bibliographic_info.issue",
              "page_start": "bibliographic_info.start_page",
              "page_end": "bibliographic_info.end_page"}

openalex_works = {"title": "title",
                  "authors": "authorships.author.display_name",
                  "source": "host_venue.display_name",
                  "year": "publication_year",
                  "volume": "biblio.volume",
                  "issue": "biblio.issue",
                  "page_start": "biblio.first_page",
                  "page_end": "biblio.last_page"}
