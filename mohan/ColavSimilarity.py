import re
from unidecode import unidecode
from thefuzz import fuzz, process


def parse_doi(doi):
    """
    Clean a doi string to get the doi code only.
    Parameters:
    -----------
    doi: str doi string

    Returns:
    --------
    doi: str doi code
    """
    return re.sub(r'https*\:\/\/[\w\.]+\/', '', doi.lower())


def parse_string(string: str):
    """
    Clean a string to get rid of html tags, latex tags, special characters and accents.

    Parameters:
    -----------
    string: str string to be cleaned

    Returns:
    --------
    data: str cleaned string
    """
    data = str(string).lower()
    data = re.sub(r'<[^>]+>', '', data)
    data = re.sub(r'[\$_\^]', '', re.sub(r'\\\w+', '', data))
    data = unidecode(data)
    return data


def __colav_similarity(title1, title2, journal1, journal2, year1, year2, ratio_thold=90, partial_thold=95, low_thold=80, verbose=0):
    """
    Core function to compare two papers to know if they are the same or not.
    Customized for the Colav project.
    Parameters:
    -----------
    title1: str title of one of the papers
    title2: str title of the other paper
    journal1: str name of the journal in which one of the papers was published
    journal2: str name of the journal in which the other paper was published
    year1: int year in which one of the papers was published
    year2: int year in which the other paper was published
    ratio_thold: int threshold to compare through ratio function in thefuzz library
    partial_ratio_thold: int threshold to compare through partial_ratio function in thefuzz library
    low_thold: int threshold to discard some results with lower score values
    verbose: int level of verbosity

    Returns:
    --------
    label: bool true when the papers are (potentially) the same.
    """
    label = False

    # Se revisa si los años y las revistas coinciden
    journal_check = False
    if journal1 and journal2:
        if fuzz.partial_ratio(unidecode(journal1.lower()), unidecode(journal2.lower())) > ratio_thold:
            journal_check = True
    year_check = False
    if year1 and year2:
        if year1 == year2:
            year_check = True

    length_check = False
    if len(title1.split()) > 3 and len(title2.split()) > 3:
        length_check = True

    # Si son pocas palabras y no hay por lo menos revista o año para revisar, se descarta de uan vez
    if length_check == False and (journal_check == False or year_check == False):
        return label

    if verbose == 5:
        if journal_check:
            print("Journals are the same")
        if year_check:
            print("Years are the same")

    ratio = fuzz.ratio(title1, title2)
    if verbose == 5:
        print("Initial ratio: ", ratio)
    if ratio > ratio_thold and length_check:  # Comparación "directa"
        label = True
    if label == False:
        # Comparaciones cuando el título viene en varios idiomas
        title1_list = title1.split("[")
        title2_list = title2.split("[")
        if min([len(item) for item in title1_list]) > 10 and min([len(item) for item in title2_list]) > 10:
            for title in title1_list:
                tmp_title, ratio = process.extractOne(
                    title, title2_list, scorer=fuzz.ratio)
                if ratio > ratio_thold:
                    label = True
                    break
            # if verbose==5: print("ratio over list: ",ratio)
            if label == False:
                for title in title1_list:
                    tmp_title, ratio = process.extractOne(
                        title, title2_list, scorer=fuzz.partial_ratio)
                    if ratio > partial_thold:
                        label = True
                        break
                    elif ratio > low_thold:
                        if journal_check and year_check:
                            label = True
                            break
                # if verbose==5: print("partial ratio over list: ",ratio)

    # Partial ratio section
    if label == False:
        # Cuando la comparación "directa" falla, relajamos el scorer
        ratio = fuzz.partial_ratio(title1, title2)
        # if verbose==5: print("partial ratio: ",ratio)

        # si el score supera el umbral (que debería ser mayor al umbral del ratio)
        if ratio > partial_thold and length_check:
            label = True
        elif ratio > low_thold:  # si no lo supera pero sigue siendo un valor alto, revisa el año y la revista
            if journal_check and year_check:
                label = True

    return label


def ColavSimilarity(title1: str, title2: str, journal1: str, journal2: str, year1, year2, ratio_thold=90, partial_thold=95, low_thold=80, use_regex=True):
    '''
    Compare two papers to know if they are the same or not.

    It uses the title, year and journal names of both papers to compare them in an somewhat elaborated way. Titles are compared using various algorithms of string comparison from thefuzz library with diferent levels of tolerance if its within a range determined by some threshold variables. The years must be the same, though we've seen an erro of +/- one year. Journal names are also compared using thefuzz's algorithms

    parameters
    ----------
        title1: str title of one of the papers
        title2: str title of the other paper
        journal1: str name of the journal in which one of the papers was published
        journal2: str name of the journal in which the other paper was published
        year1: int year in which one of the papers was published
        year2: int year in which the other paper was published
        ratio_thold: int threshold to compare through ratio function in thefuzz library
        partial_ratio_thold: int threshold to compare through partial_ratio function in thefuzz library
        low_thold: int threshold to discard some results with lower score values
        use_regex: bool Uses a regex to clean the titles

    Returns
    -------
    label: bool true when the papers are (potentially) the same.
    '''
    if title1 is None:
        title1 = ""
    if title2 is None:
        title2 = ""
    if not isinstance(journal1, str):
        journal1 = ""
    if not isinstance(journal2, str):
        journal2 = ""

    title1 = unidecode(title1.lower())
    title2 = unidecode(title2.lower())

    if year1:
        year1 = int(year1)
    if year2:
        year2 = int(year2)

    label = False

    if not use_regex:
        label = __colav_similarity(
            title1, title2, journal1, journal2, year1, year2, ratio_thold, partial_thold, low_thold)
    elif use_regex:
        label = __colav_similarity(parse_string(title1), parse_string(
            title2), journal1, journal2, year1, year2, ratio_thold, partial_thold, low_thold)
    return label
