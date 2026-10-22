"""jobs.add_journal pure helpers, offline (no DB, no network)."""
from jobs import add_journal as aj

OAI = 'xmlns="http://www.openarchives.org/OAI/2.0/" xmlns:dc="http://purl.org/dc/elements/1.1/"'


def _page(records, token=""):
    return f"""<?xml version="1.0"?><OAI-PMH {OAI}><ListRecords>{records}{token}</ListRecords></OAI-PMH>""".encode()


def _rec(n, src, deleted=False, date="2024-05-01"):
    if deleted:
        return f'<record><header status="deleted"><identifier>oai:ojs2.example.org:article/{n}</identifier></header></record>'
    return (f"<record><header><identifier>oai:ojs2.example.org:article/{n}</identifier></header><metadata>"
            f"<dc:title>T{n}</dc:title><dc:source>{src}</dc:source><dc:publisher>Example Press</dc:publisher>"
            f"<dc:date>{date}</dc:date></metadata></record>")


def test_valid_issn():
    assert aj.valid_issn("2757-6132") and aj.valid_issn("3139-9835") and aj.valid_issn("1896-396X")
    assert not aj.valid_issn("1920-1930")  # a year range in dc:source, not an ISSN


def test_journal_key():
    assert aj.journal_key("https://h.org/index.php/index/oai", "xphi") == "xphi"
    assert aj.journal_key("https://h.org/index.php/index/oai", None) is None          # whole install
    assert aj.journal_key("https://h.org/index.php/zerl/oai", None) == "zerl"
    assert aj.journal_key("https://h.org/asian-health/oai", None) == "asian-health"   # restful OJS URL
    assert aj.journal_key("https://hrcak.srce.hr/oai", None) is None


def test_overlaps_is_conservative():
    inst = "https://journals.ub.uni-koeln.de/index.php/index/oai"
    assert aj.overlaps(inst, "xphi", inst, None)                                      # install-wide row
    assert aj.overlaps(inst, "xphi", "https://journals.ub.uni-koeln.de/index.php/xphi/oai", None)
    assert not aj.overlaps(inst, "xphi", "https://journals.ub.uni-koeln.de/index.php/AAeo/oai", None)
    assert not aj.overlaps(inst, "JFDA", inst, "lr")
    assert aj.overlaps(inst, "JFDA", inst, "jfda")                                    # same set, other case
    assert aj.overlaps("https://hrcak.srce.hr/oai/", "journal:479", "https://hrcak.srce.hr/oai/", "openaire")
    assert not aj.overlaps("https://hrcak.srce.hr/oai/", "journal:479", "https://hrcak.srce.hr/oai/", "journal:12")


def test_pick_set():
    sets = [("IJAPOB", "Int J Poultry"), ("IJAPOB:ART", "Articles")]
    assert aj.pick_set(sets, "https://www.ijapob.com/index.php/IJAPOB")[0] == "IJAPOB"
    multi = [("lr", "Landscape Review"), ("JFDA", "The Journal of FinTech"), ("JFDA:ART", "Articles")]
    assert aj.pick_set(multi, "https://journals.lincoln.ac.nz/index.php/JFDA/index")[0] == "JFDA"
    assert aj.pick_set(multi, "https://journals.lincoln.ac.nz/")[0] is None          # can't tell: HOLD
    assert aj.pick_set([("x", "Only Journal")], "https://example.org/")[0] == "x"     # single-journal install


def test_oai_candidates_prefers_install_url():
    c = aj.oai_candidates("https://ijamsr.org/index.php/ijamsr/about")
    assert c[0] == "https://ijamsr.org/index.php/index/oai" and "https://ijamsr.org/index.php/ijamsr/oai" in c


def test_parse_records():
    src = "Int J Poultry; Vol. 3 No. 1 (2024); 1-10; 2757-6132"
    page = _page(_rec(1, src) + _rec(2, src, date="2021-01-01") + _rec(3, "", deleted=True),
                 '<resumptionToken completeListSize="24">t</resumptionToken>')
    r = aj.parse_records(page)
    assert r["size"] == 24 and r["records_on_page"] == 3 and r["deleted_on_page"] == 1
    assert r["issns"] == ["2757-6132"] and r["title"] == "Int J Poultry" and r["publisher"] == "Example Press"
    assert r["id_host"] == "ojs2.example.org" and r["years"] == (2021, 2024)
    err = aj.parse_records(f'<?xml version="1.0"?><OAI-PMH {OAI}><error code="noRecordsMatch"/></OAI-PMH>'.encode())
    assert err == {"error": "noRecordsMatch"}
