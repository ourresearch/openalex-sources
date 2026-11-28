from jobs.host_check import COMPANY_FAMILY, JOURNAL_PUBLISHER, PLATFORM, decide, family, titles_match

BMJ, ELSEVIER, WILEY, SPRINGER_NATURE = 4310319945, 4310320990, 4310320595, 4310319965
SRC = {"publisher_id": BMJ, "institution_id": None, "override_timestamp": None,
       "display_name": "Annals of the Rheumatic Diseases"}
EV = {"journal_publisher": "Elsevier", "journal_title": "Annals of the Rheumatic Diseases",
      "newest_doi": "10.1016/j.ard.2026.07.008", "newest_publisher": "Elsevier BV",
      "facet": {"Elsevier BV": 72876}, "landing_host": "linkinghub.elsevier.com"}


def test_moves_a_transferred_journal():
    status, target, why = decide(SRC, EV)
    assert status == "move" and target[0] == ELSEVIER, why


def test_family_patterns():
    assert family("Springer Science and Business Media LLC") == "springer"
    assert family("Informa UK Limited") == "tf"
    assert family("Ovid Technologies (Wolters Kluwer Health)") == "wk"
    assert family("Walter de Gruyter GmbH (Brill)") == "degruyter"
    # a society named for nature conservation is not Springer Nature; a university named Ovidius is not Ovid
    assert family("Hungarian Ornithological and Nature Conservation Society") is None
    assert family("Ovidius University") is None


def test_every_target_has_a_family_and_platform():
    for pid, fam, name in JOURNAL_PUBLISHER.values():
        assert COMPANY_FAMILY[pid] == fam and fam in PLATFORM and name


def test_same_host_and_unmapped():
    assert decide({**SRC, "publisher_id": ELSEVIER}, EV)[0] == "same_host"
    assert decide(SRC, {**EV, "journal_publisher": "ASME International"})[0] == "held"


def test_never_moves_curated_institution_or_society_hosted_rows():
    assert decide({**SRC, "override_timestamp": "2025-04-11"}, EV)[0] == "held"
    assert decide({**SRC, "institution_id": 123}, EV)[0] == "held"
    # ASME (a society record) on its way to Wiley: never moved here
    asme = {**SRC, "publisher_id": 4310321320, "display_name": "Journal of Applied Mechanics"}
    ev = {**EV, "journal_publisher": "Wiley (John Wiley & Sons)", "journal_title": "Journal of Applied Mechanics",
          "newest_publisher": "Wiley", "facet": {"Wiley": 9000, "ASME International": 6000},
          "landing_host": "asme.onlinelibrary.wiley.com"}
    assert decide(asme, ev)[0] == "held"


def test_imprint_change_within_one_company_is_not_a_transfer():
    sn = {**SRC, "publisher_id": SPRINGER_NATURE}
    ev = {**EV, "journal_publisher": "Springer (Biomed Central Ltd.)", "newest_publisher": "Springer Science and Business Media LLC",
          "facet": {"Springer Science and Business Media LLC": 100}, "landing_host": "x.biomedcentral.com"}
    assert decide(sn, ev)[0] == "held"


def test_holds_when_backfile_site_newest_or_title_disagree():
    assert decide(SRC, {**EV, "facet": {"Elsevier BV": 300, "BMJ": 700}})[0] == "held"
    assert decide(SRC, {**EV, "landing_host": "ard.bmj.com"})[0] == "held"
    assert decide(SRC, {**EV, "newest_publisher": "BMJ"})[0] == "held"
    assert decide(SRC, {**EV, "journal_title": "Rheumatology Advances in Practice"})[0] == "held"


def test_titles_match():
    assert titles_match("Knee Surgery, Sports Traumatology, Arthroscopy", "Knee Surgery Sports Traumatology Arthroscopy")
    assert not titles_match("Audiological Medicine", "Hearing Balance and Communication")
    assert not titles_match(None, "x")


def test_partner_platforms():
    import re
    assert re.search(PLATFORM["tup"], "ieeexplore.ieee.org")
    assert re.search(PLATFORM["frontiers"], "www.frontierspartnerships.org")
    assert not re.search(PLATFORM["wiley"], "asmedigitalcollection.asme.org")
