from jobs.clean_source_names import clean_source_name as c


def test_strips_issn_clauses():
    assert c("AI Tech International Journal, ISSN: 3079-4749") == "AI Tech International Journal"
    assert c("Prospectus (ISSN: 2674-8576)") == "Prospectus"
    assert c("Borneo International Journal eISSN 2636-9826") == "Borneo International Journal"
    assert c("World Quarterly p-ISSN 3051-4096 e-ISSN 3051-410X") == "World Quarterly"
    assert c("International Journal of Islamic Thoughts ISSN: 2306-7012 (Print), 2313-5700 (Online)") == "International Journal of Islamic Thoughts"
    assert c("Via Litterae (ISSN 2176-6800): Revista de Linguística e Teoria Literária") == "Via Litterae: Revista de Linguística e Teoria Literária"
    assert c("Journal of Modern Academic Social Science ISSN: 3056-9958 (Online)") == "Journal of Modern Academic Social Science"
    assert c("YACHAQ- ISSN-L 2663-4155 (Virtual) e-ISSN 2617-5495 (Impresa)") == "YACHAQ"
    assert c("Anais da Semana dos Povos Indígenas (SPI) (ISSN em fase de registro)") == "Anais da Semana dos Povos Indígenas (SPI)"


def test_strips_marketing():
    assert c("Ajasraa ISSN 2278-3741 UGC CARE 1") == "Ajasraa"
    assert c("Universal Journal of Advanced Studies P-ISSN -3051-0570 ,E-ISSN -3051-0589 Impact Factor: 6.8") == "Universal Journal of Advanced Studies"
    assert c("Research Stream (a Bi-Annual, Open Access, Peer Reviewed International Journal) eISSN 3049-2610") == "Research Stream"
    assert c("Purakala with ISSN 0971-2143 is an UGC CARE Journal") == "Purakala"
    assert c("Academic Social Research:(P),(E) ISSN: 2456-2645, Impact Factor: 6.901 Peer-Reviewed, International Refereed Journal") == "Academic Social Research"


def test_leaves_real_names_alone():
    for t in ["Neo Science Peer Reviewed Journal", "QUEST - A Peer Reviewed Research Journal", "Plain Title",
              "(Journal didactique des sciences de l'éducation", "AH-Scopus to ORCID", "Nature"]:
        assert c(t) == t
    assert c(None) is None


def test_control_characters():  # oxjob #1891
    from sources_lib import clean_control_chars as cc
    # MARC non-sort marks around a leading article (Unicode C1 and MARC-8 forms, doubled marks)
    assert cc("\x98The \x9ccryosphere") == "The cryosphere"
    assert cc("Pharmaceutical journal/\x98The \x9cpharmaceutical journal") == "Pharmaceutical journal/The pharmaceutical journal"
    assert cc("\x88Das \x89Tierreich") == "Das Tierreich"
    assert cc("\x98\x98Th\x9ce \x9cPalawan scientist") == "The Palawan scientist"
    # a lone Windows-1252 dash / quote
    assert cc("Cahiers Bruxellois \x96 Brusselse Cahiers") == "Cahiers Bruxellois – Brusselse Cahiers"
    assert cc("AUSTRIB \x9298, Tribology at Work") == "AUSTRIB ’98, Tribology at Work"
    # UTF-8 read as Windows-1252
    assert cc("Denmark, 14â\x80\x9319 May, 2017") == "Denmark, 14–19 May, 2017"
    assert cc("Ù…Ø¬Ù„Ù‡ Ø¯Ø§Ù†Ø´Ú©Ø¯Ù‡ Ø¯Ù†Ø¯Ø§Ù†Ù¾Ø²Ø´Ú©ÛŒ Ø§ØµÙ\x81Ù‡Ø§Ù†") == "مجله دانشکده دندانپزشکی اصفهان"
    # a stray control after a correct letter; newlines and tabs
    assert cc("Hrvatski č\x8dasopis za javno zdravstvo") == "Hrvatski časopis za javno zdravstvo"
    assert cc("Journal of\nthings\t ") == "Journal of things"
    # names without controls are returned as they are, accents and curly quotes included
    for t in ["The Cryosphere", "Revista Española", "L’Homme", "  spaced  ", None, ""]:
        assert cc(t) == t
    assert c("\x98The \x9cjournal, ISSN: 3079-4749") == "The journal"
    # repair mode keeps every letter and digit (walden's name keys unchanged)
    assert cc("Denmark, 14â\x80\x9319 May", decode_mojibake=False) == "Denmark, 14â“19 May"
    assert cc("\x98The \x9ccryosphere", decode_mojibake=False) == "The cryosphere"
