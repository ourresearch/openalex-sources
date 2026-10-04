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
