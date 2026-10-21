"""fetch_source_list ddh adapter: OAI-PMH paging + JMEF parsing, offline."""
from jobs import fetch_source_list as fsl

NS = ('xmlns="http://www.openarchives.org/OAI/2.0/"')
JM = 'xmlns="https://raw.githubusercontent.com/CeON/jmef/"'


def _journal(ddh_id, title, issns, no_fees="true"):
    issn_xml = "".join(f'<issn publication-format="electronic">{i}</issn>' for i in issns)
    return f"""
    <record>
      <header><identifier>oai:ddh.edch.eu:{ddh_id}</identifier><datestamp>2026-10-01T00:00:00Z</datestamp></header>
      <metadata>
        <journal {JM}>
          <id type="ddh">{ddh_id}</id>
          <title-group><title language-iso2="ENG">{title}</title>
            <other-title type="translation" language-iso2="DEU">Translated</other-title></title-group>
          {issn_xml}
          <diamond-criteria>
            <scholarly-journal value="true"/><community-owned value="true"/>
            <open-access-with-open-licenses value="true"/><no-fees value="{no_fees}"/>
            <open-to-all-authors value="true"/>
          </diamond-criteria>
        </journal>
      </metadata>
    </record>"""


PAGE1 = f"""<?xml version="1.0"?><OAI-PMH {NS}><ListRecords>
  {_journal(42, "AI-Linguistica", ["2943-0070"])}
  <record><header status="deleted"><identifier>oai:ddh.edch.eu:21</identifier>
    <datestamp>2025-08-21T00:00:00Z</datestamp></header></record>
  {_journal(43, "No ISSN Journal", [])}
  <resumptionToken expirationDate="2026-11-20T00:00:00Z">TOKEN1</resumptionToken>
</ListRecords></OAI-PMH>""".encode()

PAGE2 = f"""<?xml version="1.0"?><OAI-PMH {NS}><ListRecords>
  {_journal(44, "Fee  Charging\n Journal", ["1234-5679", "2049-3630"], no_fees="false")}
  {_journal(42, "AI-Linguistica", ["2943-0070"])}
  <resumptionToken/>
</ListRecords></OAI-PMH>""".encode()


def test_fetch_ddh_pages_and_parses(monkeypatch):
    calls = []

    def fake_get(url, **kw):
        calls.append(url)
        return PAGE1 if "metadataPrefix" in url else PAGE2

    monkeypatch.setattr(fsl, "_get", fake_get)
    monkeypatch.setattr(fsl.time, "sleep", lambda s: None)
    rows = fsl.fetch_ddh()

    assert calls[1].endswith("verb=ListRecords&resumptionToken=TOKEN1")
    assert len(calls) == 2
    assert rows[0] == {"name": "AI-Linguistica", "issns": "2943-0070", "active": "true",
                       "withdrawn_date": "", "withdrawal_reason": ""}
    assert rows[1]["name"] == "Fee Charging Journal"
    assert rows[1]["issns"] == "1234-5679;2049-3630"
    assert rows[1]["active"] == "false"
    assert "no-fees" in rows[1]["withdrawal_reason"]
    assert len(rows) == 2  # deleted, ISSN-less and repeated records dropped


def test_fetch_ddh_oai_error(monkeypatch):
    err = f'<OAI-PMH {NS}><error code="badArgument">nope</error></OAI-PMH>'.encode()
    monkeypatch.setattr(fsl, "_get", lambda url, **kw: err)
    try:
        fsl.fetch_ddh()
    except SystemExit as e:
        assert "badArgument" in str(e)
    else:
        raise AssertionError("expected SystemExit")
