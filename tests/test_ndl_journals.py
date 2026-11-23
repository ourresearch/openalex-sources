from jobs.ndl_journals import clean_publisher, parse, place_in_japan


def test_clean_publisher():
    assert clean_publisher("東京 : ぎょうせい, [1958]-") == "ぎょうせい"
    assert clean_publisher("2007-2017") == ""
    assert clean_publisher("精糖技術研究会, [19--]-") == "精糖技術研究会"
    assert clean_publisher("富山 : 富山大学人間発達科学部 =The Faculty of Human Development, University of Toyama, 2006-2021") \
        == "富山大学人間発達科学部"
    assert clean_publisher("久留米大学法学部[ほか") == "久留米大学法学部"
    assert clean_publisher("日本原子力研究開発機構, c2005-") == "日本原子力研究開発機構"


def test_place_in_japan():
    assert place_in_japan("東京 : ぎょうせい, [1958]-")
    assert place_in_japan("[名古屋] : パーリ学仏教文化学会 ; 1988-")
    assert place_in_japan("西原町 (沖縄県) : 琉球大学, 1990-")
    assert place_in_japan("Sendai : Tohoku University")
    assert not place_in_japan("Bangkok : 盤谷日本人商工会議所")
    assert not place_in_japan("Carlton : Blackwell Pub. Asia Pty Ltd.")
    assert not place_in_japan("北京 : 中国社会科学院")
    assert not place_in_japan("")


def test_parse_row():
    line = "\t".join(["000000013236", "c", "0514-2512", "ZD3", "Z4-1", "税理 : 税理士と関与先のための総合誌 / ぎょうせい 編.",
                      "ゼイリ", "東京 : ぎょうせい ; 1958-", "月刊", "(1) 1958～", "C", ""])
    (r,) = parse(line, "sairokushi_202610")
    assert r["bibid"] == "000000013236" and r["issns"] == ["0514-2512"] and r["publisher"] == "ぎょうせい"
    assert r["title"] == "税理 : 税理士と関与先のための総合誌" and r["place_in_japan"] and r["first_year"] == 1958
