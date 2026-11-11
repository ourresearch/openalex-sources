from jobs.reconcile_issn_l import classify as c


def test_repoint_when_the_table_issn_l_is_owned():
    # Journal of Materials Chemistry A: stored the print ISSN, the ISSN IC gives the online one
    assert c(2764437742, "2050-7488", ["2050-7488", "2050-7496"], ["2050-7496"], None) == ("repoint", "2050-7496")


def test_agrees_is_left_alone():
    assert c(1, "2050-7496", ["2050-7488", "2050-7496"], ["2050-7496"], None) == ("agrees", None)


def test_two_families_never_written():
    assert c(1, "1111-1111", ["1111-1111", "2222-2222"], ["3333-3333", "4444-4444"], None) == ("multi_family", None)


def test_missing_issn_l_adopted_only_when_vetted():
    args = (7, "1052-3812", ["1052-3812"], ["1949-4645"], None)
    assert c(*args) == ("missing", "1949-4645")
    assert c(*args, adopt_ids=frozenset({7})) == ("adopt", "1949-4645")
    assert c(*args, adopt_ids=frozenset({7}), shared=frozenset({"1949-4645"})) == ("shared_missing", "1949-4645")


def test_issn_l_owned_by_another_source_is_a_merge_question():
    assert c(7, "1052-3812", ["1052-3812"], ["1949-4645"], 99, adopt_ids=frozenset({7})) == ("owned_elsewhere", "1949-4645")
