from src.quotas import load_plan, proportional_quotas, save_plan


def test_quotas_are_proportional_to_yearly_volume():
    assert proportional_quotas({2011: 100, 2012: 300}, 40) == {2011: 10, 2012: 30}


def test_quotas_use_largest_remainder_and_sum_to_target():
    quotas = proportional_quotas({2011: 1, 2012: 1, 2013: 1}, 2)
    assert sum(quotas.values()) == 2
    assert sorted(quotas.values()) == [0, 1, 1]


def test_quota_never_exceeds_the_year_total():
    assert proportional_quotas({2011: 3, 2012: 5}, 100) == {2011: 3, 2012: 5}


def test_empty_years_get_no_quota():
    assert proportional_quotas({2011: 0, 2012: 0}, 10) == {2011: 0, 2012: 0}
    assert proportional_quotas({2011: 5}, 0) == {2011: 0}


def test_plan_is_reused_only_for_the_same_key(tmp_path):
    path = tmp_path / "plans" / "scopus.json"
    assert load_plan(path, {"target": 10}) is None
    save_plan(path, {"target": 10}, {"totals": {"2011": 5}, "quotas": {"2011": 5}})
    assert load_plan(path, {"target": 10})["quotas"] == {"2011": 5}
    assert load_plan(path, {"target": 20}) is None


def test_year_plan_is_computed_once_and_returned_with_integer_years(tmp_path):
    from src.quotas import ensure_year_plan

    calls = []

    def totals():
        calls.append(1)
        return {2011: 30, 2012: 10}

    path = tmp_path / "plans" / "src.json"
    first = ensure_year_plan(path, {"target": 8}, totals, 8)
    second = ensure_year_plan(path, {"target": 8}, totals, 8)
    assert first == second == ({2011: 30, 2012: 10}, {2011: 6, 2012: 2})
    assert len(calls) == 1
    ensure_year_plan(path, {"target": 9}, totals, 9)
    assert len(calls) == 2
