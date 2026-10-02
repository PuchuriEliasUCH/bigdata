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
