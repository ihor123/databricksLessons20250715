from TestBundle3.main import get_taxis, get_spark
from TestBundle3.helper.hp import hp


def test_main():
    taxis = get_taxis(get_spark())
    assert taxis.count() > 5

def test_hp_add_standard_columns_all():
    hp_instance = hp(get_spark())
    df = get_taxis(get_spark())
    df2 = hp_instance.add_standard_columns(df, createdBy="test_user", modifiedBy="test_user")
    assert "createdBy" in df2.columns
    assert "modifiedBy" in df2.columns
    assert df2.filter("createdBy != 'test_user'").count() == 0
    assert df2.filter("modifiedBy != 'test_user'").count() == 0
    assert "createdOn" in df2.columns
    assert "modifiedOn" in df2.columns
    assert df2.filter("createdOn is null").count() == 0
    assert df2.filter("modifiedOn is null").count() == 0

def test_hp_add_standard_columns_creat():
    hp_instance = hp(get_spark())
    df = get_taxis(get_spark())
    df2 = hp_instance.add_standard_columns(df, createdBy="test_user")
    assert "createdBy" in df2.columns
    assert "modifiedBy" in df2.columns
    assert df2.filter("createdBy != 'test_user'").count() == 0
    assert df2.filter("modifiedBy  is not null").count() == 0
    assert "createdOn" in df2.columns
    assert "modifiedOn" in df2.columns
    assert df2.filter("createdOn is null").count() == 0
    assert df2.filter("modifiedOn is not null").count() == 0
