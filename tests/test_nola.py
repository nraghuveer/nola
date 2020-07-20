import pytest


from nola import nola


def test_sampleapi():
    db = nola.db.new(connection_string)
    view = db.view('tablename')

    # window represents an abstract of a filtered query on view/table
    window = view.col1==1 & view.col2==3
    # by default returns a df
    df = window.select(view.col1)

    for row in (view.col1==1).orderby(view.createdon, "desc").limit(10).select():
        print(row)  # tuple
    
    df = view.groupby(view.usertype).select(view.usertype, nola.count())  # same as select * from view groupby "usertype"


    # automatically detect jsonb columns and adapt the queries
    # needs more thought and coffee
    # animal is jsonb columns
    {
        "lion": {
            "strength": 10,
            "pet_level": 0
        },
        "cow": {
            "strength": 2,
            "pet_level": 10
        }
    }
    # above schema...
    jdict = view.jsonb.dict
    view.animal.set_jsonb_scheme("name", string, jdict("character", string, jdict(string, int)))
    # get all animals having pet level above 7
    petanimals = (view.animal.character=="pet_level" & view.animal.character.value > 7).select(view.animal.name).name.aslist()
