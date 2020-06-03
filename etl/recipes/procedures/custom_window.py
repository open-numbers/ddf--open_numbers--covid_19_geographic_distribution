from ddf_utils.chef.model.chef import Chef
from ddf_utils.chef.model.ingredient import ProcedureResult
from ddf_utils.chef.helpers import debuggable

@debuggable  # adding debug option to the procedure
def one_week_before(chef, ingredients, result, **options):
    # you must have chef(a Chef object), ingredients (a list of string),
    # result (a string) as parameters
    #

    # procedures...

    # and finally return a ProcedureResult object
    return ProcedureResult(chef, result, primarykey, data)