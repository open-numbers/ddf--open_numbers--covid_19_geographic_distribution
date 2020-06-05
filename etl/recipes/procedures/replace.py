# -*- coding: utf-8 -*-

"""window procedure for recipes"""

import logging
from typing import List

import pandas as pd
from ddf_utils.chef.helpers import debuggable, read_opt, mkfunc, query
from ddf_utils.chef.model.ingredient import Ingredient, get_ingredient_class
from ddf_utils.chef.model.chef import Chef

logger = logging.getLogger('replace')

@debuggable
def by_query(chef: Chef, ingredients: List[Ingredient], result, **options) -> Ingredient:
    assert len(ingredients) == 1, "procedure only support 1 ingredient for now."
    ingredient = ingredients[0]
    logger.info('replace.by_query: ' + ingredient.id)

    data = ingredient.compute()
    newdata = dict()

    row_filters = read_opt(options, 'rows', False, None)
    columns = read_opt(options, 'columns', False, None)
    columns = [columns] if isinstance(columns, str) else columns
    value = read_opt(options, 'value', False, None)

    newdata = dict()
    data = ingredient.get_data()
    keys = ingredient.key

    for k, df in data.items():
        if k in columns:
            # Dask df can't use multi-index, so joining key values to one index
            # Dask df doesn't support index assignment (df.loc[filtered, column] = 0), so using mask
            def create_index(df, keys):
                return df[keys].astype(str).apply('-'.join, axis=1, meta='str')
            index = create_index(df, keys)
            filtered = create_index(query(df, row_filters, available_scopes=df.columns), keys)
            # need to compute() to pandas df since dask doesn't support .isin(daskdf)
            df[k] = df[k].mask(index.isin(filtered.compute()), 0)
        newdata[k] = df

    return get_ingredient_class(ingredient.dtype).from_procedure_result(result, ingredient.key, data_computed=newdata)
