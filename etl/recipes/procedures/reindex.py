# -*- coding: utf-8 -*-

"""window procedure for recipes"""

import logging
from typing import List

import pandas as pd
from ddf_utils.chef.helpers import debuggable, read_opt, mkfunc
from ddf_utils.chef.model.ingredient import DataPointIngredient
from ddf_utils.chef.model.chef import Chef


logger = logging.getLogger('reindex')



ddf_time_formats = {
  'second': '%Y%m%dt%H%M%S',
  'minute': '%Y%m%dt%H%M',
  'hour': '%Y%m%dt%H',
  'day': '%Y%m%d',
  'month': '%Y-%m',
  'year': '%Y'
}
def _get_time_format(df, column):
  if column=='time':
    return _find_time_format(df[column].iloc[0], ddf_time_formats)
  else:
    return ddf_time_formats[column]

def _find_time_format(datestr, formats):
  for format in formats.values():
    try:
      pd.to_datetime(datestr, format=format)
      return format
    except:
      continue 
  assert False, f"Invalid time format passed: {datestr}"


@debuggable
def by_date(chef: Chef, ingredients: List[DataPointIngredient], result, column, freq, fill_value, range='local') -> DataPointIngredient:
    assert len(ingredients) == 1, "procedure only support 1 ingredient for now."
    # ingredient = chef.dag.get_node(ingredients[0]).evaluate()
    ingredient = ingredients[0]
    logger.info('reindex.by_date: ' + ingredient.id)

    data = ingredient.compute()
    newdata = dict()

    first_key = list(data.keys())[0]
    time_format = _get_time_format(data[first_key], column)

    for k, df in data.items():
        # keys for grouping. in multidimensional data like datapoints, we want create
        # groups before rolling. Just group all key column except the column to aggregate.
        keys = ingredient.key.copy()

        try:
            df[column] = pd.to_datetime(df[column], format=time_format)
        except:
            print(f'Could not parse {column} to format: {time_format}')
            raise


        # then remove the rolling column from primary keys, group by remaining keys
        keys.remove(column)

        def reindex(re_df):
          # reindex
          if range == 'local':
            reindexed = re_df.set_index(column).asfreq(freq=freq).reset_index()
          elif range == 'global':
            date_range = pd.date_range(start=df[column].min(), end=df[column].max(), freq=freq, name=column)
            reindexed = re_df.set_index(column).reindex(date_range).reset_index()
          else:
            assert False , f'Given range ({range}) is invalid, must be either \'local\' or \'global\''

          # fill newly added keys
          for k in keys:
            reindexed[k] = reindexed[k].fillna(value=re_df.iloc[0][k])

          # fill newly added indicators  
          values = list(set(re_df.columns) - set(ingredient.key))
          reindexed[values] = reindexed[values].fillna(0)
          return reindexed

        # reindex per other-keys
        newdata[k] = (df.groupby(by=keys, sort=False, group_keys=False)
                        .apply(reindex))
      
        # back to original dtype, ints for time
        newdata[k][column] = newdata[k][column].dt.strftime(time_format).astype(int)

    return DataPointIngredient.from_procedure_result(result, ingredient.key, newdata)