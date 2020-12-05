import pandas as pd
import os
from scipy import stats

project_path = "c:\\Users\\Anna\\Desktop\\AI_camp\\YT\\"

data_path = os.path.join(project_path, "data")
supporting_dp = os.path.join(data_path, "supporting")

borders = pd.read_csv(os.path.join(supporting_dp,'regions.csv')
                      , sep = ';', header =0)

cleaning_columns = pd.read_csv(os.path.join(supporting_dp,'cleaned_columns_of_interest.csv')
                               , sep = ';', header =0)
cleaned_columns_uq = cleaning_columns.cleaned.unique()

long_borders_list = list(set(borders.west.tolist() + borders.east.tolist()))
lat_borders_list = list(set(borders.south.tolist() + borders.north.tolist()))

long_borders_list.sort()
lat_borders_list.sort()

long_borders = [borders.west.min().tolist(), borders.east.max().tolist()]
lat_borders  = [borders.south.min().tolist(), borders.north.max().tolist()]


def hours_only(ts):
    t = pd.DataFrame({'Year': ts.dt.year
                  ,'Month':ts.dt.month
                  ,'Day':ts.dt.day
                  ,'Hour':ts.dt.hour })
    return(pd.to_datetime(t))


def basic_data_cleaning_and_new_features_wk1(dt):

    dt['cleaned_tpep_pickup_datetime'] = pd.to_datetime(dt['cleaned_tpep_pickup_datetime'])
    dt['cleaned_tpep_dropoff_datetime'] = pd.to_datetime(dt['cleaned_tpep_dropoff_datetime'])

    # simple approach under assumption that NYC is actually a square
    dt['is_in_NYC'] = 0

    dt['cleaned_tpep_pickup_datetime_short'] = hours_only(dt['cleaned_tpep_pickup_datetime'])
    dt['cleaned_tpep_dropoff_datetime_short'] = hours_only(dt['cleaned_tpep_dropoff_datetime'])

    dt.loc[(dt.cleaned_pickup_longitude >= long_borders[0])
           & (dt.cleaned_pickup_longitude <= long_borders[1])
           & (dt.cleaned_pickup_latitude >= lat_borders[0])
           & (dt.cleaned_pickup_latitude <= lat_borders[1])
    , 'is_in_NYC'] = 1

    ### cleaning
    return (dt.loc[(dt.is_in_NYC == 1)
                   & (dt.cleaned_trip_distance > 0)
                   & (dt.cleaned_passenger_count > 0)
                   & ~(dt.cleaned_tpep_pickup_datetime == dt.cleaned_tpep_dropoff_datetime)])

def counts_per_region(x,y):
    tmp = stats.binned_statistic_2d(x, y
                          , None, 'count'
                          , bins=[long_borders_list,lat_borders_list]
                          , expand_binnumbers=True)
    return(tmp.statistic.reshape((1, 2500)))


def get_counts_per_hour_per_region(dt):
    time_list = dt['cleaned_tpep_pickup_datetime_short'].unique()
    rez_time = []
    rez_count = []

    rez_id = list(range(1, 2501, 1))*len(time_list)

    for t in time_list:
        x = dt.loc[dt.cleaned_tpep_pickup_datetime_short == t, 'cleaned_pickup_longitude']
        y = dt.loc[dt.cleaned_tpep_pickup_datetime_short == t, 'cleaned_pickup_latitude']

        rez = counts_per_region(x, y).tolist()[0]
        rez_count = rez_count + rez
        rez_time = rez_time + [t] * 2500

    return (pd.DataFrame({'Time': rez_time
                             , 'Square_id': rez_id
                             , 'Counts': rez_count}))


def seach_the_element(l,e):
    try: list(l).index(e)
    except: pass
    else: return(list(l).index(e))

def target_column_clean_up(dt):

    dt_cleaned = dt.copy()
    #print(dt_cleaned.columns)

    #print(cleaned_columns_uq)
    for c in cleaned_columns_uq:
        #print(c)
        possible_col_names = cleaning_columns.loc[cleaning_columns.cleaned == c].column.tolist()
        #print(possible_col_names)
        pos = list(filter(None.__ne__, [seach_the_element(list(dt_cleaned.columns), e) for e in possible_col_names]))
        #print(pos)
        if len(pos) > 1:
            raise Exception("several possible columns found!")
        else:
            col_to_rename = dt_cleaned.columns[pos[0]]
            dt_cleaned[c] = dt_cleaned[col_to_rename]
            dt_cleaned.drop(columns = col_to_rename, inplace = True)

    return(dt_cleaned)

