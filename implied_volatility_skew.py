import pandas as pd
import pandasql as ps
from pandasql import *
import plotly
import plotly.express as px
import plotly.graph_objects as go
import numpy as np
from nsepython import *
import glob
import os
import warnings

warnings.filterwarnings('ignore')

# For the widget

from __future__ import print_function
from ipywidgets import interact, interactive, fixed, interact_manual
import ipywidgets as widgets

folder_path = 'your/folder/path/here'
days_to_expiry = [2]
expiry = ['2023-01-12 15:30:00']

monthly_expiries = [
    "2010-11-25", "2010-12-30", "2011-01-27", "2011-02-24", "2011-03-31", "2011-04-28", 
    "2011-05-26", "2011-06-30", "2011-07-28", "2011-08-25", "2011-09-29", "2011-10-25", 
    "2011-11-24", "2011-12-29", "2012-01-25", "2012-02-23", "2012-03-29", "2012-04-26", 
    "2012-05-31", "2012-06-28", "2012-07-26", "2012-08-30", "2012-09-27", "2012-10-25", 
    "2012-11-29", "2012-12-27", "2013-01-31", "2013-02-28", "2013-03-28", "2013-04-25", 
    "2013-05-30", "2013-06-27", "2013-07-25", "2013-08-29", "2013-09-26", "2013-10-31", 
    "2013-11-28", "2013-12-26", "2014-01-30", "2014-02-26", "2014-02-27", "2014-03-27", 
    "2014-04-24", "2014-05-29", "2014-06-26", "2014-07-31", "2014-08-28", "2014-09-25", 
    "2014-10-30", "2014-11-27", "2014-12-24", "2015-01-29", "2015-02-26", "2015-03-26", 
    "2015-04-30", "2015-05-28", "2015-06-25", "2015-07-30", "2015-08-27", "2015-09-24", 
    "2015-10-29", "2015-11-26", "2015-12-31", "2016-01-28", "2016-02-25", "2016-03-31", 
    "2016-04-28", "2016-05-26", "2016-06-30", "2016-07-28", "2016-08-25", "2016-09-29", 
    "2016-10-27", "2016-11-24", "2016-12-29", "2017-01-25", "2017-02-23", "2017-03-30", 
    "2017-04-27", "2017-05-25", "2017-06-29", "2017-07-27", "2017-08-31", "2017-09-28", 
    "2017-10-26", "2017-11-30", "2017-12-28", "2018-01-25", "2018-02-22", "2018-03-28", 
    "2018-03-29", "2018-04-26", "2018-05-31", "2018-06-28", "2018-07-26", "2018-08-30", 
    "2018-09-27", "2018-10-25", "2018-11-29", "2018-12-27", "2019-01-31", "2019-02-28", 
    "2019-03-28", "2019-04-25", "2019-05-30", "2019-06-27", "2019-07-25", "2019-08-29", 
    "2019-09-26", "2019-10-31", "2019-11-28", "2019-12-26", "2020-01-30", "2020-02-27", 
    "2020-03-26", "2020-04-30", "2020-05-28", "2020-06-25", "2020-07-30", "2020-08-27", 
    "2020-09-24", "2020-10-29", "2020-11-26", "2020-12-31", "2021-01-28", "2021-02-25", 
    "2021-03-25", "2021-04-29", "2021-05-27", "2021-06-24", "2021-07-29", "2021-08-26", 
    "2021-09-30", "2021-10-28", "2021-11-25", "2021-12-30", "2022-01-27", "2022-02-24", 
    "2022-03-31", "2022-04-28", "2022-05-26", "2022-06-30", "2022-07-28", "2022-08-25",
    "2022-09-29", "2022-10-27", "2022-11-24", "2022-12-29"
]

underlying = 'NIFTY'
x_axis = 'strike' # Either 'moneyness' or 'strike'
print(days_to_expiry, expiry)
print(pd.Timestamp.now().round('min'))
put_iv_ratio(date1, date2, expiry, from_expiry, days_to_expiry, underlying, x_axis)
call_iv_ratio(date1, date2, expiry, from_expiry, days_to_expiry, underlying, x_axis)


def put_iv_ratio(date1, date2, expiry, from_expiry, days_to_expiry, underlying, x_axis):

    # Put

    path = folder_path + '/sample_input_1.csv'
    df = pd.read_csv(latest_file)
    
    path = folder_path + '/sample_input_2.csv'
    close_options = pd.read_csv(path)
        
    for days_to_expiry, expiry in zip(days_to_expiry, expiry):
        
        put = df[
            (df['expiry'] == expiry)
            & (df['put_price'] != 0)
            & (df['call_price'] != 0)
            & (df['put_open_interest'] != 0)
            & (df['call_open_interest'] != 0)
        ]
        
        put['straddle_price'] = put['put_price'] + put['call_price']
        put['s_minus_k_abs'] = (put['forward'] - put['strike']).abs()
        put['timestamp'] = pd.to_datetime(put['timestamp'])
        put = put.sort_values(by=['timestamp'], ascending=[True])
        temp = put.groupby(['timestamp'], sort=False)['s_minus_k_abs'].min()
        temp = pd.DataFrame(temp)
        put = put[['timestamp', 's_minus_k_abs', 'strike', 'expiry', 'moneyness', 'implied_volatility', 'close_option', 'straddle_price', 'forward', 'theta', 'gamma']]
        put = temp.merge(put, on=['timestamp', 's_minus_k_abs'], how='inner')
        atm_put_price = put['straddle_price'].max()
        atm_forward = put['forward'].max()
        atm_iv = put['implied_volatility'].max()
        print(atm_forward)
        put = df[
            (df['expiry'] == expiry)
            & (df['close_option'] != 0)
        ]
        put['moneyness'] = np.log(atm_forward / put['strike'])
        put = put[put['moneyness'] >= 0]

        put['iv_ratio_otm_atm'] = 1.00 * put['implied_volatility'] / atm_iv

        put_historical = pd.DataFrame(index=range(len(put['moneyness'])))
        put_historical['strike'] = ''
        put_historical['moneyness'] = ''
        put_historical['average_iv_ratio'] = ''
        put_historical['stdev_iv_ratio'] = ''
        put_historical['min_iv_ratio'] = ''
        put_historical['max_iv_ratio'] = ''
        put_historical['10pc_iv_ratio'] = ''
        put_historical['25pc_iv_ratio'] = ''
        put_historical['50pc_iv_ratio'] = ''
        put_historical['75pc_iv_ratio'] = ''
        put_historical['90pc_iv_ratio'] = ''

        for moneyness, strike, i in zip(put['moneyness'], put['strike'], range(0, len(put['moneyness']))):
            def iv_ratio(moneyness, from_expiry, days_to_expiry, underlying):

                # Put
                if days_to_expiry > 35:
                    put_atm = close_options[
                        (close_options['days_to_expiry'].between( days_to_expiry, days_to_expiry))
                        & (close_options['put_close'] != 0)
                        & (close_options['call_close'] != 0)
                        & (close_options['expiry'].isin(monthly_expiries))
                    ][[
                        'date', 'expiry', 'strike', 'otm_option_type', 'put_close', 
                        'call_close', 'close_option', 'forward_close', 'days_to_expiry', 'implied_volatility', 
                        'delta', 'gamma', 'theta', 'vega', 'vanna', 'charm', 'volga'
                    ]]
                else:
                    put_atm = close_options[
                        (close_options['days_to_expiry'].between( days_to_expiry,  days_to_expiry))
                        & (close_options['put_close'] != 0)
                        & (close_options['call_close'] != 0)
                    ][[
                        'date', 'expiry', 'strike', 'otm_option_type', 'put_close', 
                        'call_close', 'close_option', 'forward_close', 'days_to_expiry', 'implied_volatility', 
                        'delta', 'gamma', 'theta', 'vega', 'vanna', 'charm', 'volga'
                    ]]
                put_atm['s_minus_k_abs'] = (put_atm['forward_close'] - put_atm['strike']).abs()
                put_atm['straddle_price'] = put_atm['put_close'] + put_atm['call_close']
                put_atm['close_option'] = put_atm['straddle_price']
                put_atm = put_atm.rename(columns={
                    'date': 'date_start_atm', 'expiry': 'expiry_start_atm', 
                    'strike': 'strike_start_atm', 'close_option': 'close_option_start_atm', 
                    'forward_close': 'forward_close_start_atm', 
                    'days_to_expiry': 'days_to_expiry_start_atm', 
                    'implied_volatility': 'iv_atm', 'delta': 'delta_atm', 'gamma': 'gamma_atm', 
                    'theta': 'theta_atm', 'vega': 'vega_atm', 'vanna': 'vanna_atm', 
                    'charm': 'charm_atm', 'volga': 'volga_atm'})
                temp = put_atm.groupby(['expiry_start_atm'], sort=False)['days_to_expiry_start_atm'].min()
                temp = pd.DataFrame(temp)

                put_atm = temp.merge(put_atm, on=['expiry_start_atm', 'days_to_expiry_start_atm'], how='inner')

                temp = put_atm.groupby(['expiry_start_atm'], sort=False)['s_minus_k_abs'].min()
                temp = pd.DataFrame(temp)

                put_atm = temp.merge(put_atm, on=['expiry_start_atm', 's_minus_k_abs'], how='inner')
                if days_to_expiry > 35:
                    put_otm = close_options[
                        (close_options['days_to_expiry'].between( days_to_expiry,  days_to_expiry))
                        & (close_options['moneyness_close'].between(0.95 * moneyness, 1.05 * moneyness))
                        & (close_options['put_close'] != 0)
                        & (close_options['call_close'] != 0)
                        & (close_options['expiry'].isin(monthly_expiries))
                    ][[
                        'date', 'expiry', 'strike', 'otm_option_type', 'put_close', 
                        'call_close', 'close_option', 'forward_close', 'days_to_expiry', 'implied_volatility', 
                        'delta', 'gamma', 'theta', 'vega', 'vanna', 'charm', 'volga'
                    ]]
                else:
                    put_otm = close_options[
                        (close_options['days_to_expiry'].between( days_to_expiry,  days_to_expiry))
                        & (close_options['moneyness_close'].between(0.95 * moneyness, 1.05 * moneyness))
                        & (close_options['put_close'] != 0)
                        & (close_options['call_close'] != 0)
                    ][[
                        'date', 'expiry', 'strike', 'otm_option_type', 'put_close', 
                        'call_close', 'close_option', 'forward_close', 'days_to_expiry', 'implied_volatility', 
                        'delta', 'gamma', 'theta', 'vega', 'vanna', 'charm', 'volga'
                    ]]

                put_atm_to_merge = put_atm[[
                    'expiry_start_atm', 
                    'strike_start_atm', 
                    'close_option_start_atm', 
                    'forward_close_start_atm',
                    'iv_atm', 
                    'vega_atm']]

                put_otm = put_otm.rename(columns={
                    'date': 'date_start_otm', 'expiry': 'expiry_start_otm', 'strike': 'strike_start_otm', 
                    'close_option': 'close_option_start_otm', 'forward_close': 'forward_close_start_otm', 
                    'days_to_expiry': 'days_to_expiry_start_otm', 'implied_volatility': 'iv_otm', 'delta': 'delta_otm',
                    'gamma': 'gamma_otm', 'theta': 'theta_otm', 'vega': 'vega_otm', 'vanna': 'vanna_otm',
                    'charm': 'charm_otm', 'volga': 'volga_otm'})

                put_atm_to_merge = put_atm_to_merge.rename(columns={
                    'expiry_start_atm': 'expiry_start_otm'})

                temp = put_otm.groupby(['expiry_start_otm'], sort=False)['days_to_expiry_start_otm'].min()
                temp = pd.DataFrame(temp)

                put_otm = temp.merge(put_otm, on=['expiry_start_otm', 'days_to_expiry_start_otm'], how='inner')

                temp = put_otm.groupby(['expiry_start_otm'], sort=False)['strike_start_otm'].max()
                temp = pd.DataFrame(temp)

                put_otm = temp.merge(put_otm, on=['expiry_start_otm', 'strike_start_otm'], how='inner')

                put_otm['expiry_start_otm'] = pd.to_datetime(put_otm['expiry_start_otm'])
                put_atm_to_merge['expiry_start_otm'] = pd.to_datetime(put_atm_to_merge['expiry_start_otm'])

                put_otm = put_otm.merge(put_atm_to_merge, on=['expiry_start_otm'], how='inner')

                put_otm['iv_ratio_otm_atm'] = 1.00 * (
                    put_otm['iv_otm'] / put_otm['iv_atm']
                )
                
                put_otm['avg_iv_ratio'] = put_otm['iv_ratio_otm_atm'].mean()
                put_otm['std_iv_ratio'] = put_otm['iv_ratio_otm_atm'].std()
                put_otm['min_iv_ratio'] = put_otm['iv_ratio_otm_atm'].min()
                put_otm['10pc_iv_ratio'] = put_otm['iv_ratio_otm_atm'].quantile(0.1)
                put_otm['25pc_iv_ratio'] = put_otm['iv_ratio_otm_atm'].quantile(0.25)
                put_otm['50pc_iv_ratio'] = put_otm['iv_ratio_otm_atm'].quantile(0.5)
                put_otm['75pc_iv_ratio'] = put_otm['iv_ratio_otm_atm'].quantile(0.75)
                put_otm['90pc_iv_ratio'] = put_otm['iv_ratio_otm_atm'].quantile(0.9)
                put_otm['max_iv_ratio'] = put_otm['iv_ratio_otm_atm'].max()

                return strike, moneyness, put_otm['avg_iv_ratio'].max(), put_otm['std_iv_ratio'].max(), put_otm['min_iv_ratio'].max(), put_otm['max_iv_ratio'].max(), put_otm['10pc_iv_ratio'].max(), put_otm['25pc_iv_ratio'].max(), put_otm['50pc_iv_ratio'].max(), put_otm['75pc_iv_ratio'].max(), put_otm['90pc_iv_ratio'].max()


            strike, moneyness, average_iv_ratio, stdev_iv_ratio, min_iv_ratio, max_iv_ratio, _10pc_iv_ratio, _25pc_iv_ratio, _50pc_iv_ratio, _75pc_iv_ratio, _90pc_iv_ratio = iv_ratio(moneyness, from_expiry, days_to_expiry, underlying)
            put_historical['strike'][i] = strike
            put_historical['moneyness'][i] = moneyness
            put_historical['average_iv_ratio'][i] = average_iv_ratio
            put_historical['stdev_iv_ratio'][i] = stdev_iv_ratio
            put_historical['min_iv_ratio'][i] = min_iv_ratio
            put_historical['max_iv_ratio'][i] = max_iv_ratio
            put_historical['10pc_iv_ratio'][i] = _10pc_iv_ratio
            put_historical['25pc_iv_ratio'][i] = _25pc_iv_ratio
            put_historical['50pc_iv_ratio'][i] = _50pc_iv_ratio
            put_historical['75pc_iv_ratio'][i] = _75pc_iv_ratio
            put_historical['90pc_iv_ratio'][i] = _90pc_iv_ratio

        put = put.merge(put_historical, on=['strike', 'moneyness'], how='inner')
        put['iv_ratio_diff'] = 1.00 * (put['iv_ratio_otm_atm'] - put['average_iv_ratio']) / put['stdev_iv_ratio']
        put[[
            'iv_ratio_otm_atm', 'iv_ratio_diff', 'average_iv_ratio', 
            'stdev_iv_ratio', 'min_iv_ratio', 'max_iv_ratio', 
            '10pc_iv_ratio', '25pc_iv_ratio', '50pc_iv_ratio', 
            '75pc_iv_ratio', '90pc_iv_ratio'
        ]] = put[[
            'iv_ratio_otm_atm', 'iv_ratio_diff', 'average_iv_ratio', 
            'stdev_iv_ratio', 'min_iv_ratio', 'max_iv_ratio', 
            '10pc_iv_ratio', '25pc_iv_ratio', '50pc_iv_ratio', 
            '75pc_iv_ratio', '90pc_iv_ratio'
        ]].multiply(atm_iv, axis="index")
        put_df = put[['strike', 'moneyness', 'expiry', 'close_option', 'moneyness', 'iv_ratio_otm_atm', 'iv_ratio_diff', 'average_iv_ratio', 'stdev_iv_ratio', 'min_iv_ratio', 'max_iv_ratio', '10pc_iv_ratio', '25pc_iv_ratio', '50pc_iv_ratio', '75pc_iv_ratio', '90pc_iv_ratio']]
        put_df = pd.melt(put, id_vars=[x_axis], value_vars=['iv_ratio_otm_atm', 'min_iv_ratio', '10pc_iv_ratio', '25pc_iv_ratio', '50pc_iv_ratio', '75pc_iv_ratio', '90pc_iv_ratio', 'max_iv_ratio'])
        fig = px.scatter(put_df, x=x_axis, y='value', color='variable', log_x=True, log_y=True, title="Put - " + underlying + " - " + expiry)
        fig.show()
        print(put[['strike', 'moneyness', 'expiry', 'iv_ratio_otm_atm', '25pc_iv_ratio', '10pc_iv_ratio', 'min_iv_ratio']].to_string())
        
def call_iv_ratio(date1, date2, expiry, from_expiry, days_to_expiry, underlying, x_axis):
    # call
    path = folder_path + '/sample_input_1.csv'
    df = pd.read_csv(latest_file)
    path = folder_path + '/sample_input_2.csv'
    close_options = pd.read_csv(path)
        
    for days_to_expiry, expiry in zip(days_to_expiry, expiry):
        
        call = df[
            (df['expiry'] == expiry)
            & (df['put_price'] != 0)
            & (df['call_price'] != 0)
            & (df['put_open_interest'] != 0)
            & (df['call_open_interest'] != 0)
        ]
        call['strike'] = pd.to_numeric(call['strike'], errors='coerce')
        call['moneyness'] = pd.to_numeric(call['moneyness'], errors='coerce')
        call['straddle_price'] = call['put_price'] + call['call_price']
        call['s_minus_k_abs'] = (call['forward'] - call['strike']).abs()
        call['timestamp'] = pd.to_datetime(call['timestamp'])
        call = call.sort_values(by=['timestamp'], ascending=[True])
        temp = call.groupby(['timestamp'], sort=False)['s_minus_k_abs'].min()
        temp = pd.DataFrame(temp)
        call = call[[
            'timestamp', 's_minus_k_abs', 'strike', 'expiry', 'moneyness', 
            'implied_volatility', 'close_option', 'straddle_price', 
            'forward', 'theta', 'gamma'
        ]]
        call = temp.merge(call, on=['timestamp', 's_minus_k_abs'], how='inner')
        atm_call_price = call['straddle_price'].max()
        atm_forward = call['forward'].max()
        atm_iv = call['implied_volatility'].max()
        print(atm_forward)
        call = df[
            (df['expiry'] == expiry)
            & (df['close_option'] != 0)
        ]
        call['moneyness'] = np.log(atm_forward / call['strike'])
        call = call[call['moneyness'] < 0]

        call['iv_ratio_otm_atm'] = 1.00 * call['implied_volatility'] / atm_iv

        call_historical = pd.DataFrame(index=range(len(call['moneyness'])))
        call_historical['strike'] = ''
        call_historical['moneyness'] = ''
        call_historical['average_iv_ratio'] = ''
        call_historical['stdev_iv_ratio'] = ''
        call_historical['min_iv_ratio'] = ''
        call_historical['max_iv_ratio'] = ''
        call_historical['10pc_iv_ratio'] = ''
        call_historical['25pc_iv_ratio'] = ''
        call_historical['50pc_iv_ratio'] = ''
        call_historical['75pc_iv_ratio'] = ''
        call_historical['90pc_iv_ratio'] = ''

        for moneyness, strike, i in zip(call['moneyness'], call['strike'], range(0, len(call['moneyness']))):
            def iv_ratio(moneyness, from_expiry, days_to_expiry, underlying):

                # call
                if days_to_expiry > 35:
                    call_atm = close_options[
                        (close_options['days_to_expiry'].between( days_to_expiry,  days_to_expiry))
                        & (close_options['put_close'] != 0)
                        & (close_options['call_close'] != 0)
                        & (close_options['expiry'].isin(monthly_expiries))
                    ][[
                        'date', 'expiry', 'strike', 'otm_option_type', 'close_option', 'put_close', 'call_close', 
                        'forward_close', 'days_to_expiry', 'implied_volatility', 
                        'delta', 'gamma', 'theta', 'vega', 'vanna', 'charm', 'volga']]
                else:
                    call_atm = close_options[
                        (close_options['days_to_expiry'].between( days_to_expiry,  days_to_expiry))
                        & (close_options['put_close'] != 0)
                        & (close_options['call_close'] != 0)
                    ][[
                        'date', 'expiry', 'strike', 'otm_option_type', 'close_option', 'put_close', 'call_close', 
                        'forward_close', 'days_to_expiry', 'implied_volatility', 
                        'delta', 'gamma', 'theta', 'vega', 'vanna', 'charm', 'volga']]
                call_atm['s_minus_k_abs'] = (call_atm['forward_close'] - call_atm['strike']).abs()
                call_atm['straddle_price'] = call_atm['put_close'] + call_atm['call_close']
                call_atm['close_option'] = call_atm['straddle_price']
                call_atm = call_atm.rename(columns={
                    'date': 'date_start_atm', 'expiry': 'expiry_start_atm', 'strike': 'strike_start_atm', 
                    'put_close': 'put_close_atm', 'call_close': 'call_close_atm', 
                    'close_option': 'close_option_start_atm', 
                    'forward_close': 'forward_close_start_atm', 
                    'days_to_expiry': 'days_to_expiry_start_atm', 
                    'implied_volatility': 'iv_atm', 'delta': 'delta_atm', 
                    'gamma': 'gamma_atm', 
                    'theta': 'theta_atm', 'vega': 'vega_atm', 'vanna': 'vanna_atm', 
                    'charm': 'charm_atm', 'volga': 'volga_atm'})
                temp = call_atm.groupby(['expiry_start_atm'], sort=False)['days_to_expiry_start_atm'].min()
                temp = pd.DataFrame(temp)

                call_atm = temp.merge(call_atm, on=['expiry_start_atm', 'days_to_expiry_start_atm'], how='inner')

                temp = call_atm.groupby(['expiry_start_atm'], sort=False)['s_minus_k_abs'].min()
                temp = pd.DataFrame(temp)
                call_atm = temp.merge(call_atm, on=['expiry_start_atm', 's_minus_k_abs'], how='inner')
                if days_to_expiry > 35:
                    call_otm = close_options[
                        (close_options['days_to_expiry'].between( days_to_expiry,  days_to_expiry))
                        & (close_options['moneyness_close'].between(1.05 * moneyness, 0.95 * moneyness))
                        & (close_options['put_close'] != 0)
                        & (close_options['call_close'] != 0)
                        & (close_options['expiry'].isin(monthly_expiries))
                    ][[
                        'date', 'expiry', 'strike', 'close_option', 'forward_close',
                        'days_to_expiry', 'implied_volatility', 'delta', 'gamma',
                        'theta', 'vega', 'vanna', 'charm', 'volga'
                    ]]
                else:
                    call_otm = close_options[
                        (close_options['days_to_expiry'].between( days_to_expiry,  days_to_expiry))
                        & (close_options['moneyness_close'].between(1.05 * moneyness, 0.95 * moneyness))
                        & (close_options['put_close'] != 0)
                        & (close_options['call_close'] != 0)
                    ][[
                        'date', 'expiry', 'strike', 'close_option', 'forward_close', 
                        'days_to_expiry', 'implied_volatility', 'delta', 'gamma',
                        'theta', 'vega', 'vanna', 'charm', 'volga'
                    ]]

                call_atm_to_merge = call_atm[[
                    'expiry_start_atm', 
                    'strike_start_atm', 
                    'close_option_start_atm', 
                    'forward_close_start_atm',
                    'iv_atm', 
                    'vega_atm']]

                call_otm = call_otm.rename(columns={
                    'date': 'date_start_otm', 'expiry': 'expiry_start_otm', 'strike': 'strike_start_otm', 
                    'close_option': 'close_option_start_otm', 'forward_close': 'forward_close_start_otm', 
                    'days_to_expiry': 'days_to_expiry_start_otm', 'implied_volatility': 'iv_otm', 'delta': 'delta_otm',
                    'gamma': 'gamma_otm', 'theta': 'theta_otm', 'vega': 'vega_otm', 'vanna': 'vanna_otm',
                    'charm': 'charm_otm', 'volga': 'volga_otm'})

                call_atm_to_merge = call_atm_to_merge.rename(columns={
                    'expiry_start_atm': 'expiry_start_otm'})

                temp = call_otm.groupby(['expiry_start_otm'], sort=False)['days_to_expiry_start_otm'].min()
                temp = pd.DataFrame(temp)

                call_otm = temp.merge(call_otm, on=['expiry_start_otm', 'days_to_expiry_start_otm'], how='inner')

                temp = call_otm.groupby(['expiry_start_otm'], sort=False)['strike_start_otm'].min()
                temp = pd.DataFrame(temp)

                call_otm = temp.merge(call_otm, on=['expiry_start_otm', 'strike_start_otm'], how='inner')

                call_otm['expiry_start_otm'] = pd.to_datetime(call_otm['expiry_start_otm'])
                call_atm_to_merge['expiry_start_otm'] = pd.to_datetime(call_atm_to_merge['expiry_start_otm'])

                call_otm = call_otm.merge(call_atm_to_merge, on=['expiry_start_otm'], how='inner')

                call_otm['iv_ratio_otm_atm'] = 1.00 * (
                    call_otm['iv_otm'] / call_otm['iv_atm']
                )
                
                call_otm['avg_iv_ratio'] = call_otm['iv_ratio_otm_atm'].mean()
                call_otm['std_iv_ratio'] = call_otm['iv_ratio_otm_atm'].std()
                call_otm['min_iv_ratio'] = call_otm['iv_ratio_otm_atm'].min()
                call_otm['10pc_iv_ratio'] = call_otm['iv_ratio_otm_atm'].quantile(0.1)
                call_otm['25pc_iv_ratio'] = call_otm['iv_ratio_otm_atm'].quantile(0.25)
                call_otm['50pc_iv_ratio'] = call_otm['iv_ratio_otm_atm'].quantile(0.5)
                call_otm['75pc_iv_ratio'] = call_otm['iv_ratio_otm_atm'].quantile(0.75)
                call_otm['90pc_iv_ratio'] = call_otm['iv_ratio_otm_atm'].quantile(0.9)
                call_otm['max_iv_ratio'] = call_otm['iv_ratio_otm_atm'].max()

                return strike, moneyness, call_otm['avg_iv_ratio'].max(), call_otm['std_iv_ratio'].max(), call_otm['min_iv_ratio'].max(), call_otm['max_iv_ratio'].max(), call_otm['10pc_iv_ratio'].max(), call_otm['25pc_iv_ratio'].max(), call_otm['50pc_iv_ratio'].max(), call_otm['75pc_iv_ratio'].max(), call_otm['90pc_iv_ratio'].max()


            strike, moneyness, average_iv_ratio, stdev_iv_ratio, min_iv_ratio, max_iv_ratio, _10pc_iv_ratio, _25pc_iv_ratio, _50pc_iv_ratio, _75pc_iv_ratio, _90pc_iv_ratio = iv_ratio(moneyness, from_expiry, days_to_expiry, underlying)
            call_historical['strike'][i] = strike
            call_historical['moneyness'][i] = moneyness
            call_historical['average_iv_ratio'][i] = average_iv_ratio
            call_historical['stdev_iv_ratio'][i] = stdev_iv_ratio
            call_historical['min_iv_ratio'][i] = min_iv_ratio
            call_historical['max_iv_ratio'][i] = max_iv_ratio
            call_historical['10pc_iv_ratio'][i] = _10pc_iv_ratio
            call_historical['25pc_iv_ratio'][i] = _25pc_iv_ratio
            call_historical['50pc_iv_ratio'][i] = _50pc_iv_ratio
            call_historical['75pc_iv_ratio'][i] = _75pc_iv_ratio
            call_historical['90pc_iv_ratio'][i] = _90pc_iv_ratio

        call = call.merge(call_historical, on=['strike', 'moneyness'], how='inner')
        call['iv_ratio_diff'] = 1.00 * (call['iv_ratio_otm_atm'] - call['average_iv_ratio']) / call['stdev_iv_ratio']
        call[[
            'iv_ratio_otm_atm', 'iv_ratio_diff', 'average_iv_ratio', 
            'stdev_iv_ratio', 'min_iv_ratio', 'max_iv_ratio', 
            '10pc_iv_ratio', '25pc_iv_ratio', '50pc_iv_ratio', 
            '75pc_iv_ratio', '90pc_iv_ratio'
        ]] = call[[
            'iv_ratio_otm_atm', 'iv_ratio_diff', 'average_iv_ratio', 
            'stdev_iv_ratio', 'min_iv_ratio', 'max_iv_ratio', 
            '10pc_iv_ratio', '25pc_iv_ratio', '50pc_iv_ratio', 
            '75pc_iv_ratio', '90pc_iv_ratio'
        ]].multiply(atm_iv, axis="index")
        call_df = call[['strike', 'moneyness', 'expiry', 'close_option', 'moneyness', 'iv_ratio_otm_atm', 'iv_ratio_diff', 'average_iv_ratio', 'stdev_iv_ratio', 'min_iv_ratio', 'max_iv_ratio', '10pc_iv_ratio', '25pc_iv_ratio', '50pc_iv_ratio', '75pc_iv_ratio', '90pc_iv_ratio']]
        call_df = pd.melt(call, id_vars=[x_axis], value_vars=['iv_ratio_otm_atm', 'min_iv_ratio', '10pc_iv_ratio', '25pc_iv_ratio', '50pc_iv_ratio', '75pc_iv_ratio', '90pc_iv_ratio', 'max_iv_ratio'])
        fig = px.scatter(call_df, x=x_axis, y='value', color='variable', log_x=True, log_y=True, title="call - " + underlying + " - " + expiry)
        fig.show()
        print(call[['strike', 'moneyness', 'expiry', 'close_option', 'iv_ratio_otm_atm', '25pc_iv_ratio', '10pc_iv_ratio', 'min_iv_ratio']].to_string())        