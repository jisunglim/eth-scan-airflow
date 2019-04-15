#%%
import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output, State
import dash_table
import subprocess

import plotly.graph_objs as go
import pandas as pd
import json

#%%
mockup_eth = json.dumps([{
    'code': 'ETH',
    'name': 'Ethereum',
    'balance_or_id': '3242342123424234232'
}])

mockup_erc20 = json.dumps([{
    'code': 'ABC',
    'name': 'erc20_token1',
    'balance_or_id': '21312412412412412',
    'decimal': '18'
}, {
    'code': 'DEF',
    'name': 'erc20_token2',
    'balance_or_id': '22412',
    'decimal': '4'
}, {
    'code': 'GHI',
    'name': 'erc20_token3',
    'balance_or_id': '21312412132412',
    'decimal': '32'
}, {
    'code': 'JKL',
    'name': 'erc20_token4',
    'balance_or_id': '2131312512354424232412412',
    'decimal': '100'
}])

mockup_erc721 = json.dumps([{
    'code': 'ABCD',
    'name': 'erc721_token1',
    'balance_or_id': '2131 24124124 12412232131 241124124 1233412 21131 22124124 1122412'
}, {
    'code': 'EFGH',
    'name': 'erc721_token2',
    'balance_or_id': '22412'
}, {
    'code': 'IJKL',
    'name': 'erc721_token3',
    'balance_or_id': '2131 24121 32412'
}, {
    'code': 'MNOP',
    'name': 'erc721_token4',
    'balance_or_id': '2131 312512 35442 423241 2412'
}])

df = pd.DataFrame(columns=['code', 'name', 'balance_or_id'])

eth = json.loads(mockup_eth)[0]
eth['balance_or_id'] = str(int(eth['balance_or_id']) / (10**18))
eth = pd.DataFrame([eth])
df = df.append(eth, sort=False)

erc20 = []
for row in json.loads(mockup_erc20):
    row['balance_or_id'] = str(
        int(row['balance_or_id']) / (10**int(row['decimal'])))
    del row['decimal']
    print(row)
    erc20.append(row)
erc20 = pd.DataFrame(erc20)
df = df.append(erc20, sort=False)

erc721 = json.loads(mockup_erc721)
df = df.append(erc721, sort=False)

df
#%%
address = '0x1e411ae37f558c5e8e6567bb5ba0016e6bb6bd23'

def get_erc20(address):
    subprocess.check_output(
        'bq query --format "json" --use_legacy_sql=false '
        'SELECT '
        'tokens.symbol AS code, '
        'tokens.name AS name, '
        'erc20.balance AS balance_or_id, '
        'tokens.decimals as decimal '
        'FROM `gx-project-190412.gx_dataset.erc20` AS erc20 '
        'INNER JOIN `bigquery-public-data.ethereum_blockchain.tokens` AS tokens '
        'ON tokens.address = erc20.token_address '
        'WHERE erc20.address="{address}" '.format(address=address)
    )

get_erc20(address)


#%%
app = dash.Dash()

#%%
app.layout = html.Div([
    html.Div([
        dcc.Input(id='input-box', type='text', style=dict(
            width='60%',
            height='44px',
            border='2px solid rgb(147, 162, 173)',
            margin='0 8px',
            boxSizing='border-box'
        )),
        html.Button('Find', id='find-button', style=dict(
            width='10%',
            height='44px',
            border='2px solid rgb(147, 162, 173)',
            margin='0 8px',
            boxSizing='border-box'
        ))
    ], style=dict(
        position='relative',
        display='flex',
        justifyContent='center',
        alignItems='center',
        width='100%'
    )),
    html.Div(id='output-table-wrapper', style=dict(
        position='relative',
        width='100%',
        margin="100px 0",
        padding="0 15%",
        boxSizing="border-box"
    ))
], style=dict(
    position='relative',
    width='100%'
))

@app.callback(
    dash.dependencies.Output('output-table-wrapper', 'children'),
    [dash.dependencies.Input('find-button', 'n_clicks')],
    [dash.dependencies.State('input-box', 'value')])
def update_output(n_clicks, value):
    return html.Div([
        dash_table.DataTable(
            id='table',
            columns=[{"name": i, "id": i} for i in df.columns],
            data=df.to_dict("rows")
        )
    ], style=dict(
        textAlign="center"
    ))

if __name__ == '__main__':
    app.run_server(debug=True)