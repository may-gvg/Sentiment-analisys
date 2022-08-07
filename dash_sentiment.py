import dash
import pandas as pd
import requests
from dash import dash_table, dcc
from dash import html
import matplotlib.pyplot as plt
import plotly.graph_objects as go

url = '######'

headers = {'accept': 'application/json', 'content-type': 'application/json'}
jsonobj = {'appId': ########, 'password': '#######'}
resp = requests.post(url=url, headers=headers, json=jsonobj)
data = resp.json()
token = data.get("access_token")

url3 = '######'

headers['Authorization'] = f"Bearer {token}"

resp = requests.get(url=url3, headers=headers)
data = resp.json()

df3 = pd.json_normalize(data)
df3 = df3[['logDayFrom', 'user.firstName', 'user.lastName', 'comment']]
df3.dropna(subset=["comment"], inplace=True)
df3 = df3[df3.comment != '']

from textblob import TextBlob

for t in df3['comment']:

    testimonial = TextBlob(str(t))
    # print(str(t) + " : " + str(testimonial.sentiment))

df3["subjectivity"] = df3.apply(lambda x: TextBlob(x['comment']).sentiment.subjectivity, axis=1)
df3["polarity"] = df3.apply(lambda x: TextBlob(x['comment']).sentiment.polarity, axis=1)
df3["polaryzacja"] = df3.apply(lambda x: TextBlob(x['comment']).sentiment.polarity, axis=1)

def sentiment(x):
    if x < 0:
        return 'neg'
    elif x == 0:
        return 'neu'
    else:
        return 'pos'

df3['polaryzacja'] = df3['polaryzacja'].map(lambda x: sentiment(x))


fig = go.Figure()
fig.add_trace(go.Histogram(histfunc="count",  x=df3['polaryzacja']))

plt.bar(df3.polarity.value_counts().index, df3.polarity.value_counts())


app = dash.Dash()

app.layout = html.Div(children=[
    html.H1('Dashboard', style={'margin': 'auto'}),
    html.Div('Tu pojawi się content', style={'color': 'red'})

])

app.layout = dash_table.DataTable(
    data=df3.to_dict('records'),
    columns=[{'id': c, 'name': c} for c in df3.columns],
    style_cell={'fontSize': 15, 'font-family': 'Garamond'},

)

html.Div(children='''
           multi line atm_transactions
   '''),

dcc.Graph(
    id='graph10',
    figure=fig
),

if __name__ == '__main__':
    app.run_server()
