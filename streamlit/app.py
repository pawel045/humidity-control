import altair as alt
from datetime import datetime, timedelta
import pandas as pd
from pymongo import MongoClient
import streamlit as st
import streamlit.components.v1 as components


def load_data():
    '''
    Get data from MongoDB
    '''
    # Fetch MongoDB credentials from secrets
    username = st.secrets["mongodb"]["username"]
    password = st.secrets["mongodb"]["password"]
    cluster_name = st.secrets["mongodb"]["cluster_name"]
    db_name = st.secrets["mongodb"]["database"]

    # Connection string
    connection_string = f'mongodb+srv://{username}:{password}@{cluster_name}.sm089.mongodb.net/?retryWrites=true&w=majority&appName={db_name}"'

    # Create a MongoDB client
    client = MongoClient(connection_string)
    db = client.HumidityControl
    hum_temp = db.hum_temp
    df = pd.DataFrame(hum_temp.find())

    # string to datetime object
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    return df

def filter_hours(df, hours):
    '''
    Get data greater than now - hours

    :param df: pd.DataFrame with timestamp column
    :param hours: decare past hours
    :return filtered_df:
    '''
    past_hours = datetime.now() - timedelta(hours=hours)
    filtered_df = df[df['timestamp'] > past_hours]
    return filtered_df

def set_widget_font_size(wgt_txt, wch_font_size = '12px'):
    '''
    from forum: 
    https://discuss.streamlit.io/t/how-to-change-font-size-of-streamlit-radio-widget-title/35945/3
    '''
    htmlstr = """<script>var elements = window.parent.document.querySelectorAll('*'), i;
                    for (i = 0; i < elements.length; ++i) { if (elements[i].innerText == |wgt_txt|) 
                        { elements[i].style.fontSize='""" + wch_font_size + """';} } </script>  """

    htmlstr = htmlstr.replace('|wgt_txt|', "'" + wgt_txt + "'")
    components.html(f"{htmlstr}", height=0, width=0)

# global var
LABEL_FONT_SIZE = 20
CHART_HEIGHT = 300
TEMP_CHART_RANGE = (15, 30)
HUM_CHART_RANGE = (30, 70)
HOURS_FOR_AVG_VAL = 3 # avg hum/temp -> how many hours in past
DIV_MARGIN = 6 # in px

# get data from mongodb
data = load_data()

# customize div margin
margin_txt = """
    <style>
    /* Apply margin to all div elements (Streamlit components are often wrapped in divs) */
    div {
        margin-bottom: """ + str(DIV_MARGIN) + 'px;' +  """
    }
    </style>
    """

st.markdown(margin_txt, unsafe_allow_html=True)

# title
st.title('Check temperature and humidity')

# show data as table
if st.checkbox('Show raw data'):
    st.subheader('Raw data')
    st.write(data.tail(10))

# value now
last_row = data.iloc[-1]
last_temp = str(round(last_row['temperature'], 1)) + ' °C'
last_hum = str(round(last_row['humidity'], 1)) + ' %'

cols = st.columns(2)
cols[0].metric('Current temperature', last_temp)
cols[1].metric('Current humidity', last_hum)

# avg value
last_3h_data = filter_hours(data, HOURS_FOR_AVG_VAL)
avg_temp = str(round(last_3h_data['temperature'].mean(), 1)) + ' °C'
avg_hum = str(round(last_3h_data['humidity'].mean(), 1)) + ' %'

cols = st.columns(2)
cols[0].metric('Average temperature', avg_temp)
cols[1].metric('Average humidity', avg_hum)

# slider for data range
hours = st.slider('**How many hours back?**', 1, 24, 12)
set_widget_font_size('How many hours back?', f'{LABEL_FONT_SIZE}px')

# line chart
filtered_data = filter_hours(data, hours)

chart_temp = alt.Chart(filtered_data).mark_line().encode(
    x='timestamp:T',
    y=alt.Y('temperature:Q', scale=alt.Scale(domain=TEMP_CHART_RANGE))
).properties(
    title="Temperature Over Time",
    width='container',  # Responsive width
    height=CHART_HEIGHT
)

chart_hum = alt.Chart(filtered_data).mark_line(color='orange').encode(
    x='timestamp:T',
    y=alt.Y('humidity:Q', scale=alt.Scale(domain=HUM_CHART_RANGE))
).properties(
    title="Humidity Over Time",
    width='container',  # Responsive width
    height=CHART_HEIGHT
)

# Using columns to place charts side by side for larger screens
cols = st.columns(2)

with cols[0]:
    st.altair_chart(chart_temp, use_container_width=True)  # Auto-resize chart to container width

with cols[1]:
    st.altair_chart(chart_hum, use_container_width=True) 
