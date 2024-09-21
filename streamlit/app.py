import altair as alt
from datetime import datetime, timedelta
import pandas as pd
from pymongo import MongoClient
import streamlit as st
import streamlit.components.v1 as components

@st.cache_data
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


LABEL_FONT_SIZE = 20
data = load_data()

st.markdown("""
    <style>
    /* Apply margin to all div elements (Streamlit components are often wrapped in divs) */
    div {
        margin-bottom: 6px;
    }
    </style>
    """, unsafe_allow_html=True)

# title
st.title('Check temperature and humidity')

# show data as table
if st.checkbox('Show raw data'):
    st.subheader('Raw data')
    st.write(data)

# avg value
last_3h_data = filter_hours(data, 3)
avg_temp = str(round(last_3h_data['temperature'].mean(), 2)) + ' °C'
avg_hum = str(round(last_3h_data['humidity'].mean(), 2)) + ' %'

cols = st.columns(2)
cols[0].metric('Average temperature', avg_temp)
cols[1].metric('Average humidity', avg_hum)


# radio btn temp/hum
# data_option = st.radio(label='**What do you want to see?**', options=['temperature', 'humidity'])
# set_widget_font_size('What do you want to see?', f'{LABEL_FONT_SIZE}px')

# slider for data range
hours = st.slider('**How many hours back?**', 1, 24, 12)
set_widget_font_size('How many hours back?', f'{LABEL_FONT_SIZE}px')

# line chart
filtered_data = filter_hours(data, hours)
# col1, col2 = st.columns(2)

# chart_temp = alt.Chart(filtered_data).mark_line().encode(
#     x='timestamp:T',  # X-axis as timestamp
#     y=alt.Y('temperature:Q', scale=alt.Scale(domain=[16, 28]))  # Y-axis with range 15-30
# ).properties(
#     title=alt.TitleParams(text="Temperature Over Time", fontSize=LABEL_FONT_SIZE),
#     width='container',  # Responsive width
#     # height=300
# ).interactive()

# chart_hum = alt.Chart(filtered_data).mark_line().encode(
#     x='timestamp:T',  # X-axis as timestamp
#     y=alt.Y('humidity:Q', scale=alt.Scale(domain=[30, 90]), axis=alt.Axis(orient='right'))  # Y-axis with range 15-30
# ).properties(
#     title=alt.TitleParams(text="Humidity Over Time", fontSize=LABEL_FONT_SIZE),
#     width='container',  # Responsive width
#     # height=300
# ).interactive()

# col1.altair_chart(chart_temp)
# col2.altair_chart(chart_hum)

chart1 = alt.Chart(filtered_data).mark_line().encode(
    x='timestamp:T',
    y=alt.Y('temperature:Q', scale=alt.Scale(domain=[15, 30]))
).properties(
    title="Temperature Over Time",
    width='container',  # Responsive width
    height=300
)

chart2 = alt.Chart(filtered_data).mark_line(color='orange').encode(
    x='timestamp:T',
    y=alt.Y('humidity:Q', scale=alt.Scale(domain=[30, 90]))
).properties(
    title="Humidity Over Time",
    width='container',  # Responsive width
    height=300
)

# Using columns to place charts side by side for larger screens
cols = st.columns(2)

with cols[0]:
    st.altair_chart(chart1, use_container_width=True)  # Auto-resize chart to container width

with cols[1]:
    st.altair_chart(chart2, use_container_width=True)  # Auto-resize chart to container width
