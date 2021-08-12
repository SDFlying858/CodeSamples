
# coding: utf-8

# In[689]:


import psycopg2
import psycopg2.extras
import folium
from folium import FeatureGroup, Marker
import altair as alt
import pandas as pd
import numpy as np
import json
import vincent


# In[1]:


conn = None
try:
    conn = psycopg2.connect()
    cur = conn.cursor()

except (Exception, psycopg2.DatabaseError) as error:
    print(error)


# In[691]:


try:
    cur = conn.cursor()
    cur.execute("SELECT id, node_name, latitude, longitude FROM nodes_caiso WHERE node_type='GEN' ORDER BY id ASC")
    row = cur.fetchone()
except (Exception, psycopg2.DatabaseError) as error:
    print(error)


# In[692]:


feature_group_node = FeatureGroup(name='Nodes')

while row is not None:
    print(row)
    cur2 = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur2.execute("SELECT extract(hour from timestamp) AS time, real_time_price AS price, day_ahead_energy AS day_ahead FROM nodes_pricing WHERE node_id=%s AND timestamp > CURRENT_DATE - interval '2 days' ORDER BY timestamp ASC LIMIT 24", [row[0]])
    df = pd.DataFrame(cur2.fetchall(), columns=['time', 'price', 'day_ahead'])
    df.set_index('time', drop=False)

    #print(df)
    
    line_chart = vincent.Line(df, iter_idx='time', width=600, height=300)
    line_json = line_chart.to_json()

    

    popup = folium.Popup(max_width=650)
    folium.Vega(line_json, height=350, width=650).add_to(popup)
    #folium.Marker([row[2], row[3]], popup=popup).add_to(folium_map)
    feature_group_node.add_child(Marker([row[2], row[3]], popup=popup, icon=folium.Icon(color='blue', icon='flash', prefix='glyphicon')))
    folium_map.add_child(feature_group_node);
    row = cur.fetchone()


# In[693]:


sql = """SELECT names AS name, mw_size AS size, longitude, latitude FROM power_plant WHERE fuel_type = 'Solar' AND status = 'Operating';"""
solar = pd.read_sql(sql,conn)


# In[694]:


solar.head()


# In[695]:


feature_group = FeatureGroup(name='Solar Power Plants')


# In[696]:


#for s in solar.iterrows():
#    print(s.name)


# In[697]:


#for s in solar.iterrows():
#    print(s.name)

s_tuples = [tuple(x) for x in solar.values]
for s in s_tuples:
    label = '<b>'+s[0] + '</b><br>MW: ' + str(s[1])
    feature_group.add_child(Marker([s[3], s[2]], popup=label, icon=folium.Icon(color='yellow', icon='certificate', prefix='glyphicon')))


# In[698]:


folium_map.add_child(feature_group);
#folium_map.add_children(folium.map.LayerControl());


# In[699]:


#tuples


# In[700]:


windsql = """SELECT names AS name, mw_size AS size, longitude, latitude FROM power_plant WHERE fuel_type = 'Wind' AND status = 'Operating';"""
wind = pd.read_sql(windsql,conn)
wind.head()


# In[701]:


feature_group_wind = FeatureGroup(name='Wind Power Plants')


# In[702]:


w_tuples = [tuple(w) for w in wind.values]
for w in w_tuples:
    label = '<b>'+w[0] + '</b><br>MW: ' + str(w[1])
    feature_group_wind.add_child(Marker([w[3], w[2]], popup=label, icon=folium.Icon(color='green', icon='flag', prefix='glyphicon')))


# In[703]:


#print(label)


# In[704]:


folium_map.add_child(feature_group_wind)
folium_map.add_child(folium.map.LayerControl());


# In[705]:


folium_map.save("price_map.html")
cur.close()
conn.close()


# In[706]:


folium_map


# In[707]:


help(folium.Icon)


# In[189]:


folium_map = folium.Map(location=[37.534, -119.058],
                        zoom_start=6,
                        tiles = 'Stamen Terrain',
                        API_key=,
                )

# In[190]
    
    try:
        cur.execute("SELECT id, node_name, latitude, longitude FROM nodes_caiso WHERE node_type='GEN' ORDER BY id ASC")
        row = cur.fetchone()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    while row is not None:
        print(row)
        cur2 = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur2.execute("SELECT extract(hour from timestamp) AS time, real_time_price AS price, day_ahead_energy AS day_ahead FROM nodes_pricing WHERE 	   		node_id=%s AND timestamp > CURRENT_DATE - interval '2 days' ORDER BY timestamp ASC LIMIT 24", [row[0]])
        df = pd.DataFrame(cur2.fetchall(), columns=['time', 'price', 'day_ahead'])
        df.set_index('time', drop=False)

        line_chart = vincent.Line(df, iter_idx='time', width=600, height=300)
        line_json = line_chart.to_json()

        popup = folium.Popup(max_width=650)
        folium.Vega(line_json, height=350, width=650).add_to(popup)
        folium.Marker([row[2], row[3]], popup=popup).add_to(folium_map)

        row = cur.fetchone()

        folium_map.save("price_map.html")
        cur.close()
        conn.close()

    return folium_map


# In[191]:


GetNodes()


# In[138]:


source = pd.DataFrame(np.cumsum(np.random.randn(100, 3), 0).round(2),
                    columns=['A', 'B', 'C'], index=pd.RangeIndex(100, name='x'))
source = source.reset_index().melt('x', var_name='category', value_name='y')

# Create a selection that chooses the nearest point & selects based on x-value
nearest = alt.selection(type='single', nearest=True, on='mouseover',
                        fields=['x'], empty='none')

# The basic line
line = alt.Chart().mark_line(interpolate='basis').encode(
    x='x:Q',
    y='y:Q',
    color='category:N'
)

# Transparent selectors across the chart. This is what tells us
# the x-value of the cursor
selectors = alt.Chart().mark_point().encode(
    x='x:Q',
    opacity=alt.value(0),
).add_selection(
    nearest
)

# Draw points on the line, and highlight based on selection
points = line.mark_point().encode(
    opacity=alt.condition(nearest, alt.value(1), alt.value(0))
)

# Draw text labels near the points, and highlight based on selection
text = line.mark_text(align='left', dx=5, dy=-5).encode(
    text=alt.condition(nearest, 'y:Q', alt.value(' '))
)

# Draw a rule at the location of the selection
rules = alt.Chart().mark_rule(color='gray').encode(
    x='x:Q',
).transform_filter(
    nearest
)

# Put the five layers into a chart and bind the data
alt.layer(line, selectors, points, rules, text,
          data=source, width=600, height=300)

