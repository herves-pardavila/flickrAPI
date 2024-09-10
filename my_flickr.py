import flickrapi
import json
from collections import defaultdict
import geopandas as gpd
import pandas as pd
import time
import shapely.geometry
import datetime
from random import random

def get_data(photos):
    '''
    '''
    toret = defaultdict(list)
    for row in photos['photos']['photo']:
        toret['id'].append(row['id'])
        toret['date'].append(datetime.datetime.strptime(
            row['datetaken'], "%Y-%m-%d %H:%M:%S"))
        toret['Title'].append(row['title'])
        toret['tags'].append(row['tags'])
        toret['owner'].append(row['owner'])
        toret['owner_name'].append(row['ownername'])
        toret['views'].append(float(row['views']))
        toret['url'].append(row['url_q'])
        toret['latitude'].append(row['latitude'])
        toret['longitude'].append(row['longitude'])
        toret['context'].append(row['context'])

    df = pd.DataFrame(toret)
    #gdf = gpd.GeoDataFrame(
        #df, geometry=gpd.points_from_xy(df.longitude.astype(float), df.latitude.astype(float)))
    # gdf['geometry'] = gdf['geometry'].apply(
    #     lambda x: shapely.geometry.mapping(x))
    return df

def look_through_pages(df,flickr,pages,the_start_date,the_days):

    for i in range(1,pages+1):
        
        photos=flickr.photos.search(bbox="-7.79288367353, 35.846850084, 4.3948408368, 43.8483377142",geo_context=0,min_taken_date=str(the_start_date),max_taken_date=str(the_days),content_type=7,extras='geo, views, date_taken, owner_name, description, tags, url_q, machine_tags, media, o_dims, original_format, icon_server',page=i,per_page=500)
        newdf=get_data(photos)
        newdf=newdf.drop_duplicates(subset="id")
        df=pd.concat([df,newdf])
        df=df.drop_duplicates(subset="id")
    return df

if __name__== "__main__":

    final_date=1672527600 #01-10-2022
    start_date=1640991600 #16-09-2023
    day_duration=final_date-start_date #timestep
    #cooredenadas de los parques naturales
    coordenadas_españa_sin_galicia="-7.79288367353, 35.846850084, 4.3948408368, 43.8483377142"
    coordenadas_canarias="-18.49929, 27.45132, -13.15671,29.50759"
    coordenadas_aiguestortes="0.78, 42.49, 1.1, 42.66"
    coordenadas_cabrera="2.87, 39.0, 3.01, 39.25"
    coordenadas_cabañeros="-4.8, 39.26, -4.2, 39.6"
    coordenadas_taburiente="-17.92, 28.67, -17.81, 28.78"
    coordenadas_doñana="-6.83, 36.75, -6.1, 37.38"
    coordenadas_garajonay="-17.4, 28.06, -17.15, 28.2"
    coordenadas_islas_atlanticas="-9.1, 42.15, -8.84, 42.53"
    coordenadas_monfrague="-6.5, 39.6, -5.5, 40.0"
    coordenadas_ordesa="-0.2, 42.5,0.17, 42.75"
    coordenadas_guadarrama="-4.3, 40.7, -3.6, 41.2"
    coordenadas_nieves="-5.15, 36.54,-4.8, 36.8"
    coordenadas_nevada="-3.7, 36.8, -2.5, 37.27"
    coordenadas_daimiel="-3.8, 39.0, -3.6, 39.2"
    coordenadas_teide="-16.8, 28.16, -16.42, 28.35"
    coordenadas_timanfaya="-13.9, 28.9, -13.7, 29.1"
    coordenadas=coordenadas_teide

    flickr1=flickrapi.FlickrAPI("029c094d1b5e98e7768d25edc2348cd9", "f945fb4cff0b0b67", format='parsed-json')
    flickr2=flickrapi.FlickrAPI("29222bf718a8d96ccb100cc8b340fbff", "773ddbdab9c8fcb2", format='parsed-json')
    flickr3=flickrapi.FlickrAPI("e937b92cc7aaae62a278c878bfc9892f", "cde1ef5a9ec2b8ed", format='parsed-json')
    
    final_date=1672527600 #01-10-2022
    start_date=1640991600 #16-09-2023
    day_duration=final_date-start_date #timestep

    #DF=pd.DataFrame()
    DF=pd.read_csv("photos_parques_naturales.csv")
    start=start_date
    
    for days in range(start_date,final_date,day_duration):
        DFday=pd.DataFrame()
        if days==start_date: 
            continue
        else:
            repetir=True

            while repetir==True:

                photos1=flickr1.photos.search(bbox="-7.79288367353, 35.846850084, 4.3948408368, 43.8483377142",page=1,per_page=500,geo_context=0,min_taken_date=str(start),max_taken_date=(days),content_type=7,extras='geo, views, date_taken, owner_name, description, tags, url_q, machine_tags, media, o_dims, original_format, icon_server')
                print("Nphotos=",photos1["photos"]["total"],",","Npages=",photos1["photos"]["pages"])
                photos2=flickr2.photos.search(bbox="-7.79288367353, 35.846850084, 4.3948408368, 43.8483377142",page=1,per_page=500,geo_context=0,min_taken_date=str(start),max_taken_date=(days),content_type=7,extras='geo, views, date_taken, owner_name, description, tags, url_q, machine_tags, media, o_dims, original_format, icon_server')
                print("Nphotos=",photos2["photos"]["total"],",","Npages=",photos2["photos"]["pages"])
                photos3=flickr3.photos.search(bbox="-7.79288367353, 35.846850084, 4.3948408368, 43.8483377142",page=1,per_page=500,geo_context=0,min_taken_date=str(start),max_taken_date=(days),content_type=7,extras='geo, views, date_taken, owner_name, description, tags, url_q, machine_tags, media, o_dims, original_format, icon_server')
                print("Nphotos=",photos3["photos"]["total"],",","Npages=",photos3["photos"]["pages"])
                
                df1=get_data(photos1)
                df1=df1.drop_duplicates(subset="id")
                df1=look_through_pages(df1,flickr1,photos1["photos"]["pages"],start_date,days)
                

                df2=get_data(photos2)
                df2=df2.drop_duplicates(subset="id")
                df2=look_through_pages(df2,flickr2,photos2["photos"]["pages"],start_date,days)
                

                df3=get_data(photos3)
                df3=df3.drop_duplicates(subset="id")
                df3=look_through_pages(df3,flickr3,photos3["photos"]["pages"],start_date,days)
                
                
                df=pd.concat([df1,df2,df3])
                df=df.drop_duplicates(subset="id")
                
                dfnew=pd.concat([DFday,df])
                dfnew=dfnew.drop_duplicates(subset="id")
 
                print("Nuevas fotos encontradas",len(dfnew)-len(DFday))
                if len(dfnew) - len(DFday)<100:
                    DFday=dfnew
                    DF=pd.concat([DF,DFday])
                    DF.to_csv("photos_parques_naturales.csv",index=False)
                    print("\n")
                    time.sleep(3600*random())
                    start=days
                    repetir=False
                else:
                    DFday=dfnew
                    time.sleep(3600)
              





