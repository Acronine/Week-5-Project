import pandas as pd
import requests, json
import mysql.connector
import pymysql
from sqlalchemy import create_engine

def wrangle(file_path):
    # Creating the column names for the first four columns, then setting them into the DataFrame Object.
    column_names = ['winning_team','cluster_id','game_mode','game_type']
    df = pd.read_csv(file_path, usecols=[*range(0,4)],header= None, names= column_names)
    # Python terminal was giving a weird error, this pd set option turns it off. Code works fine afterwards
    pd.set_option('mode.chained_assignment', None)
    df['winning_team'].replace(-1,'dire',inplace=True)
    df['winning_team'].replace(1,'radiant',inplace=True)
    
    cluster_page = requests.get(r'https://raw.githubusercontent.com/kronusme/dota2-api/master/data/regions.json').json()
    cluster_dict={}
    for i in range(len(cluster_page['regions'])):
        cluster_dict[str(cluster_page['regions'][i]['id'])]=(cluster_page['regions'][i]['name']).replace(' ','_').lower()
    cluster_dict['232'] = 'china' # This cluster id is set to the city of Tianjin, China
    for i in range(len(df['cluster_id'])):
        df['cluster_id'][i] = cluster_dict[(str(df['cluster_id'][i]))]
    
    mode_page = requests.get(r'https://raw.githubusercontent.com/kronusme/dota2-api/master/data/mods.json').json()
    mode_dict = {}
    for i in range(len(mode_page['mods'])):
        mode_dict[str(mode_page['mods'][i]['id'])]=(mode_page['mods'][i]['name']).replace(' ','_').lower()
    mode_dict['6'] = 'intro_death' # directly fixing the ??_*_??
    for i in range(len(df['game_mode'])):
        df['game_mode'][i] = mode_dict[(str(df['game_mode'][i]))]
        
    lobby_page = requests.get(r'https://raw.githubusercontent.com/kronusme/dota2-api/master/data/lobbies.json').json()
    lobby_dict = {}
    for i in range(len(lobby_page['lobbies'])):
        lobby_dict[str(lobby_page['lobbies'][i]['id'])]=(lobby_page['lobbies'][i]['name']).replace(' ','_').lower()
    for i in range(len(df['game_type'])):
        df['game_type'][i] = lobby_dict[(str(df['game_type'][i]))]
        
    heros_df = pd.read_csv(file_path, usecols=[*range(4,27),*range(28,117)],header=None, na_values=0)
    # skipping the column indexed at 27. The entire column is 0 and if were placed at 24 by index
    # the i hero id 24 is missing in the dota 2 api as well. No information is provided why.
    heros_df = heros_df.transpose().reset_index(drop=True)
    hero_page = requests.get(r'https://raw.githubusercontent.com/kronusme/dota2-api/master/data/heroes.json').json()
    hero_dict = {}
    for i in range(len(hero_page['heroes'])):
        # I will be using the index of the heros dataframe to pull the hero name,
        # so I need the index to start at 0.
        if i < 23:
            hero_dict[str((hero_page['heroes'][i]['id']-1))]=(hero_page['heroes'][i]['localized_name']).replace(' ','_').lower()
        # After analyzing, I found #24 missing, so once the ids hit 24, it will
        # subtract to match with the column length of the heroes.
        elif i >= 23:
            hero_dict[str((hero_page['heroes'][i]['id'])-2)]=(hero_page['heroes'][i]['localized_name']).replace(' ','_').lower()
    # While tracking through the api, discovered it had these two as their original
    # Warcraft 3 Dota names, so I changed them to their more relative names.
    hero_dict['40'] = 'wraith_king'
    hero_dict['89'] = 'io'
    
    data = []
    for dex in range(len(heros_df.columns.tolist())):
        radiant_team = []
        dire_team = []
        hero_col = {}
        for i in range(len(heros_df[dex])):
            if heros_df[dex][i] == 1:
                radiant_team.append(hero_dict[str(i)])
            elif heros_df[dex][i] == -1:
                dire_team.append(hero_dict[str(i)])
        for ind in range(len(radiant_team)):
            hero_col[f'radiant_player_{ind+1}'] = radiant_team[ind]
        for inde in range(len(dire_team)):
            hero_col[f'dire_player_{inde+1}'] = dire_team[inde]
        data.append(hero_col)

    df2 = pd.DataFrame.from_dict(data)
    df3 = pd.concat([df,df2],axis=1)
    return df3

if __name__ == '__main__':
    df1 = wrangle(r'C:\Users\Owner\Documents\Week 5 Project\dota2Train.csv')
    df2 = wrangle(r'C:\Users\Owner\Documents\Week 5 Project\dota2Test.csv')
    df = pd.concat([df1,df2],axis=0)
    # df.to_csv('dota_2_matches',index=False)
    # print('test')

    mydb = create_engine("mysql+mysqldb://user_5:Bonfire124@localhost/week_5_project")
    df.to_sql(con=mydb,name='dota_2_matches',if_exists='replace')